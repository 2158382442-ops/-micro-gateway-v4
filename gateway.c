#include <stdio.h>       // printf/perror
#include <stdlib.h>      // malloc/free/exit
#include <string.h>      // memcpy/memset/strlen
#include <stdint.h>      // uint8_t（frame_t 用）
#include <unistd.h>      // read/write/close/usleep
#include <fcntl.h>       // open + O_RDWR/O_NOCTTY
#include <termios.h>     // tcgetattr/tcsetattr/VMIN/VTIME/c_cflag
#include <pthread.h>     // 线程三件套（编译要 -pthread）
#include <sqlite3.h>     // SQLite C API（编译要 -lsqlite3）
#include <mosquitto.h>   // MQTT C 库（编译要 -lmosquitto）
#include <cjson/cJSON.h> // cJSON 组包（apt 版路径；若源码版则写 cJSON.h）
#include "log.h"         // 拿日志函数（预处理指令行尾不加分号）
#include <signal.h>      // signal 挂号 + sig_atomic_t + SIGTERM/SIGINT（B3 新增）
#include <sys/stat.h>    // umask：新文件权限减法清单（B3 新增）
#include <errno.h>
#include <stdatomic.h>

/* ================= 宏参数 ======================= */
#define FRAME_MAX 32                   // 帧容器大小
#define REG_MAX ((FRAME_MAX - 5) / 2)  // =13：数据区最多几个寄存器（减3头减2CRC，每寄存器2字节）
#define JSON_LEN 256                   // 128→256：格式化 JSON 13 个寄存器会超 128，防截断
#define BUILD_ID __DATE__ " " __TIME__ // 编译时间戳当 build-id，证明 72h 跑的是哪个二进制

/* ================ 全局变量 =================== */
int fd;                              // 描述符
struct termios tty;                  // 串口
sqlite3 *db;                         // 数据库
struct mosquitto *mq = NULL;         // MQTT
const char *host = "192.168.31.219"; // VM 局域网 IP（与 HTTP 目标同址；08-29 实踩：原值 localhost 跟运行者走，板子上=板子自己，MQTT 全天自环假上云）
const char *topic = "gw/registers";  // 地址牌

/* ========= 08-29 真回执升级：publish 返回值=入队成功 ≠ 送达成功 =========
 * QoS1 真送达=broker 回 PUBACK=lib 调 on_publish 回调，那时才盖 sent=1；
 * 否则断云期间进程一死，“盖了戳没送达”的帧静默丢失（假回执缺陷）
 * mid→账本id 映射表：回调只拿到 mid（lib 的编号），要反查账本行号 */
#define MID_MAP_SIZE 64
static struct
{
    int mid;
    int dbid;
} g_mid_map[MID_MAP_SIZE]; // mid==0=空槽（lib 的 mid 从 1 起）
static pthread_mutex_t g_mid_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_db_lock = PTHREAD_MUTEX_INITIALIZER; // 保护共享 db：INSERT+取rowid 必须原子
static volatile sig_atomic_t g_mqtt_up = 0;                   // 连接状态牌：on_connect/on_disconnect 翻面，resend 看它决定动不动
static atomic_ulong g_frames_recv = 0;                        // 捞到的有效帧（producer 埋）
static atomic_ulong g_frames_sent = 0;                        // 真送达 PUBACK（on_publish_cb 埋）
static atomic_ulong g_frames_drop = 0;                        // 丢弃帧（CRC/malloc/push/dump_json/insert 埋）
static time_t g_last_beat = 0;                                // 上次心跳时刻（只 producer 线程碰，不用 atomic）

static void mid_map_set(int mid, int dbid) // 入队成功：占槽登记，等回调盖戳
{
    pthread_mutex_lock(&g_mid_lock);
    for (int i = 0; i < MID_MAP_SIZE; i++)
        if (g_mid_map[i].mid == 0)
        {
            g_mid_map[i].mid = mid;
            g_mid_map[i].dbid = dbid;
            break;
        }
    pthread_mutex_unlock(&g_mid_lock);
}

static int mid_map_take(int mid) // 凭 mid 查账本 id，取走清槽；找不到: -1
{
    int dbid = -1;
    pthread_mutex_lock(&g_mid_lock);
    for (int i = 0; i < MID_MAP_SIZE; i++)
        if (g_mid_map[i].mid == mid)
        {
            dbid = g_mid_map[i].dbid;
            g_mid_map[i].mid = 0;
            break;
        }
    pthread_mutex_unlock(&g_mid_lock);
    return dbid;
}

static int mid_map_holds(int dbid) // 查这个账本id是否已在途(等PUBACK)：在=1别重发，不在=0
{
    int held = 0;
    pthread_mutex_lock(&g_mid_lock);
    for (int i = 0; i < MID_MAP_SIZE; i++)
        if (g_mid_map[i].mid != 0 && g_mid_map[i].dbid == dbid)
        {
            held = 1;
            break;
        }
    pthread_mutex_unlock(&g_mid_lock);
    return held;
}

/* ---- 三个回调：都跑在 lib 后台 loop 线程（sqlite 默认 serialized 模式可跨线程） ---- */
static void on_connect_cb(struct mosquitto *m, void *o, int rc)
{
    g_mqtt_up = (rc == 0);
    LOG_INFO("[MQTT] %s", rc == 0 ? "连上 broker" : "连接被拒");
}
static void on_disconnect_cb(struct mosquitto *m, void *o, int rc)
{
    g_mqtt_up = 0; // 翻牌：resend 停手，账本接管
    LOG_WARN("[MQTT] 断开(rc=%d)，账本接管", rc);
}
static void on_publish_cb(struct mosquitto *m, void *o, int mid)
{
    int dbid = mid_map_take(mid); // broker 回了 PUBACK=真送达
    if (dbid < 0)
        return;
    char upd[64];
    snprintf(upd, sizeof(upd), "UPDATE frames SET sent=1 WHERE id=%d", dbid);
    sqlite3_exec(db, upd, NULL, NULL, NULL); // db 是全局连接
    atomic_fetch_add(&g_frames_sent, 1);
    LOG_INFO("[MQTT] 送达盖戳：帧#%d", dbid);
}

/* ====================== 全局信号与守护化（B3 新增） ===================== */
static volatile sig_atomic_t g_running = 1; /* 营业牌：还开着=1。volatile 挡编译器（别缓存，每次真读）；sig_atomic_t 挡中断撕裂（读写一条指令完成不可劈半） */

/* 信号处理函数：只翻牌子不干重活（信号随机时刻插入，里面调复杂函数=赌博；重活留给主循环按顺序收尾） */
static void on_signal(int sig)
{
    (void)sig; /* sig=信号编号（SIGINT=2/SIGTERM=15）；(void)转型=故意不用，挡 -Wall 未使用参数警告 */
    g_running = 0;
}

/* 守护化三板斧+收尾：搬出终端、迁户口、断三流 */
static void daemonize(void)
{
    pid_t pid = fork(); /* 搬出来：复制一个子进程 */
    if (pid < 0)
    {
        perror("fork"); /* 此时 log_init 还没跑，LOG 是哑巴，只能 perror 喊终端 */
        exit(1);
    }
    if (pid > 0)
    {
        exit(0); /* 父进程退出（0=成功退场）：shell 认为命令已结束，终端放手 */
    }
    setsid();   /* 迁户口：子进程必然不是组长才能调成功→新会话组长，与控制终端断链（SSH 断了也带不走它） */
    chdir("/"); /* 换工作目录：别占着挂载点（cwd 赖着的目录卸载不掉） */
    umask(0);   /* 减法清单清零：新文件权限不打折（不是权限设为0，是"什么都不减"） */

    int null_fd = open("/dev/null", O_RDWR); /* 黑洞：终端通道正式死亡，LOG 文件通道接班（双通道策略收官）；命名避开全局串口 fd */
    dup2(null_fd, STDIN_FILENO);             /* dup2(旧,新)：把新编号的出口改接到旧指向的文件上（新若原本开着先关掉） */
    dup2(null_fd, STDOUT_FILENO);
    dup2(null_fd, STDERR_FILENO);
    if (null_fd > 2) /* 防御：万一 open 返回 0/1/2（标准流提前被关过），关它就等于关标准流；只关 >2 的冗余管 */
    {
        close(null_fd); /* 三根标准流已改接黑洞，原来这根冗余管收回 */
    }
}

/* ========================= 全局工具函数区 ========================= */
/* 串口配置 */
static void termios_init(int fd, struct termios *tty)
{
    // 看串口配置
    tcgetattr(fd, tty);
    // 配置8N1
    tty->c_cflag &= ~PARENB;
    tty->c_cflag &= ~CSIZE;
    tty->c_cflag |= CS8;
    tty->c_cflag &= ~CSTOPB;
    // 开启本地连接和接收
    tty->c_cflag |= (CLOCAL | CREAD);
    // 关掉终端模式
    tty->c_lflag &= ~(ECHO | ECHOE | ISIG | ICANON);
    // 配置输入输出模式
    tty->c_iflag &= ~(IXON | IXOFF | IXANY);
    tty->c_iflag &= ~(ICRNL | INLCR);
    tty->c_oflag &= ~OPOST;
    // 配置波特率
    cfsetispeed(tty, B115200);
    cfsetospeed(tty, B115200);
    // 配置轮询：0=有多少读多少，10=等 1 秒超时（单位 0.1s）
    tty->c_cc[VMIN] = 0;
    tty->c_cc[VTIME] = 10;
    // 写进串口：TCSANOW=立即生效不等排空
    tcsetattr(fd, TCSANOW, tty);
}

static void beat_tick(void)
{ // 满 60s 打一条心跳（带计数器快照）：证明"活着且在干活"
    time_t now = time(NULL);
    if (g_last_beat == 0)
        g_last_beat = now; // 首次立基准，不立刻打
    if (now - g_last_beat >= 60)
    {
        g_last_beat = now;
        LOG_INFO("[心跳] 活着 | recv=%lu sent=%lu drop=%lu",
                 atomic_load(&g_frames_recv), atomic_load(&g_frames_sent), atomic_load(&g_frames_drop));
    }
}

#define SERIAL_DEV "/dev/ttymxc5" /* 收宏，和 720 行 open 用同一个 */

static int serial_reopen(void)
{
    int backoff = 1; /* 秒，指数退避：1→2→4→8→16→30 封顶 */
    while (g_running)
    {
        fd = open(SERIAL_DEV, O_RDWR | O_NOCTTY);
        if (fd >= 0)
        {
            termios_init(fd, &tty); /* 重开必须重配 8N1/VMIN/VTIME，不然读到乱码 */
            LOG_INFO("[串口] 重开成功 fd=%d", fd);
            return 0;
        }
        LOG_WARN("[串口] 重开失败，%ds 后再试", backoff);
        for (int i = 0; i < backoff && g_running; i++)
        {
            sleep(1);
            beat_tick();
        }
        if (backoff < 30)
            backoff *= 2;
    }
    return -1; /* 只有 g_running 翻 0 才走到这：优雅退出 */
}

// 数据包结构体
typedef struct
{
    // 包含数据区 和 数据长度头
    uint8_t data[FRAME_MAX];
    int len;
} frame_t;

static uint16_t crc16(uint8_t *frame, int len)
{
    uint16_t crc = 0xFFFF; // Modbus CRC16 初值固定 0xFFFF
    for (int i = 0; i < len; i++)
    {
        crc ^= frame[i];
        for (int bit = 0; bit < 8; bit++)
        {
            if (crc & 0x0001)
            {
                crc >>= 1;
                crc ^= 0xA001; // 0x8005 的反射多项式（Modbus 反向算法）
            }
            else
            {
                crc >>= 1;
            }
        }
    }
    return crc;
}

static void rs485_dir(int tx)
{
    // 自动申请：gpio118 目录不存在就先 export + 设输出（幂等设计，08-27 移植 key_ctrl_led 模式）
    if (access("/sys/class/gpio/gpio118", F_OK) != 0)
    {
        int e = open("/sys/class/gpio/export", O_WRONLY);
        if (e >= 0)
        {
            write(e, "118", 3);
            close(e);
        }
        usleep(10000); // 等内核生成节点（10ms）
        int d = open("/sys/class/gpio/gpio118/direction", O_WRONLY);
        if (d >= 0)
        {
            write(d, "out", 3);
            close(d);
        }
    }
    // 写电平：1=发送(DE拉高) 0=接收(RE拉低)。正面丝印 GPIO3 = sysfs 118
    int f = open("/sys/class/gpio/gpio118/value", O_WRONLY);
    if (f < 0)
    {
        LOG_WARN("gpio118 export 失败");
        perror("gpio118");
        return;
    }
    write(f, tx ? "1" : "0", 1);
    close(f);
}

// 计算总长：3（头+功能码+字节数） + byte_count + 2（CRC）
static int frame_total_len(const uint8_t *buf, int pos)
{
    uint8_t byte_count = buf[pos + 2];
    return 3 + byte_count + 2;
}

/* ===================================== 全局协议区 =================================== */
static void create_table(sqlite3 *db)
{
    char *err = NULL;
    int rc = sqlite3_exec(
        db,
        "CREATE TABLE IF NOT EXISTS frames("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "json TEXT NOT NULL,"
        "sent INTEGER DEFAULT 0)",
        NULL, NULL, &err);
    if (rc != SQLITE_OK)
    {
        LOG_ERROR("建表失败：%s", err);
        fprintf(stderr, "建表失败：%s\n", err);
        sqlite3_free(err);
        exit(1);
    }
    /* 复合索引：mqtt_resend 的 WHERE sent=0 ORDER BY id 靠它走索引，
   不然 72h 表涨到几千行每轮全表扫，越来越慢 */
    rc = sqlite3_exec(db,
                      "CREATE INDEX IF NOT EXISTS idx_frames_sent_id ON frames(sent,id)",
                      NULL, NULL, &err);
    if (rc != SQLITE_OK)
    {
        LOG_WARN("建索引失败（不致命，顶多慢）：%s", err); // 索引失败不影响正确性，warn 不 exit
        sqlite3_free(err);
    }
}

static int dump_json(int *buf, int len, char *out, int out_size) // void→int：0 成功 / -1 失败
{
    cJSON *root = cJSON_CreateObject();
    if (!root)
        return -1; // 建根失败（OOM）
    cJSON *arr = cJSON_CreateIntArray(buf, len);
    if (!arr)
    {
        cJSON_Delete(root);
        return -1;
    } // 建数组失败：先回收 root 再走
    cJSON_AddItemToObject(root, "registers", arr); // root 挂走 arr，之后只删 root（arr 归 root 管）
    char *json_str = cJSON_Print(root);
    if (!json_str)
    {
        cJSON_Delete(root);
        return -1;
    } // 打印失败：回收 root
    int n = snprintf(out, out_size, "%s", json_str);
    if (n < 0 || n >= out_size)
    { // 截断闸：JSON 被切=非法，绝不能流到下游
        LOG_WARN("[json] 截断：需 %d，容器 %d", n, out_size);
        cJSON_Delete(root);
        free(json_str);
        return -1;
    }
    LOG_DEBUG("%s", json_str);
    cJSON_Delete(root); // 289 行那个 printf 顺手删掉：你选了 A 方案（LOG-only），它每帧刷屏、daemon 下还是死的
    free(json_str);
    return 0;
}

static int insert_data(sqlite3 *db, const char *json)
{
    sqlite3_stmt *stmt = NULL;
    const char *sql = "INSERT INTO frames(json,sent) VALUES(?1,0)";
    pthread_mutex_lock(&g_db_lock); // ← 临界区开始
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
    {
        LOG_ERROR("prepare失败：%s", sqlite3_errmsg(db));
        pthread_mutex_unlock(&g_db_lock); // 失败路径也要先解锁（exit 留待 P0-4 改 return -1）
        return -1;
    }
    if (sqlite3_bind_text(stmt, 1, json, -1, SQLITE_TRANSIENT) != SQLITE_OK)
    {
        LOG_ERROR("bind失败：%s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        pthread_mutex_unlock(&g_db_lock);
        return -1;
    }
    if (sqlite3_step(stmt) != SQLITE_DONE)
    {
        LOG_ERROR("step失败：%s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        pthread_mutex_unlock(&g_db_lock);
        return -1;
    }
    int id = (int)sqlite3_last_insert_rowid(db); // 紧跟 step、锁内——缝被焊死，别的 worker 进不来
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&g_db_lock); // ← 临界区结束
    return id;
}

static int db_saveJson_bySqlite(sqlite3 *db, const char *json)
{
    return insert_data(db, json);
}

// 补传函数：每轮末尾都调，网络恢复自愈不需人工干预（先落库再上报的顺序是不丢的根源）
static void mqtt_resend(sqlite3 *db)
{
    if (!g_mqtt_up)
        return; // 08-29：断开时不扫（免得每秒往 lib 队列塞重复帧），重连后一把扫完
    // 补传函数第一步找有没有sent=0的行 找没有上传成功的
    sqlite3_stmt *stmt;
    char sql[] = "SELECT id,json FROM frames WHERE sent=0 ORDER BY id LIMIT 32"; // 排序走复合索引；限量防积压一把灌爆
    // NULL：不取未编译部分指针
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK)
    {
        return;
    }
    // 循环执行扫描行 找到就改（SQLITE_ROW=读到一行）
    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        int id = sqlite3_column_int(stmt, 0);
        if (mid_map_holds(id))
            continue; // 去重：这帧已在途(等PUBACK)，跳过不重发
        const char *json = (const char *)sqlite3_column_text(stmt, 1);
        int mid = 0;
        if (mq && mosquitto_publish(mq, &mid, topic, strlen(json), json, 1, false) == MOSQ_ERR_SUCCESS)
        {
            mid_map_set(mid, id);
            LOG_INFO("[补帧] 帧#%d 入队补发（等送达盖戳）", id);
        }
    }
    sqlite3_finalize(stmt);
}

/* ================================ 线程池区：声明结构，创建与销毁  ============================= */
/* 任务节点：函数指针 + 参数 + 链扣（短任务才进池，长驻循环不进池——池的唯一假设是任务会结束） */
typedef struct task
{
    void (*pf)(void *); // 消费函数钩子（塞的是 threadPool_pop）
    void *arg;          // 消费函数参数（塞的是数据包指针）
    struct task *next;  // 队列链扣（尾插法）
} task_t;

/* 线程池结构体：队列 + 锁 + 条件变量 + 线程数组 + 停业牌（队列与停业牌 = 唯一并发修改点） */
typedef struct
{
    task_t *head, *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    // 线程数组（3 个 worker）
    pthread_t *workers;
    // 线程数量
    int worker_len;
    // 停业牌 0营业 1停业（优雅退出的开关）
    int shutdown;
} threadPool_t;

// 先声明消费者线程函数（创建函数在后面定义，先声明后使用）
static void *threadPool_consumer(void *arg);

// 线程创建函数：创建好后同时启动消费者（生产者单独 pthread_create，不进池——长驻循环进池会永久霸占 worker 饿死池子）
threadPool_t *threadPool_create(int n) // n=worker 数，网关传 3（采集速率 > 上报速率，纯防阻塞堆积）
{
    threadPool_t *tp = calloc(1, sizeof(threadPool_t)); // calloc 全零：head/tail/停业牌天然干净，免手工初始化（对比 v2 的显式 memset）
    tp->worker_len = n;
    pthread_mutex_init(&tp->lock, NULL); // NULL=默认属性（普通非递归锁）
    pthread_cond_init(&tp->cond, NULL);  // NULL=默认属性
    tp->workers = malloc(sizeof(pthread_t) * n);
    for (int i = 0; i < n; i++)
    {
        pthread_create(&tp->workers[i], NULL, threadPool_consumer, tp); // NULL=默认栈，arg=池子本体（consumer 用它摸队列）
    }
    return tp;
}

static void threadPool_shutdown(threadPool_t *tp)
{
    pthread_mutex_lock(&tp->lock);
    tp->shutdown = 1;                  // 挂停业牌：不再接新任务，存量消费完就退（优雅退出）
    pthread_cond_broadcast(&tp->cond); // 广播叫醒全部 worker（signal 只叫醒一个，关店必须全员通知）
    pthread_mutex_unlock(&tp->lock);

    for (int i = 0; i < tp->worker_len; i++)
    {
        pthread_join(tp->workers[i], NULL); // NULL=不要返回值，只等归队；防僵尸线程（daemon 化时由 signal 链触发）
    }
    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->cond);
    free(tp->workers);
    free(tp);
}

/* ===================================== 生产&消费线程函数：任务的具体调度 ============================= */
#define ADDR 0x01 // 从机地址（与从机程序约定）
#define FUNC 0x03 // 功能码 03=读保持寄存器（帧头两字节 = 捞帧识别码）
// 蓄水池容量（爆仓清空重来，08-23 背压实证跑过）
#define POOL_SIZE 1024
/**
 *  ------------- push生产函数 ------------
 *     职责： 1.申请任务节点空间
 *           2.挂载 消费函数 参数 和挂钩
 *           3.节点入队 然后叫醒消费者处理（对应四函数映射里的 submit）
 */
int threadPool_push(threadPool_t *tp, void (*pf)(void *), void *arg)
{
    // 创建任务节点（短任务：进池的前提是任务会结束）
    task_t *t = malloc(sizeof(task_t));
    // 如果创建失败直接返回（-1=入队失败，生产者自行丢数据包）
    if (t == NULL)
    {
        return -1;
    }
    // 把传过来的 消费函数和参数放进任务节点里（函数指针塞节点 = 消费时无需启动者，worker 自己执行）
    t->pf = pf;
    t->arg = arg;
    t->next = NULL;
    // 拿锁（队列 = 唯一并发修改点之一，入队必锁）
    pthread_mutex_lock(&tp->lock);
    // 看线程池状态 如果是停业那么解锁释放节点并返回（关店后不接新任务）
    if (tp->shutdown)
    {
        pthread_mutex_unlock(&tp->lock);
        free(t);
        return -1;
    }
    // 看队列状态  如果前面有任务那么尾插这个节点（尾插保证先到先消费）
    if (tp->tail)
    {
        tp->tail->next = t;
    }
    else // 如果队列前面没有任务 那么该节点被头尾双指针指向（空队插入的头指针特判）
    {
        // 这里做头指针处理（有前驱时头指针不动，只有空队时才需要重新指向）
        tp->head = t;
    }
    // 这里做尾指针统一处理 因为插任务时尾指针始终指向最后一个任务（两个分支都要走，放 if 外）
    tp->tail = t;
    // 有任务来了 就先叫醒消费者线程 去消费（signal 只叫醒一个 = 一个任务只需一个 worker）
    pthread_cond_signal(&tp->cond);
    // 最后解锁（先 signal 后 unlock，醒来的人还要重新抢锁，不会丢任务）
    pthread_mutex_unlock(&tp->lock);
    return 0;
}

/**
 *  ---------------- 消费函数 -----------------
 *      职责：消费（短任务：拆帧→转JSON→落库→上报→补传，全部干完就结束）
 *           1.拿到生产出来的数据包（所有权转移：进池后由消费者 free）
 *           2.对数据包内容做 解析拆帧 +  转json + 拿到json_str + 缓存sqlite3（先落库再上报 = 断网不丢的根源）
 *           3.把json_str上传到MQTT（销账 sent）+ 发给HTTP（无条件报，不接返回值：数据本体已被 MQTT 链保住）
 */
static void threadPool_pop(void *arg)
{
    // 拿到数据包（生产者塞进任务节点的 arg）
    frame_t *adu = (frame_t *)arg;
    // 刷新标准输出（多进程终端输出及时可见）
    fflush(stdout);
    // 休眠300ms等待发完（轮询节奏控制，防连续轰炸从机）
    usleep(300000);

    /* ================== 拆帧：提取真实数据 ================ */
    // 响应帧结构：1地址 + 1功能码 + 1字节数 + 数据 + 2crc
    int info_count = adu->len - 5;  // 数据块字节数（总长减 3 头减 2 CRC）
    int val_count = info_count / 2; // 寄存器值数量（每寄存器 2 字节，10 个寄存器=20 字节）
    /* 纵深防御：门卫①已保证 val_count<=13，这里再夹一道，防 gate 被绕过/len 被别处改 */
    if (val_count > REG_MAX)
        val_count = REG_MAX;
    if (val_count < 0)
        val_count = 0;
    int parse_val[REG_MAX]; // 固定大小，不再用 VLA
    for (int i = 0; i < val_count; i++)
    {
        // 数据块是大端（Modbus 标准）：高字节左移 8 位拼低字节；+3=跳过地址/功能码/字节数三个头字节，i*2=每寄存器占两格；data 偏移 3/4 分别取高/低字节（i=0 时 data[3]<<8|data[4]）
        parse_val[i] = adu->data[3 + i * 2] << 8 | adu->data[4 + i * 2];
    }
    /* ====================== 转json ======================= */
    char json_str[JSON_LEN];
    // 这里是把解析好的数组里的元素一个一个往json_str里转
    if (dump_json(parse_val, val_count, json_str, sizeof(json_str)) != 0)
    {
        LOG_WARN("[消费] JSON 组包失败，丢帧");
        free(adu); // 组包失败：这帧没法上报，释放数据包走人（所有权在消费者手里）
        atomic_fetch_add(&g_frames_drop, 1);
        return;    // 跳过落库/上报/补传——没有合法 JSON，存进去也是脏数据
    }
    /* ==================== 缓存进sqlite ===================== */
    // 拿那行的id号（先落库：sent=0 入账，断网也不丢）
    int id = db_saveJson_bySqlite(db, json_str);
    if (id < 0)
    { // 落库失败：丢这一帧，但 worker 活着干下一帧（绝不 exit）
        LOG_WARN("[消费] 落库失败，丢帧（worker 继续）");
        free(adu);
        atomic_fetch_add(&g_frames_drop, 1);
        return;
    }
    // 发布：返回值=入队成功（lib 内 queue 会吸收瞬时断联），真送达等 on_publish 回调盖戳
    int mid = 0;
    if (mq && mosquitto_publish(mq, &mid, topic, strlen(json_str), json_str, 1, false) == MOSQ_ERR_SUCCESS)
    {
        mid_map_set(mid, id); // 登记 mid→账本id，等 PUBACK 来盖 sent=1
        LOG_INFO("[MQTT] 已入队：帧#%d（等送达盖戳）", id);
    }
    else // 连入队都失败（队列满等）：行留账本 sent=0，等补传扫
    {
        LOG_WARN("[MQTT] 上报失败,帧#%d 留在本地等补传", id);
        printf("[MQTT] 上报失败,帧#%d 留在本地等补传\n", id);
    }
    // 上报到HTTP（无条件报：返回值不接，失败可接受——补传服务于"数据不丢"，本体已在 SQLite）
    // http_report_json("http://192.168.31.219:8000/gw", json_str);
    // 补传函数（每轮末尾都调：网络一通，积压自动倒出，自愈）
    mqtt_resend(db);
    // 释放数据包（所有权在消费者手里，进池后生产者不再碰）
    free(adu);
}

/**
 *  -------------生产者调度函数-------------（长驻循环，单独 pthread_create 不进池）
 *      职责：
 *          1.缓冲池捞帧（外层发请求，内层蓄水池+捞帧）
 *          2.生产数据包（frame_t 装完整帧）
 *          3.把消费函数指针和数据包交给生产函数去入队（只调度不亲自消费）
 */
static void *producer(void *arg)
{
    threadPool_t *tp = (threadPool_t *)arg; // 拿池子：捞到帧往池里塞任务，生产者自己不消费（不变量：长驻不进池，只调度）
    uint8_t req[8];
    while (g_running) // 外层循环：一轮 = 发一次请求 + 收一轮回程（长驻任务不进池；B3 后改看营业牌：kill 翻牌→本轮收完退出→main 等到归队→收尾链）
    {
        beat_tick();
        // 组装请求帧：读从机 1 的保持寄存器，起始 0，数量 10（0x000A）
        req[0] = 0x01;                // 从机地址（与从机约定）
        req[1] = 0x03;                // 功能码 03=读保持寄存器（响应帧沿用，=帧头识别码）
        req[2] = 0x00;                // 起始寄存器地址高字节（0 = 从第 0 个读）
        req[3] = 0x00;                // 起始地址低字节（大端：高在前）
        req[4] = 0x00;                // 寄存器数量高字节（0x000A = 10 个）
        req[5] = 0x0a;                // 数量低字节（10 = 从机模拟的 0/100/.../900 十个值）
        uint16_t crc = crc16(req, 6); // 只算前 6 字节（地址~数量），CRC 位不参与计算（Modbus 规定）
        // crc校验位 小端 低字节在前 高字节在后（Modbus 帧内 CRC 传输顺序，与寄存器数据的大端相反）
        req[6] = crc & 0xFF; // 低字节（先上线）
        req[7] = crc >> 8;   // 高字节（后上线）
        /* ============== 发送帧 ============== */
        rs485_dir(1);              // 切发送模式 DE拉高（发前拉高：半双工总线先占住才能发）
        int w = write(fd, req, 8); // 8=帧总长（6 正文 + 2 CRC）；write 返回=进内核 FIFO≠物理发完（面试必考）
        if (w != 8)
        {
            LOG_WARN("[串口] write 异常：只写了 %d/8,errno=%d", w, errno);
            /* g_serial_err++ 计数器留到 R6 统一加，现在先 LOG_WARN 留痕 */
        }
        // 等FIFO数据全部移出线路 write不等于全部发完（115200 下 8 字节≈0.7ms，不排空就切方向会截断帧尾——v1 实踩）
        if (tcdrain(fd) != 0) // 阻塞直到发送 FIFO 排空，才能切方向（切早了从机收不到完整请求帧）
        {
            LOG_WARN("[串口] tcdrain 失败 errno=%d", errno);
        }
        // 切回接收模式 RE拉低（收前拉低：把总线让出来等从机回帧）
        rs485_dir(0);
        // 先拼hex字符串，再一次性写日志（日志函数一次调用=一条完整记录）
        char hex[32];
        int hp = 0;
        for (int i = 0; i < 8; i++)
        {
            hp += snprintf(hex + hp, sizeof(hex) - hp, "%02X", req[i]);
        }
        LOG_DEBUG("[主机] 发送请求 %s", hex);
        printf("[主机] 发送请求");
        for (int i = 0; i < 8; i++)
        {
            printf("%02X", req[i]);
        }
        printf("\n");
        /* ==================== 接收帧（内层循环：蓄水池+捞帧） =================== */
        // 蓄水池：攒字节流等帧齐（粘包/半帧都靠它消化）
        uint8_t pool[POOL_SIZE];
        // 水位线：池里当前字节数（追加写/清空都维护它）
        int pool_len = 0;
        // 捞到的帧数（本轮统计）
        int got = 0;
        while (1) // 内层循环：read 到有数据就进池捞帧，read 无数据（VTIME 超时）= 回程结束退出外层发下一轮（VTIME 兜底退出内层）
        {
            // 缓冲区：单次 read 的临时落点（VMIN=0/VTIME=10：最多等 1 秒，有多少给多少）
            uint8_t buf[256];
            int n = read(fd, buf, sizeof(buf)); // 读串口：n=本次字节数，阻塞上限 1 秒（VTIME）
            if (n == 0)
            {
                break; // 超时=从机这轮回程结束（VTIME 兜底）;
            }
            else if (n < 0)
            {
                if (errno == EINTR)
                {
                    if (!g_running) /* ① 信号打断 */
                        break;
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) /* ② 暂无数据 */
                {
                    break;
                }
                /* 串口真坏了 */
                LOG_ERROR("[串口] read errno=%d,触发重开", errno);
                close(fd);
                serial_reopen(); /* 自愈：退避重开，成功才回来 */
                break;
            }
            // 水位线加本次读的内容大于池容量 就爆仓提示（异常流量保护：宁可丢一轮也不越界写）
            if (pool_len + n > POOL_SIZE)
            {
                LOG_WARN("[蓄水池] 满了(%d),清空重来", pool_len);
                printf("[蓄水池] 满了(%d),清空重来\n", pool_len);
                pool_len = 0; // 水位线归零：老数据作废，从本次读重新攒（清空重来不是报错退出）
            }
            /* 读到数据，水位线追加 数据覆盖进蓄水池（追加写：旧数据不动，新字节接在水位线后面） */
            memcpy(pool + pool_len, buf, n);
            pool_len += n;
            LOG_DEBUG("[蓄水池] +%d字节 --> 当前%d字节", n, pool_len);
            printf("[蓄水池] +%d字节 --> 当前%d字节\n", n, pool_len);
            // 逐字节检测：从池头扫帧头（i 是扫描游标，同时也是"已消费字节数"的账）
            int i = 0;
            while (i + 2 < pool_len) // +2：至少还剩 2 字节才够拼一次帧头（01 03）；i+2<pool_len：剩不足 2 字节时停扫等下轮追加（半帧留池）
            {
                // 命中帧头（01 03）才往下走（噪声字节逐个跳过，假帧头靠 CRC 兑底）
                if (pool[i] == ADDR && pool[i + 1] == FUNC)
                {
                    int total = frame_total_len(pool, i); // 由帧内 byte_count 算总长（3+数据+2）
                    /* 门卫①：长度合法性——total 来自线上不可信，必须在 crc16 之前先夹住 */
                    if (total < 7 || total > FRAME_MAX || ((total - 5) % 2) != 0)
                    {
                        i++;      /* 只能逐字节挪！total 不可信，i+=total 会跳过池尾连累 memmove */
                        continue; /* 跳过本轮剩下的完整性/CRC/拷贝，重新扫下一个字节 */
                    }
                    /* 门卫②：池内完整性——帧还没收全，半帧留池等下轮（原有逻辑） */
                    if (i + total > pool_len)
                    {
                        break; // 帧不齐：半帧留池，等下轮 read 追加后再扫（不在这里死等，先退出内层去读新字节）
                    }
                    uint16_t calc = crc16(pool + i, total - 2); // 只算正文（除末尾 2 字节 CRC）：算出的与帧尾携带的对比，不通过=噪声混入的假帧，丢（total-2：CRC 位自身不参与校验）
                    // 拼出帧尾携带的crc校验位（小端：低字节在前，拼时高字节左移 8 位）；i+total-1/i+total-2：帧尾两字节下标 = 起点+总长-1/-2（小端：高字节在后，取值时左移拼）
                    uint16_t recv = pool[i + total - 1] << 8 | pool[i + total - 2];
                    if (calc == recv)
                    {
                        atomic_fetch_add(&g_frames_recv, 1);
                        LOG_DEBUG("[捞帧] 第%d帧 共%d字节", got + 1, total);
                        printf("[捞帧] 第%d帧 共%d字节\n", ++got, total);
                        // 申请数据包空间，然后逐字节组装数据包（完整帧出池：拷贝走，池里不再留）
                        frame_t *adu = malloc(sizeof(frame_t));
                        if (!adu)
                        { // OOM：丢这帧（total 已过门卫①可信，i+=total 安全）
                            LOG_WARN("[捞帧] malloc 失败，丢帧");
                            atomic_fetch_add(&g_frames_drop, 1);
                            i += total;
                            break;
                        }
                        for (int j = 0; j < total; j++)
                        {
                            adu->data[j] = pool[i + j]; // 从命中点 i 开始逐字节拷进数据包（j 扫 0~total-1，取 pool[i+j]）
                        }
                        adu->len = total;
                        // 把数据包和消费函数指针 交给生产函数（生产函数把数据包入队然后叫醒消费者）
                        if (threadPool_push(tp, threadPool_pop, adu) != 0)
                        {
                            free(adu); // 入队失败：丢数据包（背压保护：池满/停业时宁可丢一帧不堵生产者）
                            atomic_fetch_add(&g_frames_drop, 1);
                        }
                        fflush(stdout);
                        usleep(100000); // 100ms 消费间隔保护：给 worker 留出处理窗口，防连续投任务堆爆队列（08-23 背压教训）
                        i += total;     // 游标跳过整帧，接着扫下一帧（同池可能有多帧）
                        break;          // 捞出就停：本轮只捞一帧就出内层重新 read（防同池多帧连续捞导致消费堆积）
                    }
                    else
                    {
                        LOG_WARN("[捞帧] 校验位错误 损坏值");
                        atomic_fetch_add(&g_frames_drop, 1);
                        printf("[捞帧] 校验位错误 损坏值\n");
                        // CRC 不过：假帧头（噪声混流），i++ 继续往后扫（不是整池作废）
                    }
                }
                i++;
            }
            if (i > 0) // 有消费才搬：i=0（没扫出任何东西）时 memmove 是无用功，跳过；i>0 才值得搬（没命中时 i 也在走（逐字节扫噪声），所以噪声字节同样被清理出池）
            {
                memmove(pool, pool + i, pool_len - i); // memmove 允许重叠：把未消费的残余（从 i 起）挪到池头，一锅端（不能用 memcpy，源目标重叠）
                pool_len -= i;                         // 水位线同步减：搬走多少减多少，残余长度 = 原长-已扫（残余留在池头等下轮 read 追加）
            }
        }
    }
    close(fd); // B3 后真实可达：g_running 翻 0 → 外层循环退出 → 串口在这里关（谁打开谁关：fd 由生产者用，归生产者收）
    return NULL;
}

/**
 *  --------------消费者调度函数-------------
 *      职责：
 *            1.判断队列有没有任务（没任务才等：有任务直接拿）
 *            2.睡觉等待生产者叫醒（wait 同时放锁+睡觉，醒来自动重新拿锁）
 *            3.然后把任务节点取出 执行消费函数（锁内取节点，锁外执行：消费耗时长，不占锁）
 *            4.然后释放申请的任务节点空间（取走的节点自己负责清）
 */
static void *threadPool_consumer(void *arg)
{
    threadPool_t *tp = (threadPool_t *)arg; // 拿池子：用它摸队列/锁/停业牌（3 个 worker 共拿同一个池）
    while (1)
    {
        // 先拿锁（摸队列前必锁：队列是唯一并发修改点）
        pthread_mutex_lock(&tp->lock);
        // 如果状态是停业 并且没有任务 那么放锁退出（存量清完+挂停业牌=真下班；还有任务就先干完再退）
        if (tp->shutdown && tp->head == NULL)
        {
            pthread_mutex_unlock(&tp->lock);
            break;
        }
        // 如果状态是营业 并且没有任务（&& 双条件：空队列且没停业才睡；停业时不睡，要走上面的退出分支）
        while (tp->head == NULL && !tp->shutdown)
        {
            // 只有消费者需要等待，因为会出现空队列，但是不会出现满队列（生产者只管塞，不阻塞）
            pthread_cond_wait(&tp->cond, &tp->lock); // wait = 放锁+睡觉原子操作；被 signal/broadcast 叫醒后自动重新拿锁；用 while 不用 if：防虚假唤醒，醒来复查条件（防惊群虚假唤醒：醒来必须复查条件）
        }
        // 08-29 真凶修复：内层 while 有两个出口（有任务了 / 停业了）；空队睡觉的 worker 被停业广播叫醒时 head 仍是 NULL，若直接往下取 t->next = 空指针解引用 = Segmentation fault！醒来必须复查走了哪个出口
        if (tp->shutdown && tp->head == NULL)
        {
            pthread_mutex_unlock(&tp->lock);
            break; // 被停业牌叫醒且无存量：直接下班
        }
        /* 此时状态是营业 如果队列有任务 取出（锁内只做摘节点，不做执行） */
        // 新建任务节点拿到头指针（摘走头节点）
        task_t *t = tp->head;
        // 头指针指向 原头任务节点 的下一个节点（队列前移一位）
        tp->head = t->next;
        // 如果此时头指针是空 说明队列没有任务那么头尾指针复位（取空了：尾指针也归零，下次插入走空队分支）
        if (tp->head == NULL)
        {
            tp->tail = NULL;
        }
        pthread_mutex_unlock(&tp->lock); // 先解锁再执行：消费（上报/落库）耗时长，占着锁会堵住生产（锁内取任务，锁外执行）
        // 执行消费函数（函数指针塞在节点里：无启动者，worker 被叫醒后自己跑）
        t->pf(t->arg); // 执行消费函数：t->pf=threadPool_pop，t->arg=数据包（参数随任务节点走：每个任务自带上下文）
        // t的任务是 创建节点来交换头指针位置，执行取走的任务函数 然后释放（节点用完即弃）
        free(t);
    }
    return NULL;
}

/* ======================================== 主函数区 ================================ */
int main(int argc, char *argv[]) /* B3 升级：收参数（-d 开关） */
{
    /* 带 -d 参数 = 守护模式（生产：后台常驻）；不带 = 前台调试（终端能看打印，上板验证用） */
    if (argc > 1 && strcmp(argv[1], "-d") == 0)
    {
        daemonize(); /* 必须最先：fork 后只有子进程走到这里，之后的初始化全部在守护体里发生 */
    }
    /* =========== 信号挂号（B3 新增）：kill 来了走优雅退出，不暴毙 =========== */
    signal(SIGTERM, on_signal); /* kill 默认发的就是 SIGTERM(15) */
    signal(SIGINT, on_signal);  /* 前台调试时 Ctrl+C(2) 也走同一条收尾路 */
    // 日志初始化：门槛压到底=全收（调试期）；上线改 LV_WARN 只记异常；失败不退出，裸奔模式继续（日志自己起不来不能拖累网关主链路）
    // 旧：log_init("/tmp/gateway.log", LV_DEBUG)
    if (log_init("/tmp/gateway.log", LV_INFO) != 0)    // 72h：滤掉每帧DEBUG噪音，只留INFO/WARN/ERROR
        fprintf(stderr, "日志初始化失败，裸奔模式\n"); // 0=日志文件路径（板子临时区，重启清空符合日志定位）；LV_DEBUG=0（最低门槛全收）；此时日志没开门，只能靠终端喊一声（日志失败不能拖累主链路，所以只警告不退出）
    LOG_INFO("网关 v4 启动 | build=%s",BUILD_ID);
    // 打开文件 sql数据库（相对路径：在哪个目录跑，库就建在哪——上板固定 /root 下跑）
    if (sqlite3_open("gateway.db", &db) != SQLITE_OK) // &db：结果句柄写入全局指针，之后所有函数共用这个连接（sqlite3_open 句柄通过第二参数传出：传 &db，失败时 db 仍可能非空（带错误信息），所以失败分支也 close）
    {
        LOG_ERROR("打开数据库失败");
        fprintf(stderr, "打开数据库失败\n");
        sqlite3_close(db);
        return 1;
    }
    sqlite3_busy_timeout(db, 3000);       // 抢锁时等 3s 再放弃，吸收瞬时争用（默认 0=立即 BUSY 失败）
    sqlite3_extended_result_codes(db, 1); // 开扩展错误码：BUSY 能细分 RECOVERY/SHAREDTABLE，72h 排障看得清
    // 建表函数（幂等：IF NOT EXISTS，重复跑安全）
    create_table(db);
    // MQTT全局初始化（进程级一次，对应末尾 lib_cleanup）
    mosquitto_lib_init();
    // 创建MQTT的客户端：NULL=自动生成唯一 client ID（实测坑：写死 ID 会互踢残留进程，08-11 实锤）；true=clean session 每次新会话；NULL=无用户数据（无回调上下文）
    mq = mosquitto_new(NULL, true, NULL);
    if (mq == NULL)
    {
        LOG_ERROR("MQTT 客户端创建失败");
        fprintf(stderr, "MQTT 客户端创建失败\n");
        sqlite3_close(db);
        return 1;
    }
    // 08-29 真回执升级：挂三个回调（都在 lib 后台线程触发）
    mosquitto_connect_callback_set(mq, on_connect_cb);       // 连接结果（rc=0 成功）
    mosquitto_disconnect_callback_set(mq, on_disconnect_cb); // 断联通知（翻账本接管牌）
    mosquitto_publish_callback_set(mq, on_publish_cb);       // QoS1 真回执（PUBACK 到→盖戳）
    // 连接broker：host=VM 上的 mosquitto，1883=MQTT 标准端口，60=keepalive 秒（心跳周期：60 秒内无报文则发 PINGREQ 保活）
    if (mosquitto_connect(mq, host, 1883, 60) != MOSQ_ERR_SUCCESS)
    {
        LOG_ERROR("MQTT 连接失败");
        fprintf(stderr, "MQTT 连接失败\n");
        sqlite3_close(db);
        mosquitto_destroy(mq);
        return 1;
    }
    // 网络循环开启：库自起后台线程跑收发（不阻塞主流程；⚠️ 这线程收尾必须 loop_stop 接回家，见末尾收尾四步曲）
    mosquitto_loop_start(mq);
    sleep(1); // 1 秒：等 broker 连接建立（connect 异步生效，立刻发会丢首帧）

    // 打开设备资源：板载 UART6；O_NOCTTY=不把它当控制终端（防串口信号干扰进程；不设的话该串口可能成为进程的控制终端，信号干扰主流程）
    fd = open(SERIAL_DEV, O_RDWR | O_NOCTTY);
    if (fd < 0)
    {
        LOG_ERROR("串口 /dev/ttymxc5 打开失败");
        perror("open");
        return 1;
    }
    // 配置串口（8N1 + raw + 115200 + VMIN=0/VTIME=10，细节见 termios_init）
    termios_init(fd, &tty);
    // 开线程：3 个 worker 消化短任务（数量依据：采集速率 > 上报速率，多 worker 防阻塞堆积；纯短任务消费者）
    threadPool_t *tp = threadPool_create(3);
    if (tp == NULL)
    {
        LOG_ERROR("线程池创建失败");
        fprintf(stderr, "线程池创建失败\n");
        close(fd);
        mosquitto_destroy(mq);
        sqlite3_close(db);
        exit(1);
    }
    // 开生产者线程：长驻循环单独 pthread_create，不进线程池（进池会永久霸占 worker 饿死池子——任务判据：会不会结束）
    pthread_t tid_prod;
    pthread_create(&tid_prod, NULL, producer, tp);
    pthread_join(tid_prod, NULL); // 主线程在此等生产者归队（B3 前这里永等；B3 后 kill 翻牌→生产者退出循环→join 返回→下面收尾链真实执行）
    // 退出函数（顺序：先停池（等存量消费完）→ MQTT 收尾四步曲 → 再关库关日志；由 SIGTERM 链触发：kill → 翻牌 → 生产者归队 → 走到这里）
    // 08-29 崩溃定位插桩版：每步一颗面包屑 LOG，崩后日志最后一条 [shutdown] = 最后一个活着的步骤，下一步=案发现场（抓到凶手后面包屑可留可删，留着=退出留痕不亏）
    LOG_INFO("[shutdown] 生产者归队，停池...");
    threadPool_shutdown(tp);
    LOG_INFO("[shutdown] 池停，MQTT disconnect...");
    mosquitto_disconnect(mq); // ① 跟 broker 说再见（网络层告别）
    LOG_INFO("[shutdown] disconnect 完，loop_stop...");
    mosquitto_loop_stop(mq, false); // ② 给 loop_start 的后台线程按停止钮，等它回家（false=等退出）
    LOG_INFO("[shutdown] loop_stop 完，destroy...");
    mosquitto_destroy(mq); // ③ 释放客户端对象
    LOG_INFO("[shutdown] destroy 完，lib_cleanup...");
    mosquitto_lib_cleanup(); // ④ 线程回家、对象释放后才清库地基
    LOG_INFO("[shutdown] 清库完，关 db...");
    sqlite3_close(db);
    LOG_INFO("[shutdown] db 关，关日志，再见");
    log_close(); // 日志最后关：上面的退出过程本身的日志也能记上（关门顺序：日志永远是最后一个关的）

    return 0;
}