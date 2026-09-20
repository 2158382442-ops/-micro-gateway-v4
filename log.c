#include <stdio.h>
#include <stdarg.h> /* va_list 三件套在这个头里 */
#include <string.h>
#include <time.h>    /* time/localtime/strftime */
#include <pthread.h> /* 互斥锁 */
#include "log.h"

#define LOG_MAX_SIZE (1024 * 1024) /* 滚动门槛：1MB */

static FILE *g_fp = NULL;
static int g_min_level = LV_INFO;                          /* 门槛：init 时改 */
static char g_path[128];                                   /* 存一份路径，滚动重开要用 */
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER; /* 静态初始化免 init */

/* 初始化：把日志文件路径和门槛等级拷贝成模块自己的参数 */
int log_init(const char *path, int min_level)
{
    strncpy(g_path, path, sizeof(g_path) - 1); /* 留一字节给\0 */
    g_fp = fopen(g_path, "a");                 /* a=追加：续写尾部不清空历史 */
    if (!g_fp)
        return -1;
    g_min_level = min_level;
    return 0;
}

/* 写日志：一次调用 = 一条完整日志（拼行、补换行、落盘全包办） */
void log_write(int level, const char *fmt, ...)
{
    if (!g_fp || level < g_min_level)
        return; /* 没开门/不够格：快路径直接走 */

    pthread_mutex_lock(&g_lock); /* 锁住整个写过程，多线程不交错 */
    /* -----滚动检查----- */
    /* ftell 报告写头位置：从文件开头算起第几个字节 */
    if (ftell(g_fp) > LOG_MAX_SIZE) /* 写头超 1MB → 滚动 */
    {
        char bak[140];
        snprintf(bak, sizeof(bak), "%s.1", g_path);
        fclose(g_fp);
        // 给满载的文件 g_path 改名为 bak
        rename(g_path, bak);
        // 老名字空出来，新建空文件继续记
        g_fp = fopen(g_path, "a");
        if (!g_fp)
        {
            pthread_mutex_unlock(&g_lock);
            return;
        }
    }
    /* -------拼一行--------- */
    char line[512];
    const int cap = (int)sizeof(line) - 2; // 预留 2 字节给末尾 '\n' 和 '\0'
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    int pos = strftime(line, sizeof(line), "%m-%d %H:%M:%S ", t);
    if (pos < 0 || pos > cap)
        pos = 0; // strftime 塞不下/异常，防御归零

    static const char *names[] = {"DEBUG", "INFO", "WARN", "ERROR"};
    int n = snprintf(line + pos, sizeof(line) - pos, "[%s] ", names[level]);
    if (n > 0)
        pos += n; // n<0=编码错，别把负数加进 pos
    if (pos > cap)
        pos = cap; // 钳位①：保证下一行 sizeof(line)-pos 不为负

    va_list args;
    va_start(args, fmt);
    n = vsnprintf(line + pos, sizeof(line) - pos, fmt, args);
    va_end(args);
    if (n > 0)
        pos += n;
    if (pos > cap)
        pos = cap; // 钳位②：返回值是"想写的长度"，可能远超buffer，钳回

    line[pos++] = '\n'; // pos≤cap=510 → line[510] 安全
    line[pos] = '\0';   // line[511] 安全
    fputs(line, g_fp);
    fflush(g_fp);
    pthread_mutex_unlock(&g_lock);
}

/* 日志关闭：拿锁看文件在不在，在就关，释放文件指针；不在就放锁 */
void log_close(void)
{
    pthread_mutex_lock(&g_lock);
    if (g_fp)
    {
        fclose(g_fp);
        g_fp = NULL;
    }
    pthread_mutex_unlock(&g_lock);
}