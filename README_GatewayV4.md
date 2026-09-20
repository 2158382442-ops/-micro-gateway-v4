# 微型网关 v4 —— 嵌入式 Linux 工业数据采集网关

> 运行于 **i.MX6ULL**（ARM Cortex-A7，Linux 5.x）的生产级 Modbus RTU 主站网关。
> RS485 轮询从机 → cJSON 解析 → SQLite 缓存 → MQTT 上报，全链路独立开发。

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│  i.MX6ULL 开发板（ARM Linux）                                       │
│                                                                     │
│  ┌───────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ 生产者线程 │───▶│ 任务队列      │───▶│ 3× 消费者 Worker         │  │
│  │ (RS485    │    │ (互斥锁 +     │    │  cJSON → SQLite → MQTT   │  │
│  │  轮询)    │    │  条件变量)    │    │  → 离线补传              │  │
│  └───────────┘    └──────────────┘    └──────────────────────────┘  │
│       │                                        │                     │
│  /dev/ttymxc5                            libmosquitto                │
│  + gpio118 (MAX485 方向控制)              + sqlite3                   │
└───────┼──────────────────────────────────────────┼───────────────────┘
        │ RS485 总线 (A/B/GND)                      │ TCP 1883
        ▼                                          ▼
┌──────────────┐                         ┌──────────────────┐
│ Modbus 从机  │                         │  MQTT Broker     │
│ (micro_slave │                         │  (mosquitto)     │
│  PC/VM 上)   │                         └──────────────────┘
└──────────────┘
```

## 核心特性

| 类别 | 实现 |
|:--|:--|
| **并发模型** | 生产者-消费者线程池（3 worker），互斥锁 + 条件变量任务队列 |
| **可靠性** | 守护进程化（fork + setsid），SIGTERM 优雅退出（g_running 标志位） |
| **数据链路** | RS485 Modbus RTU 主站 → CRC-16 捞帧 → cJSON 组包 → SQLite 离线缓存 → MQTT QoS1 上报 |
| **离线兜底** | 未送达帧记录在 SQLite（sent=0），重连后自动补传销账 |
| **可观测性** | C11 `<stdatomic.h>` 无锁计数器（recv/sent/drop）+ 60s 心跳日志 + build-id 时间戳 |
| **日志系统** | 4 级分级滚动日志（DEBUG/INFO/WARN/ERROR），1MB 轮转，线程安全（pthread_mutex） |
| **安全加固** | 缓冲区溢出三门卫（REG_MAX 钳位、dump_json 边界检查）、read() 三分流（数据/错误/中断） |

## 技术栈

- **语言**：C11（`-std=gnu11`）
- **线程**：POSIX pthreads
- **存储**：SQLite3（嵌入式数据库，离线缓存）
- **消息**：libmosquitto（MQTT 3.1.1 客户端）
- **序列化**：cJSON
- **协议**：Modbus RTU（RS485 半双工，CRC-16/MODBUS）
- **硬件**：NXP i.MX6ULL，正点原子 ATK-DNF103 V2.5 开发板，MAX485 收发器
- **构建**：arm-linux-gnueabihf-gcc 交叉编译

## 编译

### 前置条件

- ARM 交叉编译工具链：`arm-linux-gnueabihf-gcc`
- ARM 库：`libsqlite3.so`、`libmosquitto.so`（位于 `/home/aelys/armlib/lib/`）
- 头文件：`sqlite3.h`、`mosquitto.h`（位于 `/home/aelys/arm/lib/include/`）

### 编译命令

```bash
chmod +x ./build_gw.sh && ./build_gw.sh
```

或手动：

```bash
arm-linux-gnueabihf-gcc -std=gnu11 gateway.c log.c cJSON.c -o gateway -pthread \
  -I/home/aelys/arm/lib/include -L/home/aelys/armlib/lib \
  -lsqlite3 -lmosquitto -Wl,--allow-shlib-undefined
```

### 推送到板子

```bash
adb push gateway /root/
adb shell "chmod +x /root/gateway"
```

## 运行

### 全链路测试（需 RS485 从机 + MQTT broker）

```bash
# VM：启动 MQTT broker
sudo mosquitto -d

# VM：启动 Modbus 从机
./micro_slave

# 板子：后台启动网关
nohup ./gateway > /tmp/gateway_run.log 2>&1 &

# VM：验证 MQTT 数据
mosquitto_sub -t gw/registers -v
```

期望输出：
```json
gw/registers {"registers": [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]}
```

### 日志查看

```bash
tail -f /tmp/gateway.log          # 实时日志
grep '\[心跳\]' /tmp/gateway.log  # 心跳记录（每 60s 一条）
grep WARN /tmp/gateway.log        # 只看异常
```

## 硬件接线

| 信号 | 板子接口 | GPIO | 备注 |
|:--|:--|:--|:--|
| RS485 TX/RX | J16 (UART6) | — | `/dev/ttymxc5` @ 115200 8N1 |
| RS485 方向控制 | GPIO3 | **118** | MAX485 DE/RE 切换 |
| RS485 总线 | J16 端子 | A / B / **GND** | 三线，隔离模块必须共地 |

## 项目结构

```
GateWayV4/
├── gateway.c        # 主程序（生产者 + 消费者 + 守护进程）
├── log.h / log.c    # 分级滚动日志模块
├── cJSON.h / cJSON.c # JSON 序列化（MIT 开源库）
├── micro_slave.c    # Modbus RTU 从机（x86，测试用）
├── build_gw.sh      # 交叉编译脚本
└── .gitignore
```

## 稳定性验证

72 小时连续运行，六项成功判据：
1. 进程存活（无崩溃/重启）
2. 心跳间隔 ≤ 65s（无线程饥饿）
3. `drop` 计数器保持 0（无丢帧）
4. SQLite 缓存增长后稳定（补传正常）
5. MQTT 订阅端持续收到数据
6. 日志轮转触发（磁盘不撑爆）

## 许可

MIT
