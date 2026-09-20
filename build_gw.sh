#!/bin/bash
# ============================================================
# 微型网关 v4（重构版）交叉编译脚本
# 重构日收口：源 micro_gateway.c → gateway.c，旧名退役、划个句号
# ------------------------------------------------------------
# -std=gnu11 : R6 的 <stdatomic.h>（atomic_ulong / atomic_fetch_add 计数器）
#              是 C11 特性；老交叉工具链默认 gnu89/gnu99 编不过，必须显式加。
# cJSON.c    : 直接编源文件，不用 -lcjson。
# 链接库     : -lpthread 线程池 / -lsqlite3 缓存 / -lmosquitto MQTT 上报。
# ============================================================
arm-linux-gnueabihf-gcc -std=gnu11 gateway.c log.c cJSON.c -o gateway -pthread -I/home/aelys/arm/lib/include -L/home/aelys/armlib/lib -lsqlite3 -lmosquitto -Wl,--allow-shlib-undefined
