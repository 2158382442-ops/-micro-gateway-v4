#ifndef LOG_H            /* 头文件守卫：防止被 #include 两次，跟 http_report.h 同款 */
#define LOG_H

/* 级别即数字：数字越大越严重，比较运算就是过滤逻辑 */
enum {
    LV_DEBUG = 0,   /* 调试噪音：捞帧细节，每轮心跳 */
    LV_INFO  = 1,   /* 正常流水：上报成功、补传完成 */
    LV_WARN  = 2,   /* 异常但能活：CRC 错一帧、HTTP 报失败 */
    LV_ERROR = 3,   /* 致命：串口打不开、数据库建不起来 */
};

/* 三件套：开门 / 记账 / 关门 */
int  log_init(const char *path, int min_level);  /* 0 成功 / -1 失败 */
void log_write(int level, const char *fmt, ...); /* 变长：跟 printf 同款 */
void log_close(void);

/* 语法糖：##__VA_ARGS__ 里 __ 是双下划线；
   ## 的作用：可变参为空时吃掉多余的逗号 */
#define LOG_DEBUG(fmt, ...) log_write(LV_DEBUG, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  log_write(LV_INFO,  fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  log_write(LV_WARN,  fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) log_write(LV_ERROR, fmt, ##__VA_ARGS__)

#endif /* LOG_H */