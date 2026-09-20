/**
 *  从机侧任务：
 *      1.采集物理数据
 *      2.组装响应帧
 *      3.发送数据帧
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <termios.h>
#include <stdint.h>
#include <linux/serial.h> // 加在文件头部 include 区
#include <sys/ioctl.h>

static void init_termios(int fd, struct termios *tty)
{
    // 先读串口信息
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
    // 配置read静默阈值  从机死等
    tty->c_cc[VMIN] = 1;
    tty->c_cc[VTIME] = 0;
    // 配置写入句柄
    tcsetattr(fd, TCSANOW, tty);
}

static uint16_t crc16(uint8_t *frame, int len)
{
    uint16_t crc = 0xFFFF;
    for (int i = 0; i < len; i++)
    {
        crc ^= frame[i];
        for (int bit = 0; bit < 8; bit++)
        {
            if (crc & 0x0001)
            {
                crc >>= 1;
                crc ^= 0xA001;
            }
            else
            {
                crc >>= 1;
            }
        }
    }
    return crc;
}

int main(void)
{
    int fd = open("/dev/ttyUSB0", O_RDWR | O_NOCTTY);
    if (fd < 0)
    {
        perror("open");
        return -1;
    }

    struct termios tty;
    /* 开始配置串口 */
    init_termios(fd, &tty);
    /* 模拟数据 */
    uint16_t regs[10];
    for (int i = 0; i < 10; i++)
    {
        regs[i] = i * 100;
    }
    // 接收池
    uint8_t buf[256];
    // 响应帧
    uint8_t resp[32];

    while (1)
    {
        /* 接收区 */
        int n = read(fd, buf, sizeof(buf));
        if (n <= 0)
        {
            continue;
        }
        // 帧大小：地址+功能码+2起始寄存器地址+2寄存器数量+2crc
        if (n < 8)
        {
            printf("[从机] 未收到完整帧\n");
            continue;
        }
        // 检查帧头
        if (buf[0] != 0x01)
        {
            printf("[从机] 不是我的地址\n");
            continue;
        }
        uint16_t cale = crc16(buf, 6);
        uint16_t build_crc = buf[6] | buf[7] << 8;
        if (cale != build_crc)
        {
            printf("[从机] crc校验失败 损坏值丢弃");
            continue;
        }
        /* 解析区 */
        // 主机请求帧：地址+功能码+2起始寄存器地址+2寄存器数量+2crc
        uint16_t start_addr = buf[2] << 8 | buf[3];
        // 寄存器值数量  数量*2等于我要发送的数据字节数
        int val_count = buf[4] << 8 | buf[5];
        // 发送的数据字节数
        int byte_count = val_count * 2;
        if (start_addr + val_count > 10)
        {
            printf("[从机] 越界请求，拒绝\n");
            continue;
        }

        /* 组装响应帧 */
        // 从机响应帧：地址+功能码+字节数+数据+校验位
        resp[0] = 0x01;
        resp[1] = 0x03;
        resp[2] = byte_count;
        /* 数据位组装 */
        for (int i = 0; i < val_count; i++)
        {
            uint16_t val = regs[start_addr + i];
            resp[3 + i * 2] = val >> 8;
            resp[4 + i * 2] = val & 0xFF;
        }
        int total = 3 + byte_count + 2;
        uint16_t crc = crc16(resp, total - 2);
        resp[total - 2] = crc & 0xFF;
        resp[total - 1] = crc >> 8;

        usleep(50000);
        int wr = write(fd, resp, total);
        printf("[从机] 收到请求，回响应帧 共%d个字节 write返回%d\n", total, wr);
    }

    close(fd);
    return 0;
}