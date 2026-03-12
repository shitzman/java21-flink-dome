#!/bin/bash
# RabbitMQ 消费者启动脚本 (Linux/Mac)
# 作者: Claude Code
# 创建时间: 2026-03-12

echo "========================================"
echo "RabbitMQ 消费者测试脚本"
echo "========================================"
echo

# 检查 Python
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 Python 3，请先安装 Python 3.x"
    echo "Ubuntu/Debian: sudo apt-get install python3 python3-pip"
    echo "MacOS: brew install python"
    echo "CentOS/RHEL: sudo yum install python3 python3-pip"
    exit 1
fi

# 检查依赖
echo "检查依赖安装..."
if ! python3 -c "import pika" &> /dev/null; then
    echo "未找到 pika 库，正在安装..."
    pip3 install pika
    if [ $? -ne 0 ]; then
        echo "错误: 安装 pika 失败"
        exit 1
    fi
    echo "✓ pika 安装成功"
else
    echo "✓ pika 已安装"
fi

echo
echo "========================================"
echo "启动消费者..."
echo "按 Ctrl+C 停止程序"
echo "========================================"
echo

# 运行消费者
python3 consume_rabbitmq.py

echo
echo "========================================"
echo "消费者已停止"
echo "========================================"