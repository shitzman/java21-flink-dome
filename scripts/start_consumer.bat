@echo off
REM RabbitMQ 消费者启动脚本 (Windows)
REM 作者: Claude Code
REM 创建时间: 2026-03-12

echo ========================================
echo RabbitMQ 消费者测试脚本
echo ========================================
echo.

REM 检查 Python
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到 Python，请先安装 Python 3.x
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 检查依赖
echo 检查依赖安装...
pip show pika >nul 2>&1
if errorlevel 1 (
    echo 未找到 pika 库，正在安装...
    pip install pika
    if errorlevel 1 (
        echo 错误: 安装 pika 失败
        pause
        exit /b 1
    )
    echo ✓ pika 安装成功
) else (
    echo ✓ pika 已安装
)

echo.
echo ========================================
echo 启动消费者...
echo 按 Ctrl+C 停止程序
echo ========================================
echo.

REM 运行消费者
python consume_rabbitmq.py

echo.
echo ========================================
echo 消费者已停止
echo ========================================
pause