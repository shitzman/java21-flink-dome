# RabbitMQ 消费者测试脚本

用于测试 Flink 应用发送到 RabbitMQ 的商户统计数据的 Python 脚本。

## 安装依赖

```bash
pip install -r requirements.txt

# 或者直接安装 pika
pip install pika
```

## 使用说明

### 基本用法

1. **启动 Flink 应用**（确保 RabbitMQ 已在运行）
2. **运行消费者脚本**：

```bash
# 持续监听模式（默认）
python consume_rabbitmq.py

# 消费10条消息后退出
python consume_rabbitmq.py --count 10

# 测试连接（不消费消息）
python consume_rabbitmq.py --test
```

### 自定义连接参数

如果 RabbitMQ 不在本地或使用非默认配置：

```bash
# 自定义连接参数
python consume_rabbitmq.py --host 192.168.1.100 --port 5673 --username admin --password secret

# 自定义队列名称
python consume_rabbitmq.py --queue my-custom-queue
```

### 高级选项

```bash
# 设置超时（60秒后自动退出）
python consume_rabbitmq.py --timeout 60

# 消费5条消息，60秒超时
python consume_rabbitmq.py --count 5 --timeout 60

# 查看所有帮助选项
python consume_rabbitmq.py --help
```

## 输出示例

运行脚本后，会看到类似以下输出：

```
正在连接到 RabbitMQ: localhost:5672/
✓ 成功连接到 RabbitMQ
✓ 队列: merchant-stats-queue
--------------------------------------------------------------------------------
开始监听队列 'merchant-stats-queue'...
按 Ctrl+C 停止
================================================================================
【商户统计】14:30:25
商户号: M001
事件类型: INCREMENTAL
更新时间: 2024-03-12 14:30:24
总金额: ¥1500.00
成功金额: ¥1000.00
退款金额: ¥300.00
撤销金额: ¥200.00
净额: ¥500.00
原始数据: {"mchNo":"M001","totalAmount":150000,"successAmount":100000,"refundAmount":30000,"cancelAmount":20000,"netAmount":50000,"updateTime":1709728224000,"eventType":"INCREMENTAL"}
--------------------------------------------------------------------------------
```

## 消息格式

Flink 应用发送到 RabbitMQ 的消息是 JSON 格式，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| mchNo | string | 商户号 |
| totalAmount | long | 总金额（分） |
| successAmount | long | 成功金额（分） |
| refundAmount | long | 退款金额（分） |
| cancelAmount | long | 撤销金额（分） |
| netAmount | long | 净额（分） |
| updateTime | long | 更新时间戳（毫秒） |
| eventType | string | 事件类型（SNAPSHOT/INCREMENTAL） |

## 注意事项

1. **确保 RabbitMQ 服务已启动**：脚本默认连接 `localhost:5672`，使用 `guest/guest` 认证
2. **队列持久化**：Flink Sink 配置了持久化队列，消息不会丢失
3. **网络连接**：如果连接失败，请检查防火墙和网络配置
4. **权限问题**：确保用户有访问指定队列的权限

## 错误排查

### 连接失败
```bash
# 检查 RabbitMQ 服务状态
rabbitmqctl status

# 检查队列是否存在
rabbitmqadmin list queues
```

### 权限问题
```bash
# 查看用户权限
rabbitmqctl list_users
rabbitmqctl list_permissions
```

### 安装问题
```bash
# 如果 pip 安装失败，尝试使用 conda
conda install -c conda-forge pika
```

## 脚本特性

- ✅ 自动重连机制
- ✅ JSON 解析和格式化输出
- ✅ 支持多种消费模式
- ✅ 完整的错误处理
- ✅ 命令行参数支持
- ✅ 超时和自动退出功能