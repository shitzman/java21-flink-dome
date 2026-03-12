## 技术组件
1. Flink 版本：1.20.1
2. CDC 版本：3.2.0
3. Java 版本：Java 21

## 运行
java8+ 环境需要设置vm参数
```shell
--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED   --add-opens java.base/java.time=ALL-UNNAMED
```

```html

  ========== 核心架构 ==========
  ┌─────────────────────────────────────────────┐
  │          MySQL CDC Source (1.20)             │
  │   - 全量快照（INITIAL_ONLY）                 │
  │   - Binlog 增量事件                          │
  └────────────┬────────────────────────────────┘
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
   快照数据            增量数据
   (第一阶段)          (第二阶段)
      │                 │
      └────────┬────────┘
               ▼
      KeyedProcessFunction
      (两套处理逻辑)
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
   快照处理逻辑      增量处理逻辑
   (覆盖状态)        (累加/累减)
      │                 │
      └────────┬────────┘
               ▼
     ┌──────────────────┐
     │ 实时统计结果     │
     │ + Metrics 监控   │
     │ + 诊断信息       │
     └──────────────────┘
```



## 🔌 下游集成示例

### Sink 到 Kafka

```java
merchantStats.addSink(new FlinkKafkaProducer<>(
    "merchant-stats",
    (event, timestamp) -> {
        JSONObject json = new JSONObject();
        json.put("mchNo", event.getMchNo());
        json.put("netAmount", event.getNetAmount());
        json.put("updateTime", event.getUpdateTime());
        return json.toJSONString().getBytes();
    },
    kafkaProperties
)).name("Sink-Kafka");
```

### Sink 到 MySQL

```java
merchantStats.addSink(new JdbcSink.Builder<MerchantStats>()
    .setDBUrl("jdbc:mysql://localhost:3306/analytics")
    .setQuery("INSERT INTO merchant_stats(mch_no, net_amount, update_time) VALUES(?, ?, ?)")
    .setBatchSize(1000)
    .build()
).name("Sink-Database");
```

### Sink 到 RabbitMQ

```java
merchantStats.addSink(new MerchantStatsRabbitMQSink(
    "192.168.0.99",  // RabbitMQ 主机
    5672,            // RabbitMQ 端口
    "admin",         // 用户名
    "StrongPassword123", // 密码
    "merchant-stats-queue" // 队列名称
)).name("RabbitMQ-Sink").setParallelism(1);
```

## 🐰 RabbitMQ 集成与测试

### 1. 实现概述

项目新增了 RabbitMQ Sink 功能，将实时商户统计数据发送到 RabbitMQ 队列，供下游系统消费。

**核心组件：**
- `MerchantStatsRabbitMQSink.java` - 自定义 RabbitMQ Sink 实现
- `scripts/consume_rabbitmq.py` - Python 测试消费脚本
- `pom.xml` - 新增 RabbitMQ 依赖

### 2. 依赖配置

在 `pom.xml` 中添加了以下依赖：

```xml
<!-- Flink RabbitMQ Connector -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-rabbitmq</artifactId>
  <version>${flink.version}</version>
</dependency>

<!-- RabbitMQ Client -->
<dependency>
  <groupId>com.rabbitmq</groupId>
  <artifactId>amqp-client</artifactId>
  <version>5.16.0</version>
</dependency>
```

### 3. MerchantStatsRabbitMQSink 特性

- **自动重连机制**：支持配置最大重试次数和重试间隔
- **JSON 序列化**：使用 FastJSON2 将 `MerchantStats` 对象转为 JSON
- **连接状态监控**：自动检测连接状态，异常时尝试恢复
- **资源安全释放**：确保连接和通道正确关闭
- **可配置参数**：支持完整的 RabbitMQ 连接参数

### 4. 数据格式

发送到 RabbitMQ 的消息为 JSON 格式：

```json
{
  "mchNo": "商户号",
  "totalAmount": 总金额（分）,
  "successAmount": 成功金额（分）,
  "refundAmount": 退款金额（分）,
  "cancelAmount": 撤销金额（分）,
  "netAmount": 净额（分）,
  "updateTime": 更新时间戳（毫秒）,
  "eventType": "SNAPSHOT/INCREMENTAL"
}
```

### 5. 测试脚本使用

#### 安装依赖
```bash
pip install pika
# 或
pip install -r scripts/requirements.txt
```

#### 快速启动
```bash
# Windows
scripts\start_consumer.bat

# Linux/Mac
./scripts/start_consumer.sh

# 或直接运行
python scripts/consume_rabbitmq.py
```

#### 常用命令
```bash
# 消费10条消息后退出
python scripts/consume_rabbitmq.py --count 10

# 自定义连接参数
python scripts/consume_rabbitmq.py --host 192.168.1.100 --username admin --password secret

# 测试连接（不消费消息）
python scripts/consume_rabbitmq.py --test

# 查看帮助
python scripts/consume_rabbitmq.py --help
```

#### 输出示例
```
【商户统计】14:30:25
商户号: M001
事件类型: INCREMENTAL
更新时间: 2024-03-12 14:30:24
总金额: ¥1500.00
成功金额: ¥1000.00
退款金额: ¥300.00
撤销金额: ¥200.00
净额: ¥500.00
原始数据: {"mchNo":"M001","totalAmount":150000,...}
```

### 6. 配置说明

#### RabbitMQ 连接参数
| 参数 | 默认值 | 说明 |
|------|--------|------|
| host | 192.168.0.99 | RabbitMQ 主机地址 |
| port | 5672 | RabbitMQ 端口 |
| username | admin | 用户名 |
| password | StrongPassword123 | 密码 |
| virtual_host | / | 虚拟主机 |
| queue | merchant-stats-queue | 队列名称 |

#### 生产环境建议
1. **使用配置中心**：避免硬编码敏感信息
2. **调整重试策略**：根据网络状况调整重试次数和间隔
3. **监控队列**：监控队列长度，避免消息积压
4. **安全加固**：使用 SSL/TLS 加密连接

### 7. 故障排查

#### 连接失败
```bash
# 检查 RabbitMQ 服务状态
rabbitmqctl status

# 检查队列是否存在
rabbitmqadmin list queues
```

#### 权限问题
```bash
# 查看用户权限
rabbitmqctl list_users
rabbitmqctl list_permissions
```

#### 消息格式问题
- 确保消费者使用正确的 JSON 解析
- 检查消息编码是否为 UTF-8
- 验证数据字段完整性

### 8. 扩展建议

1. **多队列支持**：可根据商户类型或金额范围分发到不同队列
2. **消息持久化**：确保重要消息不丢失
3. **死信队列**：处理无法正常消费的消息
4. **监控集成**：集成 Prometheus/Grafana 监控消息流量

---

通过 RabbitMQ 集成，项目实现了实时数据流的标准化输出，为下游系统（监控告警、数据分析、实时大屏等）提供了可靠的数据接入点。