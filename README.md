## 技术组件
1. Flink 版本：1.20.1
2. CDC 版本：3.2.0
3. Java 版本：Java 21

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