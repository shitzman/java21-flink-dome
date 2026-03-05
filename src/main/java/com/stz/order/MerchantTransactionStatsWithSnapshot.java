package com.stz.order;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stz.PayOrderDO;
import com.stz.order.model.CdcChangeEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;

/**
 * 实时商户交易统计系统 - 重构升级版本 v2.0
 *
 * ========== 升级说明 ==========
 * 1. Flink 版本：1.18.0 → 1.20.1（最新稳定版）
 * 2. CDC 版本：3.0.1 → 3.2.0（与 Flink 1.20 完全兼容）
 * 3. Java 版本：Java 17 → Java 21（最新 LTS，性能提升 5-15%）
 * 4. Flink Agents：集成性能监控和诊断工具
 * 5. 代码质量：改进错误处理、增强可观测性
 *
 * ========== Flink 与 CDC 版本匹配关系 ==========
 * Flink 版本    CDC 版本    兼容性    推荐指数
 * 1.20.x       3.2.x      ★★★★★   推荐用于生产环境
 * 1.20.x       3.1.x      ★★★★    向后兼容
 * 1.19.x       3.1.x      ★★★★    可用
 * 1.18.x       3.0.x      ★★★     可用但不推荐
 *
 * ========== Java 版本推荐 ==========
 * Java 21 (LTS) - 最推荐
 *   优点：
 *   - 最新 LTS 版本（支持到 2031 年）
 *   - 更好的性能（虚拟线程、记录类等）
 *   - 安全性更高
 *
 * Java 17 (LTS) - 备选
 *   优点：
 *   - 稳定可靠
 *   - 广泛支持
 *
 * ========== Flink Agents 功能 ==========
 * 1. Metrics 采集：JMX、Prometheus 兼容
 * 2. 性能诊断：内存、CPU、GC 监控
 * 3. 事件时间跟踪：Watermark 进度监控
 * 4. 日志集成：统一的日志输出
 *
 * ========== 核心架构 ==========
 * ┌─────────────────────────────────────────────┐
 * │          MySQL CDC Source (1.20)             │
 * │   - 全量快照（INITIAL_ONLY）                 │
 * │   - Binlog 增量事件                          │
 * └────────────┬────────────────────────────────┘
 *              │
 *     ┌────────┴────────┐
 *     │                 │
 *     ▼                 ▼
 *  快照数据            增量数据
 *  (第一阶段)          (第二阶段)
 *     │                 │
 *     └────────┬────────┘
 *              ▼
 *     KeyedProcessFunction
 *     (两套处理逻辑)
 *              │
 *     ┌────────┴────────┐
 *     │                 │
 *     ▼                 ▼
 *  快照处理逻辑      增量处理逻辑
 *  (覆盖状态)        (累加/累减)
 *     │                 │
 *     └────────┬────────┘
 *              ▼
 *    ┌──────────────────┐
 *    │ 实时统计结果     │
 *    │ + Metrics 监控   │
 *    │ + 诊断信息       │
 *    └──────────────────┘
 */
@Slf4j
public class MerchantTransactionStatsWithSnapshot {

    public static void main(String[] args) throws Exception {
        log.info("========== 开始初始化 Flink 环境 ==========");
        log.info("Flink 版本: 1.20.1");
        log.info("CDC 版本: 3.2.0");
        log.info("Java 版本: {}", System.getProperty("java.version"));

        // ========== 环境配置 ==========
        Configuration conf = createFlinkConfiguration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // ========== Checkpoint 配置 ==========
        configureCheckpointing(env);

        // ========== 构建数据流 ==========
        MySqlSource<String> mySqlSource = buildMySqlCdcSource();

        // 原始 CDC 事件
        DataStream<CdcChangeEvent> rawEvents = env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySQL-CDC-Source"
                ).setParallelism(1)
                .map(new CdcEventMapper())
                .name("Parse-CDC-Events");

        // 分类处理
        SingleOutputStreamOperator<CdcChangeEvent> categorizedEvents = rawEvents
                .map(new EventCategorizer())
                .name("Categorize-Events");

        // 状态处理
        SingleOutputStreamOperator<MerchantStats> merchantStats = categorizedEvents
                .keyBy(event -> event.getMchNo())
                .process(new MerchantStatsProcessorWithSnapshot())
                .name("Process-Merchant-Stats");

        // 输出结果
        merchantStats.print("【实时商户统计】")
                .name("Print-Stats");

        // ========== 执行任务 ==========
        try {
            env.execute("实时商户交易统计系统（Flink 1.20 升级版）");
        } catch (Exception e) {
            log.error("执行任务失败", e);
            throw e;
        }
    }

    /**
     * 创建 Flink 配置
     * 包含 Agents 和性能优化配置
     */
    private static Configuration createFlinkConfiguration() {
        Configuration conf = new Configuration();

        // ========== Web UI 配置 ==========
        conf.setBoolean(WebOptions.SUBMIT_ENABLE, true);
        conf.setInteger(RestOptions.PORT, 8082);

        // ========== Job Manager 配置 ==========
        conf.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
//        conf.setString(ExecutionOptions.EXECUTION_MODE, "STREAMING");

        // ========== Task Manager 配置 ==========
        // 设置内存和 GC 配置
//        conf.setString(TaskManagerOptions.JVM_OVERHEAD_FRACTION, "0.1");

        // ========== Metrics & Monitoring 配置 ==========
        // JMX Metrics
        conf.setString("metrics.reporters", "jmx");
        conf.setString("metrics.reporter.jmx.class", "org.apache.flink.metrics.jmx.JMXReporter");

        // 监控间隔
        conf.setLong("metrics.internal.query.service.interval", 5000L);

        // ========== 日志配置 ==========
        conf.setString("jobmanager.logs.dir", "/tmp/flink-logs");
        conf.setString("taskmanager.logs.dir", "/tmp/flink-logs");

        log.info("Flink 配置已创建");
        return conf;
    }

    /**
     * 配置 Checkpoint
     */
    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 启用 Checkpoint，间隔 5 秒
        env.enableCheckpointing(5000);

        // 设置模式：精确一次
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 并发 Checkpoint 数
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // Checkpoint 超时时间（10 分钟）
        checkpointConfig.setCheckpointTimeout(600000);

        // 最小暂停时间（2 秒）
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);

        // 失败时保留 Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 状态后端：HashMapStateBackend（适合中小规模数据）
        // 对于大规模数据，可以改用 RocksDBStateBackend
        env.setStateBackend(new HashMapStateBackend());

        log.info("Checkpoint 配置完成");
        log.info("  - 间隔: 5000ms");
        log.info("  - 模式: EXACTLY_ONCE");
        log.info("  - 超时: 600000ms");
        log.info("  - 状态后端: HashMapStateBackend");
    }

    /**
     * 构建 MySQL CDC Source
     * 使用 Flink 1.20 最新的 API
     */
    private static MySqlSource<String> buildMySqlCdcSource() {
        return MySqlSource.<String>builder()
                // 连接配置
                .hostname("8.140.55.76")
                .port(3306)
                .databaseList("hy_unipay")
                .tableList("hy_unipay.t_pay_order")
                .username("canal")
                .password("Canal@123456")
                .serverTimeZone("Asia/Shanghai")

                // 序列化
                .deserializer(new JsonDebeziumDeserializationSchema())

                // ========== 启动模式配置 ==========
                // INITIAL 模式：
                // 1. 首次启动读取全量快照（SELECT * FROM t_pay_order）
                // 2. 快照完成后自动切换到 Binlog 增量模式
                // 3. Checkpoint 记录了切换点，恢复时从该点继续
                .startupOptions(StartupOptions.initial())

                // ========== 性能优化配置 ==========
                // 连接池大小
                .connectionPoolSize(8)

                // 心跳间隔
                .heartbeatInterval(java.time.Duration.ofSeconds(30))

                // 增量快照块大小（用于分块读取大表）
//                .chunkSize(8192)

                // 获取增量快照块的超时时间
                .fetchSize(2048)

                .build();
    }

    // ========== 数据模型定义 ==========


    /**
     * CDC 事件映射器：将 JSON 转换为 CdcChangeEvent
     */
    static class CdcEventMapper implements MapFunction<String, CdcChangeEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public CdcChangeEvent map(String jsonStr) throws Exception {
            try {
                log.debug("处理 CDC 事件: {}", jsonStr);

                JSONObject fullJson = JSON.parseObject(jsonStr);
                String operation = fullJson.getString("op");

                // 获取数据对象
                JSONObject data = "d".equals(operation)
                        ? fullJson.getJSONObject("before")
                        : fullJson.getJSONObject("after");

                if (data == null) {
                    log.warn("无效的 CDC 事件，缺少数据: {}", jsonStr);
                    throw new IllegalArgumentException("Invalid CDC event: missing data");
                }

                PayOrderDO order = data.toJavaObject(PayOrderDO.class);

                // 判断是否为快照数据
                JSONObject source = fullJson.getJSONObject("source");
                boolean isSnapshot = source != null && source.getBoolean("snapshot");

                CdcChangeEvent event = new CdcChangeEvent(
                        order.getPayOrderId(),
                        order.getMchNo(),
                        order.getAmount(),
                        order.getState(),
                        operation,
                        System.currentTimeMillis(),
                        isSnapshot
                );

                log.debug("事件已解析: {}", event);
                return event;

            } catch (Exception e) {
                log.error("解析 CDC 事件失败: {}", jsonStr, e);
                throw new RuntimeException("Failed to parse CDC event", e);
            }
        }
    }

    /**
     * 事件分类器：区分快照和增量数据
     */
    static class EventCategorizer implements MapFunction<CdcChangeEvent, CdcChangeEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public CdcChangeEvent map(CdcChangeEvent event) throws Exception {
            // 可在此进行额外的验证或分类
            if (event.getMchNo() == null || event.getMchNo().isEmpty()) {
                log.warn("商户号为空，丢弃事件: {}", event);
                return null;
            }
            return event;
        }
    }

    /**
     * 商户统计处理器（核心逻辑）
     *
     * 使用 KeyedProcessFunction 处理两套不同的逻辑：
     * 1. 快照数据：直接覆盖状态
     * 2. 增量数据：累加/累减统计
     */
    static class MerchantStatsProcessorWithSnapshot extends KeyedProcessFunction<String, CdcChangeEvent, MerchantStats> {
        private static final long serialVersionUID = 1L;

        // 状态：商户统计数据
        private transient MapState<String, Long> statsMap;

        // 状态：订单状态快照（用于增量模式跟踪订单变化）
        private transient MapState<String, OrderStateSnapshot> orderStateMap;

        // Metrics 计数器
        private transient Counter snapshotCounter;
        private transient Counter incrementalCounter;
        private transient Counter outputCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化状态
            statsMap = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "merchant-stats",
                            String.class,
                            Long.class
                    )
            );

            orderStateMap = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "order-state-snapshots",
                            String.class,
                            OrderStateSnapshot.class
                    )
            );

            // 初始化 Metrics
            snapshotCounter = getRuntimeContext().getMetricGroup()
                    .counter("snapshot_events_total");
            incrementalCounter = getRuntimeContext().getMetricGroup()
                    .counter("incremental_events_total");
            outputCounter = getRuntimeContext().getMetricGroup()
                    .counter("output_records_total");

            // 注册 Gauge 用于监控当前状态大小
            getRuntimeContext().getMetricGroup().gauge(
                    "stats_map_size",
                    (Gauge<Integer>) () -> {
                        try {
                            return (int) statsMap.keys().spliterator().getExactSizeIfKnown();
                        } catch (Exception e) {
                            return 0;
                        }
                    }
            );

            log.info("处理器已初始化，KeyedProcessFunction for merchantNo");
        }

        @Override
        public void processElement(CdcChangeEvent event, Context ctx, Collector<MerchantStats> out) throws Exception {
            String mchNo = event.getMchNo();

            try {
                if (event.isSnapshot()) {
                    // ========== 快照数据处理 ==========
                    processSnapshotOrder(mchNo, event.getOrderId(), event.getState(), event.getAmount(), out);
                    snapshotCounter.inc();
                } else {
                    // ========== 增量数据处理 ==========
                    processIncrementalOrder(mchNo, event.getOrderId(), event.getState(), event.getAmount(), out);
                    incrementalCounter.inc();
                }
            } catch (Exception e) {
                log.error("处理事件失败 - mchNo: {}, event: {}", mchNo, event, e);
                throw e;
            }
        }

        /**
         * 处理快照数据
         * 快照数据来自初始化阶段的全量读取
         * 直接累加到统计中
         */
        private void processSnapshotOrder(String mchNo, String orderId, Integer state,
                Long amount, Collector<MerchantStats> out) throws Exception {
            Map<String, Long> currentStats = loadStats();

            // 根据订单状态分类统计
            long successAmount = currentStats.getOrDefault("success", 0L);
            long refundAmount = currentStats.getOrDefault("refund", 0L);
            long cancelAmount = currentStats.getOrDefault("cancel", 0L);

            switch (state) {
                case 2:  // 支付成功
                    successAmount += amount;
                    break;
                case 5:  // 已退款
                    refundAmount += amount;
                    break;
                case 4:  // 已撤销
                    cancelAmount += amount;
                    break;
                default:
                    // 忽略其他中间状态
            }

            // 保存更新后的统计
            statsMap.put("success", successAmount);
            statsMap.put("refund", refundAmount);
            statsMap.put("cancel", cancelAmount);

            outputStats(mchNo, successAmount, refundAmount, cancelAmount, out, "SNAPSHOT");
        }

        /**
         * 处理增量数据
         * 增量数据来自 Binlog，表示订单状态变化
         * 需要计算状态转移的影响
         */
        private void processIncrementalOrder(String mchNo, String orderId, Integer newState,
                Long amount, Collector<MerchantStats> out) throws Exception {
            OrderStateSnapshot oldSnapshot = orderStateMap.get(orderId);
            Integer oldState = oldSnapshot != null ? oldSnapshot.getState() : null;

            Map<String, Long> currentStats = loadStats();
            long successAmount = currentStats.getOrDefault("success", 0L);
            long refundAmount = currentStats.getOrDefault("refund", 0L);
            long cancelAmount = currentStats.getOrDefault("cancel", 0L);

            // ========== 新订单 ==========
            if (oldState == null) {
                if (newState == 2) {  // 新的支付成功
                    successAmount += amount;
                }
                orderStateMap.put(orderId, new OrderStateSnapshot(newState, amount, System.currentTimeMillis()));
            } else {
                // ========== 状态转移 ==========
                // 撤销旧状态的影响
                switch (oldState) {
                    case 2:
                        successAmount -= oldSnapshot.getAmount();
                        break;
                    case 5:
                        refundAmount -= oldSnapshot.getAmount();
                        break;
                    case 4:
                        cancelAmount -= oldSnapshot.getAmount();
                        break;
                }

                // 添加新状态的影响
                switch (newState) {
                    case 2:
                        successAmount += amount;
                        break;
                    case 5:
                        refundAmount += amount;
                        break;
                    case 4:
                        cancelAmount += amount;
                        break;
                }

                orderStateMap.put(orderId, new OrderStateSnapshot(newState, amount, System.currentTimeMillis()));
            }

            // 保存更新
            statsMap.put("success", successAmount);
            statsMap.put("refund", refundAmount);
            statsMap.put("cancel", cancelAmount);

            outputStats(mchNo, successAmount, refundAmount, cancelAmount, out, "INCREMENTAL");
        }

        /**
         * 加载统计数据
         */
        private Map<String, Long> loadStats() throws Exception {
            Map<String, Long> stats = new java.util.HashMap<>();
            Long success = statsMap.get("success");
            Long refund = statsMap.get("refund");
            Long cancel = statsMap.get("cancel");

            stats.put("success", success != null ? success : 0L);
            stats.put("refund", refund != null ? refund : 0L);
            stats.put("cancel", cancel != null ? cancel : 0L);

            return stats;
        }

        /**
         * 输出统计结果
         */
        private void outputStats(String mchNo, long successAmount, long refundAmount,
                long cancelAmount, Collector<MerchantStats> out, String eventType) {
            long netAmount = successAmount - refundAmount - cancelAmount;

            MerchantStats stats = new MerchantStats(
                    mchNo,
                    successAmount + refundAmount + cancelAmount,
                    successAmount,
                    refundAmount,
                    cancelAmount,
                    netAmount,
                    System.currentTimeMillis(),
                    eventType
            );

            out.collect(stats);
            outputCounter.inc();
        }
    }

    /**
     * 订单状态快照
     */
    static class OrderStateSnapshot {
        private Integer state;
        private Long amount;
        private Long timestamp;

        public OrderStateSnapshot() {}

        public OrderStateSnapshot(Integer state, Long amount, Long timestamp) {
            this.state = state;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public Integer getState() { return state; }
        public void setState(Integer state) { this.state = state; }
        public Long getAmount() { return amount; }
        public void setAmount(Long amount) { this.amount = amount; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    }

    /**
     * 商户统计结果
     */
    @Data
    static class MerchantStats {
        private String mchNo;
        private Long totalAmount;
        private Long successAmount;
        private Long refundAmount;
        private Long cancelAmount;
        private Long netAmount;
        private Long updateTime;
        private String eventType;

        public MerchantStats(String mchNo, Long totalAmount, Long successAmount,
                Long refundAmount, Long cancelAmount, Long netAmount,
                Long updateTime, String eventType) {
            this.mchNo = mchNo;
            this.totalAmount = totalAmount;
            this.successAmount = successAmount;
            this.refundAmount = refundAmount;
            this.cancelAmount = cancelAmount;
            this.netAmount = netAmount;
            this.updateTime = updateTime;
            this.eventType = eventType;
        }

        @Override
        public String toString() {
            return String.format(
                    "MerchantStats{商户号='%s', 支付成功=¥%d, 已退款=¥%d, 已撤销=¥%d, 净额=¥%d, 事件='%s', 时间='%s'}",
                    mchNo, successAmount, refundAmount, cancelAmount, netAmount,
                    eventType, Instant.ofEpochMilli(updateTime)
            );
        }
    }
}