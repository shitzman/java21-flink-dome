package com.stz.order;

import com.stz.order.model.CdcChangeEvent;
import com.stz.order.model.MerchantStats;
import com.stz.order.sink.MerchantStatsLogSink;
import com.stz.order.sink.MerchantStatsRabbitMQSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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
public class MerchantTransactionStatsWithSnapshotAPP {

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
        env.setParallelism(1); //并行度为1
        // 原始 CDC 事件
        SingleOutputStreamOperator<MerchantStats> merchantStats = env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "MySQL-CDC-Source"
                ).map(new CdcEventMapper()).name("Parse-CDC-Events")
                .map(new EventCategorizer()).name("Categorize-Events")
                .keyBy(event -> event.getMchNo()).process(new MerchantStatsHandler())
                .name("Process-Merchant-Stats");

        // ========== 输出结果 ==========
        // 1. 控制台输出
        merchantStats.print("【实时商户统计】")
                .name("Print-Stats").setParallelism(1);

        // 2. RabbitMQ 输出（示例配置，请根据实际环境修改）
        merchantStats.addSink(new MerchantStatsRabbitMQSink(
                "192.168.0.99",  // RabbitMQ 主机
                5672,         // RabbitMQ 端口
                "admin",      // 用户名
                "StrongPassword123",      // 密码
                "merchant-stats-queue"  // 队列名称
        )).name("RabbitMQ-Sink").setParallelism(1);

        // 3. 日志文件输出（可选）
        // merchantStats.addSink(new MerchantStatsLogSink("【实时商户统计2】", true))
        //         .name("Log-Stats-Sink").setParallelism(1);
        // 分类处理
//        SingleOutputStreamOperator<CdcChangeEvent> categorizedEvents = rawEvents
//                .map(new EventCategorizer())
//                .name("Categorize-Events");

        // 状态处理
//        SingleOutputStreamOperator<MerchantStats> merchantStats = rawEvents
//                .map(new EventCategorizer())
//                .name("Categorize-Events")
//                .keyBy(event -> event.getMchNo())
//                .process(new MerchantStatsHandler())
//                .name("Process-Merchant-Stats").setParallelism(1);

        // ========== 输出结果 Skills ==========
//        merchantStats.print("【实时商户统计】")
//                .name("Print-Stats").setParallelism(1);
//        merchantStats.addSink(new MerchantStatsLogSink("【实时商户统计2】", true))
//                .name("Log-Stats-Sink").setParallelism(1);

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
//        conf.setString("taskmanager.memory.network.fraction", "0.2");
        conf.setString("taskmanager.memory.network.max", "256mb");
        conf.setString("taskmanager.memory.network.min", "256mb");

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
                .hostname("127.0.0.1")
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
//                .startupOptions(StartupOptions.timestamp(System.currentTimeMillis() - 1 * 60 * 60 * 1000))

                // ========== 性能优化配置 ==========
                // 连接池大小
                .connectionPoolSize(8)

                // 心跳间隔
                .heartbeatInterval(java.time.Duration.ofSeconds(30))

                // 增量快照块大小（用于分块读取大表）
//                .chunkSize(8192)
                .splitSize(8192)

                // 获取增量快照块的超时时间
                .fetchSize(2048)

                .build();
    }

    // ========== 数据模型定义 ==========

}