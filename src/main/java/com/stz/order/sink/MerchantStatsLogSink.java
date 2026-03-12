package com.stz.order.sink;

import com.stz.order.model.MerchantStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 自定义商户统计日志 Sink
 * 用于格式化输出实时商户统计数据到日志
 */
@Slf4j
public class MerchantStatsLogSink extends RichSinkFunction<MerchantStats> {

    private static final long serialVersionUID = 1L;

    /**
     * 日志前缀标识
     */
    private final String logPrefix;

    /**
     * 是否输出详细日志
     */
    private final boolean verbose;

    /**
     * 构造函数
     *
     * @param logPrefix 日志前缀标识
     * @param verbose   是否输出详细日志
     */
    public MerchantStatsLogSink(String logPrefix, boolean verbose) {
        this.logPrefix = logPrefix;
        this.verbose = verbose;
    }

    /**
     * 简化构造函数，使用默认配置
     *
     * @param logPrefix 日志前缀标识
     */
    public MerchantStatsLogSink(String logPrefix) {
        this(logPrefix, false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("{} - Sink 初始化完成", logPrefix);
        if (verbose) {
            log.info("{} - 详细模式已启用", logPrefix);
        }
    }

    @Override
    public void invoke(MerchantStats stats, Context context) throws Exception {
        if (stats == null) {
            log.warn("{} - 收到空数据，跳过", logPrefix);
            return;
        }

        // ========== 基础日志输出 ==========
        log.info("{} | 商户：{} | 总单量：{} | 净额：{} | 更新时间：{}",
                logPrefix,
                stats.getMchNo(),
                stats.getTotalAmount(),
                formatAmount(stats.getNetAmount()),
                formatTimestamp(stats.getUpdateTime())
        );

        // ========== 详细日志输出 ==========
        if (verbose) {
            log.info("{} [详细] 商户：{} | 成功：{} | 退款：{} | 撤销：{} | 净额：{} | 类型：{} | 时间：{}",
                    logPrefix,
                    stats.getMchNo(),
                    formatAmount(stats.getSuccessAmount()),
                    formatAmount(stats.getRefundAmount()),
                    formatAmount(stats.getCancelAmount()),
                    formatAmount(stats.getNetAmount()),
                    stats.getEventType(),
                    formatTimestamp(stats.getUpdateTime())
            );
        }

        // ========== 告警日志（可选） ==========
        // 如果净额为负数，输出告警
        if (stats.getNetAmount() < 0) {
            log.warn("{} ⚠️ 告警 | 商户：{} | 净额为负：{} | 可能存在异常",
                    logPrefix,
                    stats.getMchNo(),
                    formatAmount(stats.getNetAmount())
            );
        }
    }

//    @Override
//    public void close(Exception filterCause) throws Exception {
//        log.info("{} - Sink 关闭，原因：{}",
//                logPrefix,
//                filterCause != null ? filterCause.getMessage() : "正常关闭");
//        super.close(filterCause);
//    }

    /**
     * 格式化金额（分转元）
     *
     * @param amount 金额（分）
     * @return 格式化后的字符串
     */
    private String formatAmount(Long amount) {
        if (amount == null) {
            return "0.00";
        }
        return String.format("¥%.2f", amount / 100.0);
    }

    /**
     * 格式化时间戳
     *
     * @param timestamp 时间戳
     * @return 格式化后的时间字符串
     */
    private String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return "-";
        }
        java.time.Instant instant = java.time.Instant.ofEpochMilli(timestamp);
        java.time.ZonedDateTime dateTime = instant.atZone(java.time.ZoneId.of("Asia/Shanghai"));
        return java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(dateTime);
    }
}
