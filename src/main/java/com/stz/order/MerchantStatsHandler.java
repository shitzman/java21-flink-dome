package com.stz.order;

import com.stz.order.model.CdcChangeEvent;
import com.stz.order.model.MerchantStats;
import com.stz.order.model.OrderStateSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author shi.tongzhan
 * Create at 2026/3/5
 */
@Slf4j
public class MerchantStatsHandler extends KeyedProcessFunction<String, CdcChangeEvent, MerchantStats> {
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
