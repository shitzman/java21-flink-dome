package com.stz.order.model;

import lombok.Data;

import java.time.Instant;

/**
 * 商户统计结果
 * @author shi.tongzhan
 * Create at 2026/3/5
 */
@Data
public class MerchantStats {
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
