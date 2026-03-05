package com.stz.order.model;

/**
 * CDC 变化事件（内部模型）
 * @author shi.tongzhan
 * Create at 2026/3/5
 */

public class CdcChangeEvent {
    private String orderId;
    private String mchNo;
    private Long amount;
    private Integer state;
    private String operation;  // c/u/d
    private Long timestamp;
    private boolean isSnapshot;

    public CdcChangeEvent(String orderId, String mchNo, Long amount, Integer state,
            String operation, Long timestamp, boolean isSnapshot) {
        this.orderId = orderId;
        this.mchNo = mchNo;
        this.amount = amount;
        this.state = state;
        this.operation = operation;
        this.timestamp = timestamp;
        this.isSnapshot = isSnapshot;
    }

    // Getters
    public String getOrderId() { return orderId; }
    public String getMchNo() { return mchNo; }
    public Long getAmount() { return amount; }
    public Integer getState() { return state; }
    public String getOperation() { return operation; }
    public Long getTimestamp() { return timestamp; }
    public boolean isSnapshot() { return isSnapshot; }

    @Override
    public String toString() {
        return String.format(
                "CdcChangeEvent{orderId='%s', mchNo='%s', amount=%d, state=%d, op='%s', isSnapshot=%s}",
                orderId, mchNo, amount, state, operation, isSnapshot
        );
    }
}