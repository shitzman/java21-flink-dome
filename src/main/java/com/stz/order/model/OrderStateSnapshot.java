package com.stz.order.model;

/**
 * 订单状态快照
 * @author shi.tongzhan
 * Create at 2026/3/5
 */
public class OrderStateSnapshot {
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