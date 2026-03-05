package com.stz.order.model;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 支付订单 DO
 *
 * @author 芋道源码
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PayOrderDO extends BaseDO {
    private String op; // 记录操作类型：c(创建), u(更新), d(删除)
    /**
     * 支付订单号
     */
    @JSONField(name = "pay_order_id")
    private String payOrderId;
    /**
     * 商户号
     */
    @JSONField(name = "mch_no")
    private String mchNo;
    /**
     * 服务商号
     */
    @JSONField(name = "isv_no")
    private String isvNo;
    /**
     * 门店id
     */
    @JSONField(name = "store_no")
    private String storeNo;
    /**
     * 码牌id
     */
    @JSONField(name = "qr_code_no")
    private String qrCodeNo;
    /**
     * 市场编号
     */
    @JSONField(name = "market_no")
    private String marketNo;
    /**
     * 应用ID
     */
    @JSONField(name = "app_id")
    private String appId;
    /**
     * 商户名称
     */
    @JSONField(name = "mch_name")
    private String mchName;
    /**
     * 类型: 1-普通商户, 2-特约商户(服务商模式)
     */
    @JSONField(name = "mch_type")
    private Integer mchType;
    /**
     * 商户订单号
     */
    @JSONField(name = "mch_order_no")
    private String mchOrderNo;
    /**
     * 支付接口代码
     */
    @JSONField(name = "if_code")
    private String ifCode;
    /**
     * 支付方式代码
     */
    @JSONField(name = "way_code")
    private String wayCode;
    /**
     * 支付金额,单位分
     */
    @JSONField(name = "amount")
    private Long amount;
    /**
     * 商户手续费费率快照
     */
    private BigDecimal mchFeeRate;
    /**
     * 商户手续费,单位分
     */
    private Long mchFeeAmount;
    /**
     * 三位货币代码,人民币:cny
     */
    private String currency;
    /**
     * 支付状态: 0-订单生成, 1-支付中, 2-支付成功, 3-支付失败, 4-已撤销, 5-已退款, 6-订单关闭
     */
    private Integer state;
    /**
     * 入账状态(0未入账，1已入账)
     */
    private Integer entryState;
    /**
     * 向下游回调状态, 0-未发送,  1-已发送
     */
    private Integer notifyState;
    /**
     * 客户端IP
     */
    private String clientIp;
    /**
     * 商品标题
     */
    private String subject;
    /**
     * 商品描述信息
     */
    private String body;
    /**
     * 特定渠道发起额外参数
     */
    private String channelExtra;
    /**
     * 渠道用户标识,如微信openId,支付宝账号
     */
    private String channelUser;
    /**
     * 渠道订单号
     */
    private String channelOrderNo;
    /**
     * 退款状态: 0-未发生实际退款, 1-部分退款, 2-全额退款
     */
    private Integer refundState;
    /**
     * 退款次数
     */
    private Integer refundTimes;
    /**
     * 退款总金额,单位分
     */
    private Long refundAmount;
    /**
     * 订单分账模式：0-该笔订单不允许分账, 1-支付成功按配置自动完成分账, 2-商户手动分账(解冻商户金额)
     */
    private Integer divisionMode;
    /**
     * 订单分账状态：0-未发生分账, 1-等待分账任务处理, 2-分账处理中, 3-分账任务已结束(不体现状态)
     */
    private Integer divisionState;
    /**
     * 最新分账时间
     */
    private LocalDateTime divisionLastTime;
    /**
     * 渠道支付错误码
     */
    private String errCode;
    /**
     * 渠道支付错误描述
     */
    private String errMsg;
    /**
     * 商户扩展参数
     */
    private String extParam;
    /**
     * 异步通知地址
     */
    private String notifyUrl;
    /**
     * 页面跳转地址
     */
    private String returnUrl;
    /**
     * 订单失效时间
     */
    private LocalDateTime expiredTime;
    /**
     * 订单支付成功时间
     */
    private LocalDateTime successTime;

    /**
     * 微信、支付宝或银联交易流水号
     */
    private String transSerialNumber;

    /**
     * 租户id
     */
    private Long tenantId;

    private String marketName;

    private String storeName;

    private String isvName;
}
