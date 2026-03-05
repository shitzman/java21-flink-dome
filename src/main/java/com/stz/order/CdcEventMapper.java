package com.stz.order;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stz.order.model.PayOrderDO;
import com.stz.order.model.CdcChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * CDC 事件映射器：将 JSON 转换为 CdcChangeEvent
 * @author shi.tongzhan
 * Create at 2026/3/5
 */
@Slf4j
public class CdcEventMapper implements MapFunction<String, CdcChangeEvent> {
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
