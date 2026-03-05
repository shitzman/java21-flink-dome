package com.stz.order;

import com.stz.order.model.CdcChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 事件分类器：区分快照和增量数据
 * @author shi.tongzhan
 * Create at 2026/3/5
 */
@Slf4j
public class EventCategorizer implements MapFunction<CdcChangeEvent, CdcChangeEvent> {
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