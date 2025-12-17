package com.zhugeio.cachesync.entity;

import com.alibaba.fastjson.JSON;
import lombok.Data;

/**
 * 广告链接事件实体
 */
@Data
public class AdsLinkEvent {
    
    private Integer linkId = 0;
    private Integer eventId = 0;
    private String channelEvent = "";
    private String matchJson = "";
    private Integer frequency = 0;
    private String eventIds = "";
    private Long windowTime = 0L;
    
    public String toJsonString() {
        return JSON.toJSONString(this);
    }
}
