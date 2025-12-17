package com.zhugeio.cachesync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 缓存同步配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "cache.sync")
public class CacheSyncConfig {
    
    /**
     * 是否开启投放功能同步
     */
    private boolean openToufang = true;
    
    /**
     * 批量写入大小
     */
    private int batchSize = 1000;
    
    /**
     * 同步超时时间(秒)
     */
    private int timeoutSeconds = 300;
    
    /**
     * 是否使用pipeline
     */
    private boolean usePipeline = true;
    
    /**
     * pipeline批次大小
     */
    private int pipelineBatchSize = 500;
}
