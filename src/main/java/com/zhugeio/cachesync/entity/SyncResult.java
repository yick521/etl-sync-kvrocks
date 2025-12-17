package com.zhugeio.cachesync.entity;

import lombok.Data;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 同步结果统计
 */
@Data
public class SyncResult {
    
    /**
     * 缓存类型名称
     */
    private String cacheName;
    
    /**
     * 同步的记录数
     */
    private AtomicLong syncCount = new AtomicLong(0);
    
    /**
     * 开始时间
     */
    private long startTime;
    
    /**
     * 结束时间
     */
    private long endTime;
    
    /**
     * 是否成功
     */
    private boolean success = true;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    public SyncResult(String cacheName) {
        this.cacheName = cacheName;
        this.startTime = System.currentTimeMillis();
    }
    
    public void incrementCount() {
        syncCount.incrementAndGet();
    }
    
    public void addCount(long count) {
        syncCount.addAndGet(count);
    }
    
    public void finish() {
        this.endTime = System.currentTimeMillis();
    }
    
    public void fail(String errorMessage) {
        this.success = false;
        this.errorMessage = errorMessage;
        this.endTime = System.currentTimeMillis();
    }
    
    public long getCostTime() {
        return endTime - startTime;
    }
    
    @Override
    public String toString() {
        return String.format("[%s] count=%d, cost=%dms, success=%s%s",
                cacheName,
                syncCount.get(),
                getCostTime(),
                success,
                errorMessage != null ? ", error=" + errorMessage : "");
    }
}
