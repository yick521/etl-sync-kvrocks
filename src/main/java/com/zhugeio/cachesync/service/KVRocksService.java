package com.zhugeio.cachesync.service;

import com.zhugeio.cachesync.config.CacheSyncConfig;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * KVRocks服务 - 基于Lettuce
 * 
 * 支持集群和单机两种模式
 * 使用Hash Tag解决集群模式下RENAME的CROSSSLOT问题
 */
@Slf4j
@Service
public class KVRocksService {

    @Value("${spring.redis.host:localhost}")
    private String host;

    @Value("${spring.redis.port:6379}")
    private int port;

    @Value("${spring.redis.password:}")
    private String password;

    @Value("${spring.redis.cluster.enabled:false}")
    private boolean isCluster;

    @Value("${spring.redis.timeout:60000}")
    private long timeoutMs;

    @Autowired
    private CacheSyncConfig config;

    private RedisClusterClient clusterClient;
    private RedisClient standaloneClient;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private StatefulRedisConnection<String, String> standaloneConnection;

    @PostConstruct
    public void init() {
        try {
            if (isCluster) {
                initClusterMode();
            } else {
                initStandaloneMode();
            }
            
            if (testConnection()) {
                log.info("✅ KVRocks连接初始化成功：{}:{} ({}模式)", 
                        host, port, isCluster ? "集群" : "单机");
            } else {
                throw new RuntimeException("KVRocks连接测试失败");
            }
        } catch (Exception e) {
            log.error("KVRocks连接初始化失败", e);
            throw new RuntimeException("KVRocks连接初始化失败", e);
        }
    }

    private void initClusterMode() {
        RedisURI.Builder uriBuilder = RedisURI.Builder
                .redis(host, port)
                .withTimeout(Duration.ofMillis(timeoutMs));
        
        if (password != null && !password.isEmpty()) {
            uriBuilder.withPassword(password.toCharArray());
        }

        clusterClient = RedisClusterClient.create(uriBuilder.build());
        clusterClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .maxRedirects(8)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(timeoutMs)))
                .build());
        
        clusterConnection = clusterClient.connect();
        log.info("✅ Lettuce集群连接初始化成功：{}:{}", host, port);
    }

    private void initStandaloneMode() {
        RedisURI.Builder uriBuilder = RedisURI.Builder
                .redis(host, port)
                .withTimeout(Duration.ofMillis(timeoutMs));
        
        if (password != null && !password.isEmpty()) {
            uriBuilder.withPassword(password.toCharArray());
        }

        standaloneClient = RedisClient.create(uriBuilder.build());
        standaloneClient.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .pingBeforeActivateConnection(true)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(timeoutMs)))
                .build());
        
        standaloneConnection = standaloneClient.connect();
        log.info("✅ Lettuce单机连接初始化成功：{}:{}", host, port);
    }

    public boolean testConnection() {
        try {
            String pong;
            if (isCluster) {
                pong = clusterConnection.sync().ping();
            } else {
                pong = standaloneConnection.sync().ping();
            }
            return "PONG".equalsIgnoreCase(pong);
        } catch (Exception e) {
            log.error("KVRocks连接测试失败: {}", e.getMessage());
            return false;
        }
    }

    // ==================== 原子性替换操作 ====================

    /**
     * 原子性替换Hash - 使用临时Key + RENAME策略（无缝切换）
     * 
     * 集群模式下使用Hash Tag {cacheName} 确保临时key和目标key在同一slot
     * 例如: {appKeyAppIdMap}:temp:123 和 appKeyAppIdMap 会哈希到同一slot
     * 
     * 注意: 目标key也需要加Hash Tag才能保证同slot
     */
    public void atomicReplaceHash(String cacheName, Map<String, String> data) {
        if (data == null || data.isEmpty()) {
            log.warn("Empty data for cache: {}, will delete key", cacheName);
            deleteKey(cacheName);
            return;
        }

        // 集群模式下使用Hash Tag确保同一slot
        String finalKey = isCluster ? "{" + cacheName + "}" : cacheName;
        String tempKey = "{" + cacheName + "}:temp:" + System.currentTimeMillis();

        try {
            // 1. 批量写入临时Key
            syncBatchHSet(tempKey, data, timeoutMs);
            
            // 2. 原子替换
            if (isCluster) {
                clusterConnection.sync().rename(tempKey, finalKey);
            } else {
                standaloneConnection.sync().rename(tempKey, finalKey);
            }
            
            log.debug("Atomic replace hash completed: {} ({} fields)", cacheName, data.size());
        } catch (Exception e) {
            // 清理临时Key
            try {
                if (isCluster) {
                    clusterConnection.sync().del(tempKey);
                } else {
                    standaloneConnection.sync().del(tempKey);
                }
            } catch (Exception ignored) {}
            
            log.error("Atomic replace hash failed: {}", cacheName, e);
            throw new RuntimeException("Atomic replace hash failed: " + cacheName, e);
        }
    }

    /**
     * 原子性替换Set（无缝切换）
     */
    public void atomicReplaceSet(String cacheName, Set<String> data) {
        if (data == null || data.isEmpty()) {
            log.warn("Empty data for cache: {}, will delete key", cacheName);
            deleteKey(cacheName);
            return;
        }

        // 集群模式下使用Hash Tag
        String finalKey = isCluster ? "{" + cacheName + "}" : cacheName;
        String tempKey = "{" + cacheName + "}:temp:" + System.currentTimeMillis();

        try {
            // 1. 批量写入临时Key
            syncBatchSAdd(tempKey, data, timeoutMs);
            
            // 2. 原子替换
            if (isCluster) {
                clusterConnection.sync().rename(tempKey, finalKey);
            } else {
                standaloneConnection.sync().rename(tempKey, finalKey);
            }
            
            log.debug("Atomic replace set completed: {} ({} members)", cacheName, data.size());
        } catch (Exception e) {
            try {
                if (isCluster) {
                    clusterConnection.sync().del(tempKey);
                } else {
                    standaloneConnection.sync().del(tempKey);
                }
            } catch (Exception ignored) {}
            
            log.error("Atomic replace set failed: {}", cacheName, e);
            throw new RuntimeException("Atomic replace set failed: " + cacheName, e);
        }
    }

    // ==================== 批量Pipeline操作 ====================

    public void syncBatchHSet(String hashKey, Map<String, String> data, long timeoutMs) {
        if (data == null || data.isEmpty()) {
            return;
        }

        int batchSize = config.getPipelineBatchSize();
        List<Map.Entry<String, String>> entries = new ArrayList<>(data.entrySet());

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async = clusterConnection.async();
                
                for (int i = 0; i < entries.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, entries.size());
                    List<Map.Entry<String, String>> batch = entries.subList(i, end);
                    
                    async.setAutoFlushCommands(false);
                    
                    List<RedisFuture<Boolean>> futures = batch.stream()
                            .map(entry -> async.hset(hashKey, entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());
                    
                    async.flushCommands();
                    async.setAutoFlushCommands(true);
                    
                    CompletableFuture.allOf(
                            futures.stream()
                                    .map(RedisFuture::toCompletableFuture)
                                    .toArray(CompletableFuture[]::new)
                    ).get(timeoutMs, TimeUnit.MILLISECONDS);
                }
            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                
                for (int i = 0; i < entries.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, entries.size());
                    List<Map.Entry<String, String>> batch = entries.subList(i, end);
                    
                    async.setAutoFlushCommands(false);
                    
                    List<RedisFuture<Boolean>> futures = batch.stream()
                            .map(entry -> async.hset(hashKey, entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());
                    
                    async.flushCommands();
                    async.setAutoFlushCommands(true);
                    
                    CompletableFuture.allOf(
                            futures.stream()
                                    .map(RedisFuture::toCompletableFuture)
                                    .toArray(CompletableFuture[]::new)
                    ).get(timeoutMs, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            log.error("批量Hash写入失败: {}, {}", hashKey, e.getMessage());
            throw new RuntimeException("批量Hash写入失败: " + hashKey, e);
        }
    }

    public void syncBatchSAdd(String setKey, Set<String> members, long timeoutMs) {
        if (members == null || members.isEmpty()) {
            return;
        }

        int batchSize = config.getPipelineBatchSize();
        List<String> memberList = new ArrayList<>(members);

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async = clusterConnection.async();
                
                for (int i = 0; i < memberList.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, memberList.size());
                    List<String> batch = memberList.subList(i, end);
                    
                    async.setAutoFlushCommands(false);
                    
                    List<RedisFuture<Long>> futures = batch.stream()
                            .map(member -> async.sadd(setKey, member))
                            .collect(Collectors.toList());
                    
                    async.flushCommands();
                    async.setAutoFlushCommands(true);
                    
                    CompletableFuture.allOf(
                            futures.stream()
                                    .map(RedisFuture::toCompletableFuture)
                                    .toArray(CompletableFuture[]::new)
                    ).get(timeoutMs, TimeUnit.MILLISECONDS);
                }
            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                
                for (int i = 0; i < memberList.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, memberList.size());
                    List<String> batch = memberList.subList(i, end);
                    
                    async.setAutoFlushCommands(false);
                    
                    List<RedisFuture<Long>> futures = batch.stream()
                            .map(member -> async.sadd(setKey, member))
                            .collect(Collectors.toList());
                    
                    async.flushCommands();
                    async.setAutoFlushCommands(true);
                    
                    CompletableFuture.allOf(
                            futures.stream()
                                    .map(RedisFuture::toCompletableFuture)
                                    .toArray(CompletableFuture[]::new)
                    ).get(timeoutMs, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            log.error("批量Set写入失败: {}, {}", setKey, e.getMessage());
            throw new RuntimeException("批量Set写入失败: " + setKey, e);
        }
    }

    // ==================== 简单KV操作 ====================

    public void setValue(String key, String value) {
        try {
            if (isCluster) {
                clusterConnection.sync().set(key, value);
            } else {
                standaloneConnection.sync().set(key, value);
            }
        } catch (Exception e) {
            log.error("KVRocks SET失败: {}", key, e);
        }
    }

    public String getValue(String key) {
        try {
            if (isCluster) {
                return clusterConnection.sync().get(key);
            } else {
                return standaloneConnection.sync().get(key);
            }
        } catch (Exception e) {
            log.error("KVRocks GET失败: {}", key, e);
            return null;
        }
    }

    public void deleteKey(String key) {
        try {
            // 集群模式下，缓存key使用Hash Tag
            String actualKey = isCluster ? "{" + key + "}" : key;
            if (isCluster) {
                clusterConnection.sync().del(actualKey);
            } else {
                standaloneConnection.sync().del(actualKey);
            }
        } catch (Exception e) {
            log.error("KVRocks DEL失败: {}", key, e);
        }
    }

    // ==================== 异步查询操作 ====================

    public CompletableFuture<String> asyncHGet(String key, String field) {
        try {
            String actualKey = isCluster ? "{" + key + "}" : key;
            if (isCluster) {
                return clusterConnection.async().hget(actualKey, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hget(actualKey, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<Boolean> asyncSIsMember(String key, String member) {
        try {
            String actualKey = isCluster ? "{" + key + "}" : key;
            if (isCluster) {
                return clusterConnection.async().sismember(actualKey, member)
                        .toCompletableFuture()
                        .exceptionally(ex -> false);
            } else {
                return standaloneConnection.async().sismember(actualKey, member)
                        .toCompletableFuture()
                        .exceptionally(ex -> false);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            if (clusterConnection != null) {
                clusterConnection.close();
            }
            if (clusterClient != null) {
                clusterClient.shutdown();
                log.info("Lettuce集群连接已关闭");
            }

            if (standaloneConnection != null) {
                standaloneConnection.close();
            }
            if (standaloneClient != null) {
                standaloneClient.shutdown();
                log.info("Lettuce单机连接已关闭");
            }
        } catch (Exception e) {
            log.error("关闭Lettuce连接失败: {}", e.getMessage());
        }
    }
}
