package com.zhugeio.cachesync;

import com.zhugeio.cachesync.service.CacheSyncService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * ZhugeIO Cache Sync Application
 * 
 * 用于将MySQL中的缓存数据同步到KVRocks
 * 设计用于海豚调度定时调用
 * 
 * @author zhugeio
 */
@Slf4j
@SpringBootApplication
public class CacheSyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(CacheSyncApplication.class, args);
    }

    @Bean
    public CommandLineRunner run(CacheSyncService cacheSyncService) {
        return args -> {
            log.info("========================================");
            log.info("Starting ZhugeIO Cache Sync Service...");
            log.info("========================================");
            
            long startTime = System.currentTimeMillis();
            
            try {
                // 执行全量同步
                cacheSyncService.syncAll();
                
                long costTime = System.currentTimeMillis() - startTime;
                log.info("========================================");
                log.info("Cache sync completed successfully!");
                log.info("Total time: {} ms ({} seconds)", costTime, costTime / 1000);
                log.info("========================================");
                
                // 正常退出
                System.exit(0);
                
            } catch (Exception e) {
                log.error("Cache sync failed!", e);
                // 异常退出，返回非0状态码
                System.exit(1);
            }
        };
    }
}
