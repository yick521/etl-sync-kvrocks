package com.zhugeio.cachesync.service;

import com.alibaba.fastjson.JSON;
import com.zhugeio.cachesync.config.CacheSyncConfig;
import com.zhugeio.cachesync.constants.CacheKeyConstants;
import com.zhugeio.cachesync.dao.FrontDao;
import com.zhugeio.cachesync.entity.AdsLinkEvent;
import com.zhugeio.cachesync.entity.SyncResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * 缓存同步主服务
 */
@Slf4j
@Service
public class CacheSyncService {

    @Autowired
    private FrontDao frontDao;
    
    @Autowired
    private KVRocksService kvRocksService;
    
    @Autowired
    private CacheSyncConfig config;
    
    private final List<SyncResult> syncResults = Collections.synchronizedList(new ArrayList<>());
    private ExecutorService executorService;

    public void syncAll() {
        log.info("Starting full cache sync...");
        long startTime = System.currentTimeMillis();
        
        // 清除批量查询缓存，确保获取最新数据
        frontDao.clearBatchCache();
        
        int threadCount = Runtime.getRuntime().availableProcessors();
        executorService = Executors.newFixedThreadPool(threadCount);
        
        try {
            recordSyncStart();
            
            List<Callable<SyncResult>> tasks = new ArrayList<>();
            
            // 核心缓存
            tasks.add(this::syncAppKeyAppIdMap);
            tasks.add(this::syncAppIdSdkHasDataMap);
            tasks.add(this::syncAppIdPropIdMap);
            tasks.add(this::syncAppIdPropIdOriginalMap);
            tasks.add(this::syncAppIdEventIdMap);
            tasks.add(this::syncAppIdEventAttrIdMap);
            tasks.add(this::syncAppIdDevicePropIdMap);
            
            // Set集合
            tasks.add(this::syncBlackUserPropSet);
            tasks.add(this::syncBlackEventIdSet);
            tasks.add(this::syncBlackEventAttrIdSet);
            tasks.add(this::syncAppIdCreateEventForbidSet);
            tasks.add(this::syncAppIdUploadDataSet);
            tasks.add(this::syncAppIdNoneAutoCreateSet);
            tasks.add(this::syncEventIdCreateAttrForbiddenSet);
            tasks.add(this::syncEventIdPlatform);
            tasks.add(this::syncEventAttrPlatform);
            tasks.add(this::syncDevicePropPlatform);
            
            // 虚拟事件/属性
            tasks.add(this::syncVirtualEventMap);
            tasks.add(this::syncVirtualEventAttrMap);
            tasks.add(this::syncEventAttrAliasMap);
            tasks.add(this::syncVirtualEventAppidsSet);
            tasks.add(this::syncVirtualPropAppIdsSet);
            tasks.add(this::syncEventVirtualAttrIdsSet);
            tasks.add(this::syncVirtualEventPropMap);
            tasks.add(this::syncVirtualUserPropMap);
            
            // 投放相关
            if (config.isOpenToufang()) {
                tasks.add(this::syncOpenAdvertisingFunctionAppMap);
                tasks.add(this::syncLidAndChannelEventMap);
                tasks.add(this::syncAppIdSMap);
                tasks.add(this::syncAdFrequencySet);
                tasks.add(this::syncAdsLinkEventMap);
            }
            
            // DW模块
            tasks.add(this::syncEventAttrColumnMap);
            tasks.add(this::syncBaseCurrentMap);
            tasks.add(this::syncOpenCdpAppidMap);
            tasks.add(this::syncYearWeek);
            tasks.add(this::syncCidByAidMap);
            tasks.add(this::syncBusinessMap);
            
            List<Future<SyncResult>> futures = executorService.invokeAll(tasks, 
                    config.getTimeoutSeconds(), TimeUnit.SECONDS);
            
            for (Future<SyncResult> future : futures) {
                try {
                    SyncResult result = future.get();
                    syncResults.add(result);
                } catch (Exception e) {
                    log.error("Sync task failed", e);
                }
            }
            
            recordSyncComplete();
            printSyncSummary(startTime);
            
        } catch (InterruptedException e) {
            log.error("Sync interrupted", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Sync interrupted", e);
        } finally {
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }

    // ==================== 同步方法 ====================
    
    private SyncResult syncAppKeyAppIdMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_KEY_APP_ID_MAP);
        try {
            Map<String, Integer> data = frontDao.getAppKeyIdMaps();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_KEY_APP_ID_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_KEY_APP_ID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_KEY_APP_ID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdSdkHasDataMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP);
        try {
            Map<String, Integer> data = frontDao.getSdkPlatformHasDataMap();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdPropIdMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_PROP_ID_MAP);
        try {
            Map<String, Integer> data = frontDao.getUserPropIds();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_PROP_ID_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_PROP_ID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_PROP_ID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdPropIdOriginalMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP);
        try {
            Map<String, String> data = frontDao.getOriginalUserPropIds();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdEventIdMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_EVENT_ID_MAP);
        try {
            Map<String, Integer> data = frontDao.getEventIds();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_EVENT_ID_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_EVENT_ID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_EVENT_ID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdEventAttrIdMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP);
        try {
            Map<String, Integer> data = frontDao.getEventAttrIds();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdDevicePropIdMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP);
        try {
            Map<String, Integer> data = frontDao.getDevicePropIds();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncBlackUserPropSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.BLACK_USER_PROP_SET);
        try {
            Set<Integer> data = frontDao.getBlackUserPropIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.BLACK_USER_PROP_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.BLACK_USER_PROP_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.BLACK_USER_PROP_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncBlackEventIdSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.BLACK_EVENT_ID_SET);
        try {
            Set<Integer> data = frontDao.getBlackEventIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.BLACK_EVENT_ID_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.BLACK_EVENT_ID_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.BLACK_EVENT_ID_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncBlackEventAttrIdSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET);
        try {
            Set<Integer> data = frontDao.getBlackEventAttrIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdCreateEventForbidSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET);
        try {
            Set<Integer> data = frontDao.getForbiddenCreateEventAppIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdUploadDataSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_UPLOAD_DATA_SET);
        try {
            Set<Integer> data = frontDao.getUploadDatas();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.APP_ID_UPLOAD_DATA_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_UPLOAD_DATA_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_UPLOAD_DATA_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdNoneAutoCreateSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET);
        try {
            Set<Integer> data = frontDao.getNoneAutoCreateAppIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncEventIdCreateAttrForbiddenSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET);
        try {
            Set<Integer> data = frontDao.getForbiddenCreateEventAttrEventIds();
            Set<String> stringSet = toStringSet(data);
            kvRocksService.atomicReplaceSet(CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET, stringSet);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncEventIdPlatform() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_ID_PLATFORM);
        try {
            Set<String> data = frontDao.getEventPlatforms();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.EVENT_ID_PLATFORM, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_ID_PLATFORM, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_ID_PLATFORM, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncEventAttrPlatform() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_ATTR_PLATFORM);
        try {
            Set<String> data = frontDao.getEventAttrPlatforms();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.EVENT_ATTR_PLATFORM, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_ATTR_PLATFORM, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_ATTR_PLATFORM, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncDevicePropPlatform() {
        SyncResult result = new SyncResult(CacheKeyConstants.DEVICE_PROP_PLATFORM);
        try {
            Set<String> data = frontDao.getDevicePropPlatforms();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.DEVICE_PROP_PLATFORM, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.DEVICE_PROP_PLATFORM, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.DEVICE_PROP_PLATFORM, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualEventMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_EVENT_MAP);
        try {
            Map<String, List<String>> data = frontDao.getVirtualEventMap();
            Map<String, String> stringMap = toJsonStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.VIRTUAL_EVENT_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_EVENT_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_EVENT_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualEventAttrMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP);
        try {
            Map<String, Set<String>> data = frontDao.getVirtualEventAttrMap();
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : data.entrySet()) {
                stringMap.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
            }
            kvRocksService.atomicReplaceHash(CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncEventAttrAliasMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_ATTR_ALIAS_MAP);
        try {
            Map<String, String> data = frontDao.getEventAttrAliasMap();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.EVENT_ATTR_ALIAS_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_ATTR_ALIAS_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_ATTR_ALIAS_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualEventAppidsSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET);
        try {
            Set<String> data = frontDao.getVirtualEventAppidsSet();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualPropAppIdsSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET);
        try {
            Set<String> data = frontDao.getVirtualPropAppIdsSet();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncEventVirtualAttrIdsSet() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET);
        try {
            Set<String> data = frontDao.getEventVirtualAttrIds();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualEventPropMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP);
        try {
            Map<String, List<String>> data = frontDao.getVirtualEventPropMap();
            Map<String, String> stringMap = toJsonStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncVirtualUserPropMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.VIRTUAL_USER_PROP_MAP);
        try {
            Map<String, List<String>> data = frontDao.getVirtualUserPropMap();
            Map<String, String> stringMap = toJsonStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.VIRTUAL_USER_PROP_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.VIRTUAL_USER_PROP_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.VIRTUAL_USER_PROP_MAP, e);
        }
        result.finish();
        return result;
    }

    // 投放相关
    private SyncResult syncOpenAdvertisingFunctionAppMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.OPEN_ADVERTISING_FUNCTION_APP_MAP);
        try {
            Map<String, Integer> data = frontDao.getOpenAdvertisingFunctionAppId();
            Map<String, String> stringMap = toStringMap(data);
            kvRocksService.atomicReplaceHash(CacheKeyConstants.OPEN_ADVERTISING_FUNCTION_APP_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.OPEN_ADVERTISING_FUNCTION_APP_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.OPEN_ADVERTISING_FUNCTION_APP_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncLidAndChannelEventMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.LID_AND_CHANNEL_EVENT_MAP);
        try {
            Map<String, String> data = frontDao.getLidAndChannelEvent();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.LID_AND_CHANNEL_EVENT_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.LID_AND_CHANNEL_EVENT_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.LID_AND_CHANNEL_EVENT_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAppIdSMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.APP_ID_S_MAP);
        try {
            Map<Integer, Integer> data = frontDao.getEIdMap();
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
                stringMap.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
            kvRocksService.atomicReplaceHash(CacheKeyConstants.APP_ID_S_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.APP_ID_S_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.APP_ID_S_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAdFrequencySet() {
        SyncResult result = new SyncResult(CacheKeyConstants.AD_FREQUENCY_SET);
        try {
            Set<String> data = frontDao.getAdsFrequency();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.AD_FREQUENCY_SET, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.AD_FREQUENCY_SET, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.AD_FREQUENCY_SET, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncAdsLinkEventMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.ADS_LINK_EVENT_MAP);
        try {
            Map<String, AdsLinkEvent> data = frontDao.getAdsLinkEventMap();
            Map<String, String> stringMap = new HashMap<>();
            for (Map.Entry<String, AdsLinkEvent> entry : data.entrySet()) {
                stringMap.put(entry.getKey(), entry.getValue().toJsonString());
            }
            kvRocksService.atomicReplaceHash(CacheKeyConstants.ADS_LINK_EVENT_MAP, stringMap);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.ADS_LINK_EVENT_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.ADS_LINK_EVENT_MAP, e);
        }
        result.finish();
        return result;
    }

    // ==========================================================
    // DW模块同步方法
    // ==========================================================

    private SyncResult syncEventAttrColumnMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.EVENT_ATTR_COLUMN_MAP);
        try {
            Map<String, String> data = frontDao.getAttrColumnName();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.EVENT_ATTR_COLUMN_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.EVENT_ATTR_COLUMN_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.EVENT_ATTR_COLUMN_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncBaseCurrentMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.BASE_CURRENT_MAP);
        try {
            Map<String, String> data = frontDao.getCurrentKuduTable();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.BASE_CURRENT_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.BASE_CURRENT_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.BASE_CURRENT_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncOpenCdpAppidMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.OPEN_CDP_APPID_MAP);
        try {
            Map<String, String> data = frontDao.getOpenCdp();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.OPEN_CDP_APPID_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.OPEN_CDP_APPID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.OPEN_CDP_APPID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncYearWeek() {
        SyncResult result = new SyncResult(CacheKeyConstants.YEAR_WEEK);
        try {
            Map<String, String> data = frontDao.getYearWeek();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.YEAR_WEEK, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.YEAR_WEEK, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.YEAR_WEEK, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncCidByAidMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.CID_BY_AID_MAP);
        try {
            Map<String, String> data = frontDao.getCompanyIdsByAppId();
            kvRocksService.atomicReplaceHash(CacheKeyConstants.CID_BY_AID_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.CID_BY_AID_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.CID_BY_AID_MAP, e);
        }
        result.finish();
        return result;
    }

    private SyncResult syncBusinessMap() {
        SyncResult result = new SyncResult(CacheKeyConstants.BUSINESS_MAP);
        try {
            Set<String> data = frontDao.getBusiness();
            kvRocksService.atomicReplaceSet(CacheKeyConstants.BUSINESS_MAP, data);
            result.addCount(data.size());
            log.info("Synced {} - {} records", CacheKeyConstants.BUSINESS_MAP, data.size());
        } catch (Exception e) {
            result.fail(e.getMessage());
            log.error("Failed to sync {}", CacheKeyConstants.BUSINESS_MAP, e);
        }
        result.finish();
        return result;
    }

    // ==================== 辅助方法 ====================
    
    private Map<String, String> toStringMap(Map<String, Integer> map) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            result.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return result;
    }

    private Set<String> toStringSet(Set<Integer> set) {
        Set<String> result = new HashSet<>();
        for (Integer item : set) {
            result.add(String.valueOf(item));
        }
        return result;
    }

    private Map<String, String> toJsonStringMap(Map<String, List<String>> map) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            result.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
        }
        return result;
    }

    private void recordSyncStart() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());
        kvRocksService.setValue(CacheKeyConstants.SYNC_STATUS, "RUNNING");
        kvRocksService.setValue(CacheKeyConstants.SYNC_TIMESTAMP, timestamp);
        log.info("Sync started at {}", timestamp);
    }

    private void recordSyncComplete() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = sdf.format(new Date());
        boolean allSuccess = syncResults.stream().allMatch(SyncResult::isSuccess);
        kvRocksService.setValue(CacheKeyConstants.SYNC_STATUS, allSuccess ? "SUCCESS" : "PARTIAL_FAILURE");
        kvRocksService.setValue(CacheKeyConstants.SYNC_TIMESTAMP, timestamp);
        kvRocksService.setValue(CacheKeyConstants.SYNC_VERSION, String.valueOf(System.currentTimeMillis()));
        log.info("Sync completed at {}, status: {}", timestamp, allSuccess ? "SUCCESS" : "PARTIAL_FAILURE");
    }

    private void printSyncSummary(long startTime) {
        long totalTime = System.currentTimeMillis() - startTime;
        long totalRecords = syncResults.stream().mapToLong(r -> r.getSyncCount().get()).sum();
        long failedTasks = syncResults.stream().filter(r -> !r.isSuccess()).count();
        
        log.info("========================================");
        log.info("Cache Sync Summary");
        log.info("Total tasks: {}, Success: {}, Failed: {}", 
                syncResults.size(), syncResults.size() - failedTasks, failedTasks);
        log.info("Total records: {}, Time: {} ms", totalRecords, totalTime);
        log.info("========================================");
        
        for (SyncResult result : syncResults) {
            log.info(result.toString());
        }
        
        if (failedTasks > 0) {
            log.error("Failed tasks:");
            syncResults.stream().filter(r -> !r.isSuccess())
                    .forEach(r -> log.error("  - {}: {}", r.getCacheName(), r.getErrorMessage()));
        }
    }
}
