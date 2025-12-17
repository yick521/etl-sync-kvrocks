package com.zhugeio.cachesync.dao;

import com.zhugeio.cachesync.entity.AdsLinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * 前端数据库访问层
 * 
 * 优化策略：合并相同表的查询，减少数据库访问次数
 * - company_app: 4次 -> 1次
 * - user_prop_meta: 4次 -> 1次
 * - event: 2次 -> 1次
 * - event_attr: 5次 -> 1次
 */
@Slf4j
@Repository
public class FrontDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // ==========================================================
    // 批量查询结果缓存（同一次sync周期内复用）
    // ==========================================================
    
    private volatile CompanyAppData companyAppData;
    private volatile UserPropMetaData userPropMetaData;
    private volatile EventData eventData;
    private volatile EventAttrData eventAttrData;

    /**
     * 清除批量查询缓存，每次sync开始时调用
     */
    public void clearBatchCache() {
        companyAppData = null;
        userPropMetaData = null;
        eventData = null;
        eventAttrData = null;
        log.info("Batch query cache cleared");
    }

    // ==========================================================
    // company_app 表批量查询 (原4次 -> 1次)
    // ==========================================================

    public static class CompanyAppData {
        public final Map<String, Integer> appKeyAppIdMap = new HashMap<>();
        public final Map<String, String> cidByAidMap = new HashMap<>();
        public final Set<Integer> noneAutoCreateSet = new HashSet<>();
        public final Set<Integer> validAppIds = new HashSet<>();
    }

    public CompanyAppData getCompanyAppData() {
        if (companyAppData != null) {
            return companyAppData;
        }
        
        synchronized (this) {
            if (companyAppData != null) {
                return companyAppData;
            }
            
            CompanyAppData data = new CompanyAppData();
            String sql = "SELECT id, app_key, company_id, is_delete, stop, auto_event FROM company_app";
            
            Set<Integer> transferIds = new HashSet<>(
                jdbcTemplate.queryForList("SELECT id FROM tmp_transfer WHERE status = 2", Integer.class)
            );
            
            jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
                Integer id = rs.getInt("id");
                String appKey = rs.getString("app_key");
                Integer companyId = rs.getInt("company_id");
                Integer isDelete = rs.getInt("is_delete");
                Integer stop = rs.getInt("stop");
                Integer autoEvent = rs.getInt("auto_event");
                
                data.cidByAidMap.put(String.valueOf(id), String.valueOf(companyId));
                
                if (isDelete == 0 && stop == 0) {
                    data.validAppIds.add(id);
                    if (appKey != null && !transferIds.contains(id)) {
                        data.appKeyAppIdMap.put(appKey, id);
                    }
                    if (autoEvent == 0) {
                        data.noneAutoCreateSet.add(id);
                    }
                }
            });
            
            companyAppData = data;
            log.info("Loaded company_app: appKeyAppIdMap={}, cidByAidMap={}, noneAutoCreateSet={}", 
                    data.appKeyAppIdMap.size(), data.cidByAidMap.size(), data.noneAutoCreateSet.size());
            return data;
        }
    }

    public Map<String, Integer> getAppKeyIdMaps() {
        return getCompanyAppData().appKeyAppIdMap;
    }
    
    public Set<Integer> getNoneAutoCreateAppIds() {
        return getCompanyAppData().noneAutoCreateSet;
    }
    
    public Map<String, String> getCompanyIdsByAppId() {
        return getCompanyAppData().cidByAidMap;
    }

    // ==========================================================
    // user_prop_meta 表批量查询 (原4次 -> 1次)
    // ==========================================================

    public static class UserPropMetaData {
        public final Map<String, Integer> propIdMap = new HashMap<>();
        public final Map<String, String> propIdOriginalMap = new HashMap<>();
        public final Set<Integer> blackPropSet = new HashSet<>();
        public final Map<String, List<String>> virtualUserPropMap = new HashMap<>();
        public final Set<String> virtualPropAppIds = new HashSet<>();
    }

    public UserPropMetaData getUserPropMetaData() {
        if (userPropMetaData != null) {
            return userPropMetaData;
        }
        
        synchronized (this) {
            if (userPropMetaData != null) {
                return userPropMetaData;
            }
            
            UserPropMetaData data = new UserPropMetaData();
            String sql = "SELECT id, app_id, owner, name, is_delete, attr_type, sql_json, table_fields FROM user_prop_meta";
            
            jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
                Integer id = rs.getInt("id");
                Integer appId = rs.getInt("app_id");
                String owner = rs.getString("owner");
                String name = rs.getString("name");
                Integer isDelete = rs.getInt("is_delete");
                Integer attrType = rs.getInt("attr_type");
                String sqlJson = rs.getString("sql_json");
                String tableFields = rs.getString("table_fields");
                
                if (name != null) {
                    data.propIdMap.put(appId + "_" + owner + "_" + name.toUpperCase(), id);
                    data.propIdOriginalMap.put(appId + "_" + owner + "_" + id, name);
                }
                
                if (isDelete == 1) {
                    data.blackPropSet.add(id);
                }
                
                if (attrType == 1 && isDelete == 0 && name != null) {
                    data.virtualPropAppIds.add(String.valueOf(appId));
                    
                    com.alibaba.fastjson.JSONObject jsonObj = new com.alibaba.fastjson.JSONObject();
                    jsonObj.put("name", name);
                    jsonObj.put("define", sqlJson);
                    jsonObj.put("tableFields", tableFields);
                    
                    data.virtualUserPropMap.computeIfAbsent(String.valueOf(appId), k -> new ArrayList<>())
                            .add(jsonObj.toJSONString());
                }
            });
            
            userPropMetaData = data;
            log.info("Loaded user_prop_meta: propIdMap={}, blackPropSet={}, virtualUserPropMap={}", 
                    data.propIdMap.size(), data.blackPropSet.size(), data.virtualUserPropMap.size());
            return data;
        }
    }

    public Map<String, Integer> getUserPropIds() {
        return getUserPropMetaData().propIdMap;
    }
    
    public Map<String, String> getOriginalUserPropIds() {
        return getUserPropMetaData().propIdOriginalMap;
    }
    
    public Set<Integer> getBlackUserPropIds() {
        return getUserPropMetaData().blackPropSet;
    }
    
    public Map<String, List<String>> getVirtualUserPropMap() {
        return getUserPropMetaData().virtualUserPropMap;
    }

    // ==========================================================
    // event 表批量查询 (原2次 -> 1次)
    // ==========================================================

    public static class EventData {
        public final Map<String, Integer> eventIdMap = new HashMap<>();
        public final Set<Integer> blackEventSet = new HashSet<>();
        public final Map<Integer, EventInfo> eventInfoMap = new HashMap<>();
    }
    
    public static class EventInfo {
        public final Integer appId;
        public final String eventName;
        public final String owner;
        public final boolean isValid;
        
        public EventInfo(Integer appId, String eventName, String owner, boolean isValid) {
            this.appId = appId;
            this.eventName = eventName;
            this.owner = owner;
            this.isValid = isValid;
        }
    }

    public EventData getEventData() {
        if (eventData != null) {
            return eventData;
        }
        
        synchronized (this) {
            if (eventData != null) {
                return eventData;
            }
            
            EventData data = new EventData();
            String sql = "SELECT id, app_id, owner, event_name, is_delete, is_stop FROM event";
            
            jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
                Integer id = rs.getInt("id");
                Integer appId = rs.getInt("app_id");
                String owner = rs.getString("owner");
                String eventName = rs.getString("event_name");
                Integer isDelete = rs.getInt("is_delete");
                Integer isStop = rs.getInt("is_stop");
                
                data.eventInfoMap.put(id, new EventInfo(appId, eventName, owner, isDelete == 0));
                
                if (eventName != null) {
                    data.eventIdMap.put(appId + "_" + owner + "_" + eventName, id);
                }
                
                if (isDelete == 1 || isStop == 1) {
                    data.blackEventSet.add(id);
                }
            });
            
            eventData = data;
            log.info("Loaded event: eventIdMap={}, blackEventSet={}", 
                    data.eventIdMap.size(), data.blackEventSet.size());
            return data;
        }
    }

    public Map<String, Integer> getEventIds() {
        return getEventData().eventIdMap;
    }
    
    public Set<Integer> getBlackEventIds() {
        return getEventData().blackEventSet;
    }

    // ==========================================================
    // event_attr 表批量查询 (原5次 -> 1次)
    // ==========================================================

    public static class EventAttrData {
        public final Map<String, Integer> attrIdMap = new HashMap<>();
        public final Set<Integer> blackAttrSet = new HashSet<>();
        public final Map<String, String> attrAliasMap = new HashMap<>();
        public final Map<String, String> attrColumnMap = new HashMap<>();
        public final Map<String, List<String>> virtualEventPropMap = new HashMap<>();
        public final Set<String> virtualAttrIds = new HashSet<>();
        public final Set<String> virtualPropAppIds = new HashSet<>();
    }

    public EventAttrData getEventAttrData() {
        if (eventAttrData != null) {
            return eventAttrData;
        }
        
        synchronized (this) {
            if (eventAttrData != null) {
                return eventAttrData;
            }
            
            EventData evtData = getEventData();
            Set<Integer> validAppIds = getCompanyAppData().validAppIds;
            
            EventAttrData data = new EventAttrData();
            String sql = "SELECT event_id, attr_id, attr_name, owner, is_delete, is_stop, " +
                         "attr_type, alias_name, column_name, sql_json FROM event_attr";
            
            jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
                Integer eventId = rs.getInt("event_id");
                Long attrIdLong = rs.getLong("attr_id");
                Integer attrId = attrIdLong.intValue();
                String attrName = rs.getString("attr_name");
                String owner = rs.getString("owner");
                Integer isDelete = rs.getInt("is_delete");
                Integer isStop = rs.getInt("is_stop");
                Integer attrType = rs.getInt("attr_type");
                String aliasName = rs.getString("alias_name");
                String columnName = rs.getString("column_name");
                String sqlJson = rs.getString("sql_json");
                
                EventInfo eventInfo = evtData.eventInfoMap.get(eventId);
                if (eventInfo == null) {
                    return;
                }
                
                Integer appId = eventInfo.appId;
                String eventName = eventInfo.eventName;
                
                if (columnName != null) {
                    data.attrColumnMap.put(eventId + "_" + attrId, columnName);
                }
                
                if (attrName != null && validAppIds.contains(appId)) {
                    data.attrIdMap.put(appId + "_" + eventId + "_" + owner + "_" + attrName.toUpperCase(), attrId);
                }
                
                if ((isDelete == 1 || isStop == 1) && attrType != 1) {
                    data.blackAttrSet.add(attrId);
                }
                
                if (aliasName != null && !aliasName.isEmpty() && eventInfo.isValid) {
                    data.attrAliasMap.put(appId + "_" + eventInfo.owner + "_" + eventName + "_" + attrName, aliasName);
                }
                
                if (attrType == 1 && isDelete == 0) {
                    data.virtualAttrIds.add(String.valueOf(attrId));
                    data.virtualPropAppIds.add(String.valueOf(appId));
                    
                    com.alibaba.fastjson.JSONObject jsonObj = new com.alibaba.fastjson.JSONObject();
                    jsonObj.put("name", attrName);
                    jsonObj.put("define", sqlJson);
                    
                    data.virtualEventPropMap.computeIfAbsent(appId + "_eP_" + eventName, k -> new ArrayList<>())
                            .add(jsonObj.toJSONString());
                }
            });
            
            eventAttrData = data;
            log.info("Loaded event_attr: attrIdMap={}, blackAttrSet={}, attrColumnMap={}, virtualEventPropMap={}", 
                    data.attrIdMap.size(), data.blackAttrSet.size(), 
                    data.attrColumnMap.size(), data.virtualEventPropMap.size());
            return data;
        }
    }

    public Map<String, Integer> getEventAttrIds() {
        return getEventAttrData().attrIdMap;
    }
    
    public Set<Integer> getBlackEventAttrIds() {
        return getEventAttrData().blackAttrSet;
    }
    
    public Map<String, String> getEventAttrAliasMap() {
        return getEventAttrData().attrAliasMap;
    }
    
    public Map<String, String> getAttrColumnName() {
        return getEventAttrData().attrColumnMap;
    }
    
    public Map<String, List<String>> getVirtualEventPropMap() {
        return getEventAttrData().virtualEventPropMap;
    }
    
    public Set<String> getEventVirtualAttrIds() {
        return getEventAttrData().virtualAttrIds;
    }

    public Set<String> getVirtualPropAppIdsSet() {
        Set<String> result = new HashSet<>();
        result.addAll(getUserPropMetaData().virtualPropAppIds);
        result.addAll(getEventAttrData().virtualPropAppIds);
        return result;
    }

    // ==========================================================
    // 以下方法无法合并，保持独立查询
    // ==========================================================

    public Map<String, Integer> getSdkPlatformHasDataMap() {
        String sql = "SELECT main_id, sdk_platform, has_data FROM app";
        Map<String, Integer> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.put(rs.getInt("main_id") + "_" + rs.getInt("sdk_platform"), rs.getInt("has_data"));
        });
        return result;
    }

    public Map<String, Integer> getDevicePropIds() {
        String sql = "SELECT app_id, owner, name, id FROM device_prop";
        Map<String, Integer> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String name = rs.getString("name");
            if (name != null) {
                result.put(rs.getInt("app_id") + "_" + rs.getString("owner") + "_" + name, rs.getInt("id"));
            }
        });
        return result;
    }

    public Set<Integer> getForbiddenCreateEventAppIds() {
        String sql = "SELECT a.id FROM company_app a, event b " +
                     "WHERE a.is_delete = 0 AND b.is_delete = 0 AND b.owner = 'zg' AND b.is_stop = 0 AND a.id = b.app_id " +
                     "GROUP BY a.id HAVING COUNT(*) >= MAX(a.event_sum)";
        return new HashSet<>(jdbcTemplate.queryForList(sql, Integer.class));
    }

    public Set<Integer> getUploadDatas() {
        return new HashSet<>(jdbcTemplate.queryForList("SELECT app_id FROM app_data", Integer.class));
    }

    public Set<Integer> getForbiddenCreateEventAttrEventIds() {
        String sql = "SELECT b.id FROM company_app a, event b, event_attr c " +
                     "WHERE a.is_delete = 0 AND b.is_delete = 0 AND b.is_stop = 0 " +
                     "AND a.id = b.app_id AND b.id = c.event_id AND c.is_stop = 0 " +
                     "GROUP BY b.id HAVING COUNT(*) >= MAX(a.attr_sum)";
        return new HashSet<>(jdbcTemplate.queryForList(sql, Integer.class));
    }

    public Set<String> getEventPlatforms() {
        String sql = "SELECT event_id, platform FROM event_platform";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(rs.getInt("event_id") + "_" + rs.getInt("platform"));
        });
        return result;
    }

    public Set<String> getEventAttrPlatforms() {
        // 注意：字段名可能是event_attr_id而不是attr_id，根据实际表结构调整
        String sql = "SELECT event_attr_id, platform FROM event_attr_platform";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(rs.getLong("event_attr_id") + "_" + rs.getInt("platform"));
        });
        return result;
    }

    public Set<String> getDevicePropPlatforms() {
        String sql = "SELECT prop_id, platform FROM device_prop_platform";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(rs.getInt("prop_id") + "_" + rs.getInt("platform"));
        });
        return result;
    }

    // ==========================================================
    // 投放相关
    // ==========================================================

    public Map<String, Integer> getOpenAdvertisingFunctionAppId() {
        String sql = "SELECT a.app_key, a.id as app_id FROM company_app a " +
                "JOIN advertising_app b ON a.app_key = b.app_key " +
                "WHERE a.is_delete = 0 AND b.is_delete = 0 AND b.stop = 0";
        Map<String, Integer> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String appKey = rs.getString("app_key");
            if (appKey != null) {
                result.put(appKey, rs.getInt("app_id"));
            }
        });
        return result;
    }

    public Map<String, String> getLidAndChannelEvent() {
        String sql = "SELECT link_id, event_id, channel_event FROM ads_link_event WHERE is_delete = 0";
        Map<String, String> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.put(rs.getInt("link_id") + "_" + rs.getInt("event_id"), rs.getString("channel_event"));
        });
        return result;
    }

    public Map<Integer, Integer> getEIdMap() {
        String sql = "SELECT link_id, event_id FROM ads_link_event WHERE is_delete = 0";
        Map<Integer, Integer> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.put(rs.getInt("link_id"), rs.getInt("event_id"));
        });
        return result;
    }

    public Set<String> getAdsFrequency() {
        String sql = "SELECT event_id, link_id, zg_id FROM ads_frequency_first";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(rs.getInt("event_id") + "_" + rs.getInt("link_id") + "_" + rs.getString("zg_id"));
        });
        return result;
    }

    public Map<String, AdsLinkEvent> getAdsLinkEventMap() {
        String sql = "SELECT link_id, event_id, event_ids, channel_event, match_json, frequency, windows_time " +
                     "FROM ads_link_event WHERE is_delete = 0";
        Map<String, AdsLinkEvent> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            AdsLinkEvent event = new AdsLinkEvent();
            event.setLinkId(rs.getInt("link_id"));
            event.setEventId(rs.getInt("event_id"));
            event.setEventIds(rs.getString("event_ids"));
            event.setChannelEvent(rs.getString("channel_event"));
            event.setMatchJson(rs.getString("match_json"));
            event.setFrequency(rs.getInt("frequency"));
            Long windowTime = rs.getLong("windows_time");
            event.setWindowTime(windowTime != null && windowTime > 0 ? windowTime : 2592000L);
            result.put(event.getEventId() + "_" + event.getLinkId(), event);
        });
        return result;
    }

    // ==========================================================
    // 虚拟事件相关
    // ==========================================================

    public Map<String, List<String>> getVirtualEventMap() {
        String sql = "SELECT event_name, alias_name, app_id, event_json FROM virtual_event " +
                     "WHERE is_delete = 0 AND event_status = 0";
        Map<String, List<String>> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String eventJson = rs.getString("event_json");
            if (eventJson != null) {
                try {
                    com.alibaba.fastjson.JSONObject jsonObj = com.alibaba.fastjson.JSON.parseObject(eventJson);
                    jsonObj.put("virtual_name", rs.getString("event_name"));
                    jsonObj.put("virtual_alias", rs.getString("alias_name"));
                    String key = rs.getLong("app_id") + "_" + jsonObj.getString("owner") + "_" + jsonObj.getString("eventName");
                    result.computeIfAbsent(key, k -> new ArrayList<>()).add(jsonObj.toJSONString());
                } catch (Exception e) {
                    log.warn("Parse virtual event json failed: {}", eventJson, e);
                }
            }
        });
        return result;
    }

    public Map<String, Set<String>> getVirtualEventAttrMap() {
        String sql = "SELECT event_name, app_id, event_json FROM virtual_event " +
                     "WHERE is_delete = 0 AND event_status = 0";
        Map<String, Set<String>> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String virtualEventName = rs.getString("event_name");
            Long appId = rs.getLong("app_id");
            String eventJson = rs.getString("event_json");
            if (eventJson != null) {
                try {
                    com.alibaba.fastjson.JSONObject jsonObj = com.alibaba.fastjson.JSON.parseObject(eventJson);
                    com.alibaba.fastjson.JSONArray attrs = jsonObj.getJSONArray("attrs");
                    if (attrs != null) {
                        String key = appId + "_" + virtualEventName + "_" + jsonObj.getString("owner") + "_" + jsonObj.getString("eventName");
                        Set<String> attrSet = result.computeIfAbsent(key, k -> new HashSet<>());
                        for (int i = 0; i < attrs.size(); i++) {
                            attrSet.add(attrs.getString(i));
                        }
                    }
                } catch (Exception e) {
                    log.warn("Parse virtual event attr json failed: {}", eventJson, e);
                }
            }
        });
        return result;
    }

    public Set<String> getVirtualEventAppidsSet() {
        String sql = "SELECT app_id FROM virtual_event WHERE is_delete = 0 AND event_status = 0 GROUP BY app_id";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(String.valueOf(rs.getLong("app_id")));
        });
        return result;
    }

    // ==========================================================
    // DW模块独立查询
    // ==========================================================

    public Map<String, String> getCurrentKuduTable() {
        String sql = "SELECT base_name, current_name FROM kudu_exchange";
        Map<String, String> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            String baseName = rs.getString("base_name");
            String currentName = rs.getString("current_name");
            if (baseName != null && currentName != null) {
                result.put(baseName, currentName);
            }
        });
        return result;
    }

    public Map<String, String> getOpenCdp() {
        Set<Integer> validAppIds = getCompanyAppData().validAppIds;
        String sql = "SELECT app_id, app_config FROM app_custom_config " +
                     "WHERE app_config_type = 'id_mapping' AND app_config = 'true'";
        Map<String, String> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            Integer appId = rs.getInt("app_id");
            if (validAppIds.contains(appId)) {
                result.put(String.valueOf(appId), rs.getString("app_config"));
            }
        });
        return result;
    }

    public Map<String, String> getYearWeek() {
        String sql = "SELECT day, year_week FROM etl_yearkweek";
        Map<String, String> result = new HashMap<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.put(String.valueOf(rs.getInt("day")), String.valueOf(rs.getInt("year_week")));
        });
        return result;
    }

    public Set<String> getBusiness() {
        String sql = "SELECT company_id, identifier FROM business WHERE del = 0 AND state = 1";
        Set<String> result = new HashSet<>();
        jdbcTemplate.query(sql, (RowCallbackHandler) rs -> {
            result.add(rs.getInt("company_id") + "_" + rs.getString("identifier"));
        });
        return result;
    }
}
