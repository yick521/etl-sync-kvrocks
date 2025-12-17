package com.zhugeio.cachesync.constants;

/**
 * 缓存Key常量定义
 * 
 * 保持与原Scala FrontCache中的key格式完全一致
 * 包含ID模块和DW模块的所有缓存
 */
public final class CacheKeyConstants {

    private CacheKeyConstants() {}

    // ==========================================================
    // ID模块 - Hash类型
    // ==========================================================
    
    /**
     * appKey -> appId 映射
     * Hash Key: appKeyAppIdMap
     * Field: ${appKey}
     * Value: ${appId}
     */
    public static final String APP_KEY_APP_ID_MAP = "appKeyAppIdMap";
    
    /**
     * app平台数据状态
     * Hash Key: appIdSdkHasDataMap
     * Field: ${appId}_${platform}
     * Value: ${hasData}
     */
    public static final String APP_ID_SDK_HAS_DATA_MAP = "appIdSdkHasDataMap";
    
    /**
     * 用户属性ID映射
     * Hash Key: appIdPropIdMap
     * Field: ${appId}_${owner}_${name(大写)}
     * Value: ${propId}
     */
    public static final String APP_ID_PROP_ID_MAP = "appIdPropIdMap";
    
    /**
     * 用户属性原始名称映射
     * Hash Key: appIdPropIdOriginalMap
     * Field: ${appId}_${owner}_${propId}
     * Value: ${propName(原始大小写)}
     */
    public static final String APP_ID_PROP_ID_ORIGINAL_MAP = "appIdPropIdOriginalMap";
    
    /**
     * 事件ID映射
     * Hash Key: appIdEventIdMap
     * Field: ${appId}_${owner}_${eventName}
     * Value: ${eventId}
     */
    public static final String APP_ID_EVENT_ID_MAP = "appIdEventIdMap";
    
    /**
     * 事件属性ID映射
     * Hash Key: appIdEventAttrIdMap
     * Field: ${appId}_${eventId}_${owner}_${attrName(大写)}
     * Value: ${attrId}
     */
    public static final String APP_ID_EVENT_ATTR_ID_MAP = "appIdEventAttrIdMap";
    
    /**
     * 设备属性ID映射
     * Hash Key: appIdDevicePropIdMap
     * Field: ${appId}_${owner}_${propName}
     * Value: ${propId}
     */
    public static final String APP_ID_DEVICE_PROP_ID_MAP = "appIdDevicePropIdMap";

    // ========== Set Key 前缀 ==========
    
    /**
     * 禁止创建事件的应用ID集合
     * Set Key: appIdCreateEventForbidSet
     */
    public static final String APP_ID_CREATE_EVENT_FORBID_SET = "appIdCreateEventForbidSet";
    
    /**
     * 已上传数据的应用ID集合
     * Set Key: appIdUploadDataSet
     */
    public static final String APP_ID_UPLOAD_DATA_SET = "appIdUploadDataSet";
    
    /**
     * 黑名单用户属性ID集合
     * Set Key: blackUserPropSet
     */
    public static final String BLACK_USER_PROP_SET = "blackUserPropSet";
    
    /**
     * 黑名单事件ID集合
     * Set Key: blackEventIdSet
     */
    public static final String BLACK_EVENT_ID_SET = "blackEventIdSet";
    
    /**
     * 非自动创建应用ID集合
     * Set Key: appIdNoneAutoCreateSet
     */
    public static final String APP_ID_NONE_AUTO_CREATE_SET = "appIdNoneAutoCreateSet";
    
    /**
     * 黑名单事件属性ID集合
     * Set Key: blackEventAttrIdSet
     */
    public static final String BLACK_EVENT_ATTR_ID_SET = "blackEventAttrIdSet";
    
    /**
     * 禁止创建属性的事件ID集合
     * Set Key: eventIdCreateAttrForbiddenSet
     */
    public static final String EVENT_ID_CREATE_ATTR_FORBIDDEN_SET = "eventIdCreateAttrForbiddenSet";
    
    /**
     * 事件平台映射集合
     * Set Key: eventIdPlatform
     * Member: ${eventId}_${platform}
     */
    public static final String EVENT_ID_PLATFORM = "eventIdPlatform";
    
    /**
     * 事件属性平台映射集合
     * Set Key: eventAttrdPlatform
     * Member: ${attrId}_${platform}
     */
    public static final String EVENT_ATTR_PLATFORM = "eventAttrdPlatform";
    
    /**
     * 设备属性平台映射集合
     * Set Key: devicePropPlatform
     * Member: ${propId}_${platform}
     */
    public static final String DEVICE_PROP_PLATFORM = "devicePropPlatform";
    
    /**
     * 虚拟事件应用ID集合
     * Set Key: virtualEventAppidsSet
     */
    public static final String VIRTUAL_EVENT_APPIDS_SET = "virtualEventAppidsSet";
    
    /**
     * 虚拟属性应用ID集合
     * Set Key: virtualPropAppIdsSet
     */
    public static final String VIRTUAL_PROP_APP_IDS_SET = "virtualPropAppIdsSet";
    
    /**
     * 虚拟事件属性ID集合
     * Set Key: eventVirtualAttrIdsSet
     */
    public static final String EVENT_VIRTUAL_ATTR_IDS_SET = "eventVirtualAttrIdsSet";

    // ========== 投放相关 ==========
    
    /**
     * 开启广告投放的应用映射
     * Hash Key: openAdvertisingFunctionAppMap
     * Field: ${appKey}
     * Value: ${appId}
     */
    public static final String OPEN_ADVERTISING_FUNCTION_APP_MAP = "openAdvertisingFunctionAppMap";
    
    /**
     * 链接ID和渠道事件映射
     * Hash Key: lidAndChannelEventMap
     * Field: ${lid}_${eventId}
     * Value: ${channelEvent}
     */
    public static final String LID_AND_CHANNEL_EVENT_MAP = "lidAndChannelEventMap";
    
    /**
     * appId到eventId映射
     * Hash Key: appIdSMap
     * Field: ${lid}
     * Value: ${eventId}
     */
    public static final String APP_ID_S_MAP = "appIdSMap";
    
    /**
     * 广告首次回传记录集合
     * Set Key: adFrequencySet
     * Member: ${eventId}_${lid}_${zgId}
     */
    public static final String AD_FREQUENCY_SET = "adFrequencySet";
    
    /**
     * 广告链接事件映射
     * Hash Key: adsLinkEventMap
     * Field: ${eventId}_${lid}
     * Value: JSON(AdsLinkEvent)
     */
    public static final String ADS_LINK_EVENT_MAP = "adsLinkEventMap";

    // ========== 虚拟事件/属性相关 ==========
    
    /**
     * 虚拟事件映射
     * Hash Key: virtualEventMap
     * Field: ${appId}_${owner}_${eventName}
     * Value: JSON Array
     */
    public static final String VIRTUAL_EVENT_MAP = "virtualEventMap";
    
    /**
     * 虚拟事件属性映射
     * Hash Key: virtualEventAttrMap
     * Field: ${appId}_${virtualEventName}_${owner}_${eventName}
     * Value: JSON Set
     */
    public static final String VIRTUAL_EVENT_ATTR_MAP = "virtualEventAttrMap";
    
    /**
     * 事件属性别名映射
     * Hash Key: eventAttrAliasMap
     * Field: ${appId}_${owner}_${eventName}_${attrName}
     * Value: ${aliasName}
     */
    public static final String EVENT_ATTR_ALIAS_MAP = "eventAttrAliasMap";
    
    /**
     * 虚拟事件属性映射
     * Hash Key: virtualEventPropMap
     * Field: ${appId}_eP_${eventName}
     * Value: JSON Array
     */
    public static final String VIRTUAL_EVENT_PROP_MAP = "virtualEventPropMap";
    
    /**
     * 虚拟用户属性映射
     * Hash Key: virtualUserPropMap
     * Field: ${appId}
     * Value: JSON Array
     */
    public static final String VIRTUAL_USER_PROP_MAP = "virtualUserPropMap";

    // ========== 同步元数据 ==========
    
    /**
     * 同步版本号
     */
    public static final String SYNC_VERSION = "sync:version";
    
    /**
     * 同步时间戳
     */
    public static final String SYNC_TIMESTAMP = "sync:timestamp";
    
    /**
     * 同步状态
     */
    public static final String SYNC_STATUS = "sync:status";

    // ==========================================================
    // DW模块 - Hash类型
    // ==========================================================
    
    /**
     * 事件属性列名映射
     * Hash Key: eventAttrColumnMap
     * Field: ${eventId}_${attrId}
     * Value: ${columnName}
     */
    public static final String EVENT_ATTR_COLUMN_MAP = "eventAttrColumnMap";
    
    /**
     * Kudu表当前名称映射
     * Hash Key: baseCurrentMap
     * Field: ${baseName}
     * Value: ${currentName}
     */
    public static final String BASE_CURRENT_MAP = "baseCurrentMap";
    
    /**
     * 开启CDP的应用配置
     * Hash Key: openCdpAppidMap
     * Field: ${appId}
     * Value: ${appConfig}
     */
    public static final String OPEN_CDP_APPID_MAP = "openCdpAppidMap";
    
    /**
     * 年周映射
     * Hash Key: yearweek
     * Field: ${day}
     * Value: ${yearWeek}
     */
    public static final String YEAR_WEEK = "yearweek";
    
    /**
     * 公司ID与应用ID映射
     * Hash Key: cidByAidMap
     * Field: ${appId}
     * Value: ${companyId}
     */
    public static final String CID_BY_AID_MAP = "cidByAidMap";

    // ==========================================================
    // DW模块 - Set类型
    // ==========================================================
    
    /**
     * 业务标识集合
     * Set Key: businessMap
     * Member: ${companyId}_${identifier}
     */
    public static final String BUSINESS_MAP = "businessMap";
}
