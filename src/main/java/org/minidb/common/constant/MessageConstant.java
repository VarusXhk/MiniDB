package org.minidb.common.constant;

/**
 * 信息提示常量类
 */
public class MessageConstant {
    // Common Messages
    public static final String CACHE_FULL = "缓存已满";
    public static final String FILE_EXIST = "文件已存在";
    public static final String FILE_NOT_EXIST = "该文件不存在";
    public static final String FILE_CANNOT_RW = "该文件不具备读写功能";

    // Messages in Data Manager
    public static final String BAD_LOG_FILE = "日志文件已损坏";
    public static final String MEMORY_SHORTAGE = "内存过小";
    public static final String DATA_OVERFLOW = "数据过大";
    public static final String DATABASE_BUSY = "数据库繁忙";

    // Messages in Transaction Manager
    public static final String BAD_XID_FILE = "XID文件已损坏";

    // Messages in Version Manager
    public static final String DEAD_LOCK = "死锁";
    public static final String CONCURRENT_UPDATE_ISSUE = "并发更新冲突";
    public static final String NULL_ENTRY = "文件体为空";

    // Messages in Table Manager
    public static final String INVALID_FIELD_TYPE = "非法的字段类型";
    public static final String FIELD_NOT_FOUND = "未找到该字段";
    public static final String FIELD_NOT_INDEXED = "该字段没有索引";
    public static final String INVALID_LOGIC_OPERATION = "非法逻辑操作";
    public static final String INVALID_VALUES = "非法数据";
    public static final String DUPLICATED_TABLE = "该表已被遗弃";
    public static final String TABLE_NOT_FOUND = "未找到该表";

    // Messages in Parser
    public static final String INVALID_COMMAND = "非法命令";
    public static final String TABLE_NO_INDEX = "表内无索引";

    // Messages in Transport
    public static final String INVALID_PACKAGE_DATA = "非法包数据";

    // Messages in Server
    public static final String NESTED_TRANSACTION_NOT_SUPPORT = "不支持嵌套事务";
    public static final String OUT_OF_TRANSACTION = "不在事务内";

    // Messages in Launcher
    public static final String INVALID_MEMORY = "非法内存";
}
