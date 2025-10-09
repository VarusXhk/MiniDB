package org.minidb.common.constant;

public class TransactionConstant {
    // XID文件头长度
    public static final int XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    public static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    public static final byte TRANSACTION_ACTIVE   = 0;
    public static final byte TRANSACTION_COMMITTED = 1;
    public static final byte TRANSACTION_ABORTED = 2;
    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;
    // XID文件的后缀名
    public static final String XID_SUFFIX = ".xid";
}
