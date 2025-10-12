package org.minidb.common.constant;

public class VMConstant {
    // 表示创建该版本的事务的编号
    public static final int CREATE_VERSION_OFFSET = 0;
    // 表示删除该版本的事务的编号
    public static final int DELETE_VERSION_OFFSET = CREATE_VERSION_OFFSET + 8;
    public static final int DATA_OFFSET = DELETE_VERSION_OFFSET + 8;
    public static final int READ_UNCOMMITTED = 0;

}
