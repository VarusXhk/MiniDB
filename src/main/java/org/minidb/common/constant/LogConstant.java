package org.minidb.common.constant;

public class LogConstant {
    public static final int SEED = 13331;
    public static final int OFFSET_SIZE = 0;
    public static final int SIZE_OFFSET = 4;
    public static final int CHECKSUM_OFFSET = OFFSET_SIZE + 4;
    public static final int DATA_OFFSET = CHECKSUM_OFFSET + 4;
    public static final String LOG_SUFFIX = ".log";
    public static final byte LOG_TYPE_INSERT = 0;
    public static final byte LOG_TYPE_UPDATE = 1;
    public static final int REDO = 0;
    public static final int UNDO = 1;
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    public static final int TYPE_OFFSET = 0;
    public static final int XID_OFFSET = TYPE_OFFSET + 1;
    public static final int UPDATE_UID_OFFSET = XID_OFFSET + 8;
    public static final int UPDATE_RAW_OFFSET = UPDATE_UID_OFFSET + 8;
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    public static final int INSERT_PAGENUM_OFFSET = XID_OFFSET + 8;
    public static final int INSERT_OFFSET = INSERT_PAGENUM_OFFSET + 4;
    public static final int INSERT_RAW_OFFSET = INSERT_OFFSET + 2;
}
