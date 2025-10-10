package org.minidb.common.constant;

public class LogConstant {
    public static final int SEED = 13331;
    public static final int OFFSET_SIZE = 0;
    public static final int SIZE_OFFSET = 4;
    public static final int CHECKSUM_OFFSET = OFFSET_SIZE + 4;
    public static final int DATA_OFFSET = CHECKSUM_OFFSET + 4;
    public static final String LOG_SUFFIX = ".log";
}
