package org.minidb.common.constant;

public class PageConstant {
    public static final int PAGE_SIZE = 1 << 13;
    public static final int MEMORY_MIN_LIMIT = 10;
    public static final String DB_SUFFIX = ".db";
    public static final int VALID_CHECK_OFFSET = 100;
    public static final int LENGTH_VALID_CHECK = 8;
    public static final short FREE_OFFSET = 0;
    public static final short DATA_OFFSET = 2;
    public static final int MAX_FREE_SPACE = PAGE_SIZE - DATA_OFFSET;
    // 将一页划成40个区间
    public static final int INTERVALS_NUMBER = 40;
    public static final int INTERVAL_SIZE = PAGE_SIZE / INTERVALS_NUMBER;
}
