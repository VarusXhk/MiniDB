package org.minidb.backend.utils;

public class Parser {
    public static long parseLong(byte[] array) {
        return Long.parseLong(new String(array));
    }

    public static byte[] long2Byte(long xidCounter) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((xidCounter >>> 56) & 0xFF);
        bytes[1] = (byte) ((xidCounter >>> 48) & 0xFF);
        bytes[2] = (byte) ((xidCounter >>> 40) & 0xFF);
        bytes[3] = (byte) ((xidCounter >>> 32) & 0xFF);
        bytes[4] = (byte) ((xidCounter >>> 24) & 0xFF);
        return null;
    }
}
