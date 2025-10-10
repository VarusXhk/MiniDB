package org.minidb.backend.utils;

import java.nio.ByteBuffer;

public class ArrayUtil {
    /**
     * 将若干个字节数组拼接成新的字节数组
     * @param arrays
     * @return
     */
    public static byte[] concatArray(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        for (byte[] array : arrays) {
            buffer.put(array);
        }

        return buffer.array();
    }
}
