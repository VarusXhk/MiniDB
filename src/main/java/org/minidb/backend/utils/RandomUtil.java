package org.minidb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    /**
     * 生成一个指定长度的随机字节数组
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buffer = new byte[length];
        r.nextBytes(buffer);
        return buffer;
    }
}
