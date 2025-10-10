package org.minidb.backend.dm.page;

import org.minidb.backend.utils.RandomUtil;
import org.minidb.common.constant.PageConstant;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageFirst {

    /**
     * 初始化一个页面大小的字节数组
     * @return
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageConstant.PAGE_SIZE];
        setValidCheckStart(raw);
        return raw;
    }

    /**
     *在db文件启动时，为页面特定位置设置合法检查
     * @param page
     */
    public static void setValidCheckStart(Page page) {
        page.setPageDirty(true);
        setValidCheckStart(page.getPageData());
    }

    /**
     * 在db文件启动时，为页面特定位置设置合法检查
     * @param raw
     */
    private static void setValidCheckStart(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(PageConstant.LENGTH_VALID_CHECK), 0,
                raw, PageConstant.VALID_CHECK_OFFSET, PageConstant.LENGTH_VALID_CHECK);
    }

    /**
     * 在db文件关闭时，为页面特定位置设置合法检查
     * @param page
     */
    public static void setValidCheckClose(Page page) {
        page.setPageDirty(true);
        setValidCheckClose(page.getPageData());
    }

    /**
     * 在db文件关闭时，为页面特定位置设置合法检查
     * @param raw
     */
    private static void setValidCheckClose(byte[] raw) {
        System.arraycopy(raw, PageConstant.VALID_CHECK_OFFSET, raw,
                PageConstant.VALID_CHECK_OFFSET+PageConstant.LENGTH_VALID_CHECK, PageConstant.LENGTH_VALID_CHECK);
    }

    /**
     * 判断上一次数据库是否正常关闭
     * @param page
     * @return
     */
    public static boolean ValidCheck(Page page) {
        return ValidCheck(page.getPageData());
    }

    /**
     * 判断上一次数据库是否正常关闭
     * @param raw
     * @return
     */
    private static boolean ValidCheck(byte[] raw) {
        byte[] openArray = Arrays.copyOfRange(raw, PageConstant.VALID_CHECK_OFFSET,
                PageConstant.VALID_CHECK_OFFSET + PageConstant.LENGTH_VALID_CHECK);
        byte[] closeArray = Arrays.copyOfRange(raw, PageConstant.VALID_CHECK_OFFSET+PageConstant.LENGTH_VALID_CHECK,
                PageConstant.VALID_CHECK_OFFSET + 2 * PageConstant.LENGTH_VALID_CHECK);
        return Arrays.equals(openArray, closeArray);
    }
}
