package org.minidb.backend.dm.page;

import org.minidb.common.constant.PageConstant;
import org.minidb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * 一个普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移。剩下的部分都是实际存储的数据
 * 结构：[FreeSpaceOffset] [Data]
 */
public class PageOthers {
    /**
     * 初始化字节数组
     * @return
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageConstant.PAGE_SIZE];
        setFreeSpaceOffset(raw, PageConstant.DATA_OFFSET);
        return raw;
    }

    /**
     * 为字节数组设置空闲空间偏移
     * @param raw
     * @param offsetData
     */
    private static void setFreeSpaceOffset(byte[] raw, short offsetData) {
        System.arraycopy(Parser.short2Byte(offsetData), 0,
                raw, PageConstant.FREE_OFFSET, PageConstant.DATA_OFFSET);
    }

    /**
     * 获取页面的空闲空间偏移
     * @param page
     * @return
     */
    public static short getFreeSpaceOffset(Page page) {
        return getFreeSpaceOffset(page.getPageData());
    }

    /**
     * 获取字节数组的空闲空间偏移
     * @param raw
     * @return
     */
    private static short getFreeSpaceOffset(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将包含数据的字节数组插入页面中
     * @param page
     * @param raw
     * @return
     */
    public static short insert(Page page, byte[] raw) {
        page.setPageDirty(true);
        short offset = getFreeSpaceOffset(page.getPageData());
        System.arraycopy(raw, 0, page.getPageData(), offset, raw.length);
        setFreeSpaceOffset(page.getPageData(), (short)(offset + raw.length));
        return offset;
    }

    /**
     * 获取页面的空闲空间大小
     * @param page
     * @return
     */
    public static int getFreeSpace(Page page) {
        return PageConstant.PAGE_SIZE - (int) getFreeSpaceOffset(page.getPageData());
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程直接插入数据
     * 将raw插入页面中的offset位置，并将页面的offset设置为较大的offset
     * @param page
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setPageDirty(true);
        System.arraycopy(raw, 0, page.getPageData(), offset, raw.length);

        short rawFSO = getFreeSpaceOffset(page.getPageData());
        //如果数据库崩溃前已经写入一些数据，在恢复时再将全部数据写入会导致如下情况
        if(rawFSO < offset + raw.length) {
            setFreeSpaceOffset(page.getPageData(), (short)(offset+raw.length));
        }
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程修改数据
     * 将raw插入页面中的offset位置
     * @param page
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setPageDirty(true);
        System.arraycopy(raw, 0, page.getPageData(), offset, raw.length);
    }
}
