package org.minidb.backend.dm.dataItem;

import org.minidb.backend.common.SubArray;
import org.minidb.backend.dm.DataManagerImpl;
import org.minidb.backend.dm.page.Page;
import org.minidb.backend.utils.ArrayUtil;
import org.minidb.backend.utils.Parser;
import org.minidb.backend.utils.Types;
import org.minidb.common.constant.DataItemConstant;

// import com.google.common.primitives.Bytes;

import java.util.Arrays;

public interface DataItem {
    SubArray getData();

    void writePrepare();
    void undoPrepare();
    void writeAfter(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page getPage();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    /**
     * 根据字节数组生成一个Data Item
     * @param raw
     * @return
     */
    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return ArrayUtil.concatArray(valid, size, raw);
    }

    /**
     * 从页面的offset处解析出Data Item
     * @param pg
     * @param offset
     * @param dm
     * @return
     */
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getPageData();
        // Data Item的数据的大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemConstant.SIZE_OFFSET,
                offset + DataItemConstant.DATA_OFFSET));
        // Data Item的大小
        short length = (short)(size + DataItemConstant.DATA_OFFSET);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(offset, offset + length, raw), new byte[length], pg, uid, dm);
    }

    /**
     * 设置Data Item的有效位为无效
     * @param raw
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemConstant.VALID_OFFSET] = (byte)1;
    }
}
