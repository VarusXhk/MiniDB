package org.minidb.backend.vm;

//import com.google.common.primitives.Bytes;
import org.minidb.backend.common.SubArray;
import org.minidb.backend.dm.dataItem.DataItem;
import org.minidb.backend.utils.Parser;
import org.minidb.common.constant.VMConstant;
import org.minidb.backend.utils.ArrayUtil;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [Create VTN] [Delete VTN] [data]
 * The Number of Transaction of (creating / deleting) this Version
 */
public class Entry {

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    /**
     * 创建新的entry并初始化
     * @param vm
     * @param dataItem
     * @param uid
     * @return
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        return Entry.builder()
                .uid(uid)
                .dataItem(dataItem)
                .vm(vm)
                .build();
    }

    /**
     * 读取并返回一个Entry
     * @param vm
     * @param uid
     * @return
     * @throws Exception
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 往Entry内写入封装数据
     * @param xid
     * @param data
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] createVTN = Parser.long2Byte(xid);
        byte[] deleteVTN = new byte[8];
        return ArrayUtil.concatArray(createVTN, deleteVTN, data);
    }

    /**
     * 释放Entry资源
     */
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * 释放Data Item资源
     */
    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回Entry的内容(除去Create,DeleteVTN)
     * @return
     */
    public byte[] getEntryData() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.getData();
            byte[] data = new byte[subArray.end - subArray.start - VMConstant.DATA_OFFSET];
            System.arraycopy(subArray.data, subArray.start + VMConstant.DATA_OFFSET, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取创建该版本的事务的事务编号(xid)
     * @return
     */
    public long getCreateVTN() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.getData();
            return Parser.parseLong(Arrays.copyOfRange(subArray.data, subArray.start + VMConstant.CREATE_VERSION_OFFSET,
                    subArray.start + VMConstant.DELETE_VERSION_OFFSET));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取删除该版本的事务的事务编号(xid)
     * @return
     */
    public long getDeleteVTN() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.getData();
            return Parser.parseLong(Arrays.copyOfRange(subArray.data, subArray.start + VMConstant.DELETE_VERSION_OFFSET,
                    subArray.start + VMConstant.DATA_OFFSET));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置删除该版本的事务的事务编号(xid)
     * @param xid
     */
    public void setDeleteVTN(long xid) {
        dataItem.writePrepare();
        try {
            SubArray subArray = dataItem.getData();
            System.arraycopy(Parser.long2Byte(xid), 0, subArray.data, subArray.start + VMConstant.DELETE_VERSION_OFFSET, 8);
        } finally {
            dataItem.writeAfter(xid);
        }
    }

    /**
     * 获取entry的uid
     * @return
     */
    public long getUid() {
        return this.uid;
    }

    /**
     * 创建entry的builder模式
     * @return
     */
    public static Builder builder(){
        return new Builder();
    }

    public static class Builder{
        private long uid;
        private DataItem dataItem;
        private VersionManager vm;

        public Builder uid(long uid) {
            this.uid = uid;
            return this;
        }
        public Builder dataItem(DataItem dataItem) {
            this.dataItem = dataItem;
            return this;
        }
        public Builder vm(VersionManager vm) {
            this.vm = vm;
            return this;
        }
        public Entry build() {
            return new Entry(this);
        }
    }

    public Entry(Builder builder) {
        this.uid = builder.uid;
        this.dataItem = builder.dataItem;
        this.vm = builder.vm;
    }
}
