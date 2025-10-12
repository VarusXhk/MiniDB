package org.minidb.backend.dm.dataItem;

import org.minidb.backend.common.SubArray;
import org.minidb.backend.dm.DataManagerImpl;
import org.minidb.backend.dm.page.Page;
import org.minidb.common.constant.DataItemConstant;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{

    private SubArray raw;
    private byte[] oldRaw;
    // 读锁
    private Lock rLock;
    // 写锁
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.page = page;
    }

    public boolean isValid() {
        return raw.data[raw.start + DataItemConstant.VALID_OFFSET] == (byte)0;
    }

    /**
     * 获取Data Item的数据
     * @return
     */
    @Override
    public SubArray getData() {
        return new SubArray(raw.start + DataItemConstant.DATA_OFFSET, raw.end, raw.data);
    }

    /**
     * 对Data Item进行写操作时需要做的前置工作
     */
    @Override
    public void writePrepare() {
        wLock.lock();
        page.setPageDirty(true);
        System.arraycopy(raw.data, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 对Data Item进行撤销写操作时需要做的前置工作
     */
    @Override
    public void undoPrepare() {
        System.arraycopy(oldRaw, 0, raw.data, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 对Data Item进行写操作后需要做的工作，保证整体操作的原子性
     * @param xid
     */
    @Override
    public void writeAfter(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page getPage() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 获取包含Data Item所有内容的字节数组
     * @return
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }
}
