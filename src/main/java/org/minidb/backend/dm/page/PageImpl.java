package org.minidb.backend.dm.page;

import org.minidb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{
    //页面页号
    private int pageNumber;
    //页面实际数据
    private byte[] data;
    //标志脏页
    private boolean dirty;
    //并发锁
    private Lock lock;
    //页面缓存的引用
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void releasePage() {
        pc.release(this);
    }

    public void setPageDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return this.dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getPageData() {
        return data;
    }
}
