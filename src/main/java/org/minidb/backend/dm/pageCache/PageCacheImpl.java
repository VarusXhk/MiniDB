package org.minidb.backend.dm.pageCache;

import org.minidb.backend.common.AbstractCache;
import org.minidb.backend.dm.page.Page;
import org.minidb.backend.dm.page.PageImpl;
import org.minidb.backend.utils.Panic;
import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.PageConstant;
import org.minidb.common.exception.MemoryShortageException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    PageCacheImpl(FileResults fileResults, int maxResource) {
        // 调用父类AbstractCache的构造方法
        super(maxResource);
        if(maxResource < PageConstant.MEMORY_MIN_LIMIT) {
            throw new MemoryShortageException(MessageConstant.MEMORY_SHORTAGE);
        }
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = fileResults.getRandomAccessFile();
        this.fc = fileResults.getFileChannel();
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PageConstant.PAGE_SIZE);
    }

    /**
     * 根据页面编号从数据库文件中读取页数据，并包裹成Page
     * 并不会写入缓存
     */
    @Override
    protected Page get2Cache(long key) throws Exception {
        int pageNumber = (int)key;
        long offset = pageOffset(pageNumber);
        fileLock.lock();
        ByteBuffer buffer = ByteBuffer.allocate(PageConstant.PAGE_SIZE);
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
        return new PageImpl(pageNumber, buffer.array(), this);
    }

    /**
     * 若页面被标记为脏页，则写回文件系统
     * @param page
     */
    @Override
    protected void releaseByObj(Page page) {
        if(page.isDirty()) {
            flush(page);
            page.setPageDirty(false);
        }
    }

    /**
     * 创建一个页面并写回文件系统
     * @param initData
     * @return
     */
    public int newPage(byte[] initData) {
        int pageNumber = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNumber, initData, null);
        flush(page);
        return pageNumber;
    }

    /**
     * 根据页面编号从缓存中获取页面
     * @param pageNumber
     * @return
     * @throws Exception
     */
    public Page getPage(int pageNumber) throws Exception {
        return getFromCache((long)pageNumber);
    }

    /**
     * 减少对页面的一个引用
     * @param page
     */
    public void release(Page page) {
        releaseReferenceByKey((long)page.getPageNumber());
    }

    /**
     * 关闭缓存
     * @param page
     */
    @Override
    public void closeCache(Page page) {
        super.closeCache();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将文件写回文件系统
     * @param page
     */
    public void flushPage(Page page) {
        flush(page);
    }

    private void flush(Page page) {
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);

        fileLock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
            fc.position(offset);
            fc.write(buffer);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 根据给定的最大页面编号对文件进行裁剪
     * @param maxPageNumber
     */
    public void truncateByPgNumber(int maxPageNumber) {
        long size = pageOffset(maxPageNumber + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNumber);
    }

    /**
     * 计算页面编号
     */
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 通过页面编号计算页面偏移量
     * @param pageNumber
     */
    private static long pageOffset(int pageNumber) {
        return (long) (pageNumber - 1) * PageConstant.PAGE_SIZE;
    }
}
