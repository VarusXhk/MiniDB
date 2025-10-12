package org.minidb.backend.dm;

import org.minidb.backend.common.AbstractCache;
import org.minidb.backend.dm.dataItem.DataItem;
import org.minidb.backend.dm.dataItem.DataItemImpl;
import org.minidb.backend.dm.logger.Logger;
import org.minidb.backend.dm.page.Page;
import org.minidb.backend.dm.page.PageFirst;
import org.minidb.backend.dm.page.PageOthers;
import org.minidb.backend.dm.pageIndex.PageIndex;
import org.minidb.backend.dm.pageIndex.PageInfo;
import org.minidb.backend.dm.pageCache.PageCache;
import org.minidb.backend.tm.TransactionManager;
import org.minidb.backend.utils.Panic;
import org.minidb.backend.utils.Types;
import org.minidb.common.constant.DataItemConstant;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.PageConstant;
import org.minidb.common.exception.DataOverflowException;
import org.minidb.common.exception.DatabaseBusyException;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageFirst;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager tm) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    /**
     * 根据给定的唯一标识符 (uid) 获取数据项。
     * 计算页面编号和偏移量，从 pageCache 获取对应页面，并使用 DataItem.parseDataItem(pg, offset, this) 解析数据项
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem get2Cache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNumber = (int)(uid & ((1L << 32) - 1));
        Page pg = pageCache.getPage(pageNumber);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * 释放给定数据项所占用的页面资源
     * @param di
     */
    @Override
    protected void releaseByObj(DataItem di) {
        di.getPage().releasePage();
    }

    /**
     * 从缓存中读取数据项。如果数据项无效，则释放它并返回 null
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.getFromCache(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     * 插入新数据项。
     * 检查数据长度是否超过最大限制，若超过则抛出 DataOverflowException。
     * 尝试找到合适的页面索引存储数据，若没有找到则创建新页面。
     * 将数据插入页面，并记录相关日志。
     * 返回新数据项的唯一标识符
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageConstant.MAX_FREE_SPACE) {
            throw new DataOverflowException(MessageConstant.DATA_OVERFLOW);
        }

        PageInfo pi = null;
        for(int i = 0; i < DataItemConstant.MAX_INSERT_ATTEMPTS; i ++) {
            pi = pageIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pageCache.newPage(PageOthers.initRaw());
                pageIndex.add(newPgno, PageConstant.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw new DatabaseBusyException(MessageConstant.DATABASE_BUSY);
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pageCache.getPage(pi.pageNumber);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageOthers.insert(pg, raw);

            pg.releasePage();
            return Types.addressToUid(pi.pageNumber, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pageIndex.add(pi.pageNumber, PageOthers.getFreeSpace(pg));
            } else {
                pageIndex.add(pi.pageNumber, freeSpace);
            }
        }
    }

    /**
     * 关闭缓存、记录器和第一页的资源
     */
    @Override
    public void close() {
        super.closeCache();
        logger.close();

        PageFirst.setValidCheckClose(pageFirst);
        pageFirst.releasePage();
        pageCache.closeCache(pageFirst);
    }

    /**
     * 记录数据项的更新日志
     * @param xid
     * @param di
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    /**
     * 释放对给定数据项的引用
     * @param di
     */
    public void releaseDataItem(DataItem di) {
        super.releaseReferenceByKey(di.getUid());
    }


    /**
     * 初始化第一页，创建并缓存它，并刷新到持久存储
     */
    void initPageOne() {
        int pageNumber = pageCache.newPage(PageFirst.InitRaw());
        assert pageNumber == 1;
        try {
            pageFirst = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageFirst);
    }

    /**
     * 在打开已有文件时,读取并检查第一页的有效性，返回检查结果
     * @return
     */
    boolean loadCheckPageOne() {
        try {
            pageFirst = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageFirst.ValidCheck(pageFirst);
    }

    /**
     * 初始化pageIndex，遍历缓存的页面并记录每个页面的空闲空间
     */
    void fillPageIndex() {
        int pageNumber = pageCache.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(i, PageOthers.getFreeSpace(page));
            page.releasePage();
        }
    }
}
