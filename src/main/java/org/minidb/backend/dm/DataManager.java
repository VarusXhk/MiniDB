package org.minidb.backend.dm;

import org.minidb.backend.dm.dataItem.DataItem;
import org.minidb.backend.dm.logger.Logger;
import org.minidb.backend.dm.page.PageFirst;
import org.minidb.backend.dm.pageCache.PageCache;
import org.minidb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 在页面缓存和日志文件不存在时，创建页面缓存和日志文件、创建Data Manager
     * @param path
     * @param memory
     * @param tm
     * @return
     */
    static DataManager createDataManager(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCache.createPageCache(path, memory);
        Logger logger = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pageCache, logger, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * 在页面缓存和日志文件存在时，打开页面缓存和日志文件、创建Data Manager
     * @param path
     * @param memory
     * @param tm
     * @return
     */
    static DataManager openDataManager(String path, long memory, TransactionManager tm) {
        PageCache pageCache = PageCache.openPageCache(path, memory);
        Logger logger = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pageCache, logger, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, logger, pageCache);
        }
        dm.fillPageIndex();
        PageFirst.setValidCheckStart(dm.pageFirst);
        dm.pageCache.flushPage(dm.pageFirst);

        return dm;
    }
}
