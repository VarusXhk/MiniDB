package org.minidb.backend.dm.pageCache;

import org.minidb.backend.dm.page.Page;
import org.minidb.backend.utils.FileIOUtil;
import org.minidb.backend.utils.Panic;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.PageConstant;
import org.minidb.common.exception.FileExistsException;
import org.minidb.common.exception.FileNotExistException;

import java.io.File;

public interface PageCache {

    int newPage(byte[] initData);
    Page getPage(int pageNumber) throws Exception;
    void release(Page page);
    void closeCache(Page page);

    void truncateByPgNumber(int maxPageNumber);
    int getPageNumber();
    void flushPage(Page page);

    /**
     * 在db数据库不存在时，创建db文件和页面缓存
     * @param path
     * @param memory
     * @return
     */
    static PageCacheImpl createPageCache(String path, long memory) {
        File dbFile = new File(path+ PageConstant.DB_SUFFIX);

        try {
            if(!dbFile.createNewFile()) {
                throw new FileExistsException(MessageConstant.FILE_EXIST);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(FileIOUtil.fileHandle(dbFile), (int)memory/PageConstant.PAGE_SIZE);
    }

    /**
     * 在db数据库存在时，依据db文件创建页面缓存
     * @param path
     * @param memory
     * @return
     */
    static PageCacheImpl openPageCache(String path, long memory) {
        File dbFile = new File(path+PageConstant.DB_SUFFIX);
        if(!dbFile.exists()) {
            throw new FileNotExistException(MessageConstant.FILE_NOT_EXIST);
        }
        return new PageCacheImpl(FileIOUtil.fileHandle(dbFile), (int)memory/PageConstant.PAGE_SIZE);
    }
}
