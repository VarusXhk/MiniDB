package org.minidb.backend.dm;

import org.minidb.backend.common.SubArray;
import org.minidb.backend.dm.dataItem.DataItem;
import org.minidb.backend.dm.logger.Logger;
import org.minidb.backend.dm.page.Page;
import org.minidb.backend.dm.page.PageOthers;
import org.minidb.backend.dm.pageCache.PageCache;
import org.minidb.backend.tm.TransactionManager;
import org.minidb.backend.utils.ArrayUtil;
import org.minidb.backend.utils.Panic;
import org.minidb.backend.utils.Parser;
import org.minidb.common.constant.LogConstant;

import java.util.*;
import java.util.Map.Entry;

// import com.google.common.primitives.Bytes;

/**
 * 规定 1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
 * 规定 2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
 */
public class Recover {
    /**
     * 事务的插入日志：在xid文件的第pageNumber页面的offset偏移处插入数据raw
     */
    static class InsertLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] raw;
    }

    /**
     * 事务的更新日志：在xid文件的第pageNumber页面的offset偏移处将old raw 更新为 new raw
     */
    static class UpdateLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 根据日志记录判断数据恢复类型并恢复崩溃的数据
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    public static void recover(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        System.out.println("正在根据日志记录恢复崩溃的数据...");

        logger.rewind();
        int maxPageNumber = 0;
        while(true) {
            byte[] log = logger.getNextLogData();
            if(log == null) break;
            int pageNumber;
            if(isInsertLog(log)) {
                pageNumber = parseInsertLog(log).pageNumber;
            } else {
                pageNumber = parseUpdateLog(log).pageNumber;
            }
            if(pageNumber > maxPageNumber) {
                maxPageNumber = pageNumber;
            }
        }
        if(maxPageNumber == 0) {
            maxPageNumber = 1;
        }
        pageCache.truncateByPgNumber(maxPageNumber);
        System.out.println("将页面缓存截断至" + maxPageNumber + "页");

        redoTransactions(transactionManager, logger, pageCache);
        System.out.println("已重做所有正常事务");

        undoTransactions(transactionManager, logger, pageCache);
        System.out.println("已撤销所有不正常事务");

        System.out.println("数据恢复已完成！");
    }

    /**
     * 重做所有正常事务
     * @param transactionManager
     * @param logger
     * @param pageCache
     *
     */
    private static void redoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        logger.rewind();
        while(true) {
            byte[] log = logger.getNextLogData();
            if(log == null) break;
            // 当事务的状态是已完成，则重做
            if(isInsertLog(log)) {
                long xid = parseInsertLog(log).xid;
                if(!transactionManager.isActive(xid)) {
                    doInsertLog(pageCache, log, LogConstant.REDO);
                }
            } else {
                long xid = parseUpdateLog(log).xid;
                if(!transactionManager.isActive(xid)) {
                    doUpdateLog(pageCache, log, LogConstant.REDO);
                }
            }
        }
    }

    /**
     * 撤销所有不正常事务
     * @param transactionManager
     *
     * @param logger
     * @param pageCache
     */
    private static void undoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        // 获取所有状态为active的日志
        while(true) {
            byte[] log = logger.getNextLogData();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(transactionManager.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(transactionManager.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pageCache, log, LogConstant.UNDO);
                } else {
                    doUpdateLog(pageCache, log, LogConstant.UNDO);
                }
            }
            transactionManager.abortTransaction(entry.getKey());
        }
    }

    /**
     * 判断是否为插入类型日志
     * @param log
     * @return
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LogConstant.LOG_TYPE_INSERT;
    }

    /**
     * 生成一条更新类型的日志
     * @param xid
     * @param dataItem
     * @return
     */
    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LogConstant.LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw1 = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw1.data, raw1.start, raw1.end);
        return ArrayUtil.concatArray(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 根据单条更新类日志生成其日志更新信息
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, LogConstant.XID_OFFSET, LogConstant.UPDATE_UID_OFFSET));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, LogConstant.UPDATE_UID_OFFSET, LogConstant.UPDATE_RAW_OFFSET));
        updateLogInfo.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        updateLogInfo.pageNumber = (int)(uid & ((1L << 32) - 1));
        // 单个数据（旧数据、新数据）的长度
        int length = (log.length - LogConstant.UPDATE_RAW_OFFSET) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, LogConstant.UPDATE_RAW_OFFSET, LogConstant.UPDATE_RAW_OFFSET + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, LogConstant.UPDATE_RAW_OFFSET + length,
                LogConstant.UPDATE_RAW_OFFSET + length * 2);
        return updateLogInfo;
    }

    /**
     * 执行更新日志的更新内容
     * @param pageCache
     * @param log
     * @param flag
     */
    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        int pageNumber;
        short offset;
        byte[] raw;

        UpdateLogInfo updateLogInfo = parseUpdateLog(log);
        pageNumber = updateLogInfo.pageNumber;
        offset = updateLogInfo.offset;
        raw = (flag == LogConstant.REDO) ? updateLogInfo.newRaw : updateLogInfo.oldRaw;

        Page pg = null;
        try {
            pg = pageCache.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageOthers.recoverUpdate(pg, raw, offset);
        } finally {
            pg.releasePage();
        }
    }

    /**
     * 生成一条插入类型的日志
     * @param xid
     * @param page
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LogConstant.LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNumberRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageOthers.getFreeSpaceOffset(page));
        return ArrayUtil.concatArray(logTypeRaw, xidRaw, pageNumberRaw, offsetRaw, raw);
    }

    /**
     * 根据单条插入类日志生成其日志插入信息
     * @param log
     * @return
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, LogConstant.XID_OFFSET, LogConstant.INSERT_PAGENUM_OFFSET));
        insertLogInfo.pageNumber = Parser.parseInt(Arrays.copyOfRange(log, LogConstant.INSERT_PAGENUM_OFFSET, LogConstant.INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, LogConstant.INSERT_OFFSET, LogConstant.INSERT_RAW_OFFSET));
        insertLogInfo.raw = Arrays.copyOfRange(log, LogConstant.INSERT_RAW_OFFSET, log.length);
        return insertLogInfo;
    }

    /**
     * 执行插入日志的插入内容
     * @param pageCache
     * @param log
     * @param flag
     */
    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pageCache.getPage(insertLogInfo.pageNumber);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == LogConstant.UNDO) {
                DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
            PageOthers.recoverInsert(pg, insertLogInfo.raw, insertLogInfo.offset);
        } finally {
            pg.releasePage();
        }
    }
}
