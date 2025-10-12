package org.minidb.backend.dm.logger;

import org.minidb.backend.utils.FileIOUtil;
import org.minidb.backend.utils.Panic;
import org.minidb.backend.utils.Parser;
import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.LogConstant;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.exception.FileExistsException;
import org.minidb.common.exception.FileNotExistException;

import java.io.File;

public interface Logger {
    /**
     * 将一条新的日志加载至日志文件内
     * @param data
     */
    void log(byte[] data);
    /**
     * 将日志文件截断至size大小
     * @param size
     * @throws Exception
     */
    void truncate(long size) throws Exception;
    /**
     * 读取日志拼文件，将一条日志的Data部分解析出来
     * @return
     */
    byte[] getNextLogData();
    /**
     * 将日志指针倒回至第一个日志的起始处（总校验和的结尾）
     */
    void rewind();
    /**
     * 关闭整个日志文件
     */
    void close();

    static Logger create(String path) {
        File logFile = new File(path+ LogConstant.LOG_SUFFIX);
        try {
            if(!logFile.createNewFile()) {
                throw new FileExistsException(MessageConstant.FILE_EXIST);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        FileResults fileResults = FileIOUtil.fileHandle(logFile);
        fileResults.WriteHeader(Parser.int2Byte(0));

        return new LoggerImpl(fileResults, 0);
    }

    static Logger open(String path) {
        File logFile = new File(path+LogConstant.LOG_SUFFIX);
        if(!logFile.exists()) {
            throw new FileNotExistException(MessageConstant.FILE_NOT_EXIST);
        }
        FileResults fileResults = FileIOUtil.fileHandle(logFile);

        LoggerImpl logger = new LoggerImpl(fileResults);
        logger.init();

        return logger;
    }
}
