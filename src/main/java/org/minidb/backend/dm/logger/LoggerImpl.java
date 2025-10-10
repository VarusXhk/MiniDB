package org.minidb.backend.dm.logger;

import org.minidb.backend.utils.ArrayUtil;
import org.minidb.backend.utils.Panic;
import org.minidb.backend.utils.Parser;
import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.LogConstant;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.exception.BadLogFileException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Arrays;

//import com.google.common.primitives.Bytes;

/**
 * 日志的二进制文件的格式为：
 * [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 * XChecksum 是一个四字节的整数，是对后续所有日志计算的校验和
 * BadTail是在数据库崩溃时，没有来得及写完的日志数据
 * 每条日志LogN的格式为：
 * [Size][Checksum][Data]
 * Size 是一个四字节整数，标识了 Data 段的字节数。Checksum一个四字节整数，是该条日志的校验和
 */
public class LoggerImpl implements Logger {
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private Lock lock;
    // 当前日志指针的位置
    private long position;
    // 日志文件的大小
    private long loggerSize;
    // 总校验和
    private int totalCheckSum;

    LoggerImpl(FileResults fileResults) {
        this.randomAccessFile = fileResults.getRandomAccessFile();
        this.fileChannel = fileResults.getFileChannel();
        lock = new ReentrantLock();
    }

    LoggerImpl(FileResults fileResults, int totalCheckSum) {
        this.randomAccessFile = fileResults.getRandomAccessFile();
        this.fileChannel = fileResults.getFileChannel();
        this.totalCheckSum = totalCheckSum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化日志文件
     */
    void init() {
        // 获取日志文件的大小
        long loggerSize = 0;
        try {
            loggerSize = randomAccessFile.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 空日志文件最小为4字节，即只有总校验和
        if(loggerSize < LogConstant.CHECKSUM_OFFSET) {
            throw new BadLogFileException(MessageConstant.BAD_LOG_FILE);
        }

        ByteBuffer raw = ByteBuffer.allocate(LogConstant.CHECKSUM_OFFSET);
        try {
            fileChannel.position(0);
            fileChannel.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int totalCheckSum = Parser.parseInt(raw.array());
        this.loggerSize = loggerSize;
        this.totalCheckSum = totalCheckSum;

        checkAndRemoveTail();
    }

    /**
     * 检查并移除bad tail
     */
    private void checkAndRemoveTail() {
        rewind();

        int totalCheckSum = 0;
        while(true) {
            byte[] log = getNextLog();
            if(log == null) break;
            totalCheckSum = calCheckSum(totalCheckSum, log);
        }
        if(totalCheckSum != this.totalCheckSum) {
            throw new BadLogFileException(MessageConstant.BAD_LOG_FILE);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            randomAccessFile.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 计算总校验和
     * @param currTotalCheckSum
     * @param log
     * @return
     */
    private int calCheckSum(int currTotalCheckSum, byte[] log) {
        for (byte b : log) {
            currTotalCheckSum = currTotalCheckSum * LogConstant.SEED + b;
        }
        return currTotalCheckSum;
    }

    /**
     * 将一条新的日志加载至日志文件内
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fileChannel.position(fileChannel.size());
            fileChannel.write(buffer);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateTotalCheckSum(log);
    }

    /**
     * 更新总校验和
     * @param log
     */
    private void updateTotalCheckSum(byte[] log) {
        this.totalCheckSum = calCheckSum(this.totalCheckSum, log);
        try {
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(Parser.int2Byte(totalCheckSum)));
            fileChannel.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 构造单条日志
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return ArrayUtil.concatArray(size, checkSum, data);
    }

    /**
     * 将日志文件截断至size大小
     * @param size
     * @throws Exception
     */
    @Override
    public void truncate(long size) throws Exception {
        lock.lock();
        try {
            fileChannel.truncate(size);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 读取日志文件，将一条日志解析出来
     * @return
     */
    private byte[] getNextLog() {
        if(position + LogConstant.DATA_OFFSET >= loggerSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(LogConstant.SIZE_OFFSET);
        try {
            fileChannel.position(position);
            fileChannel.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int logSize = Parser.parseInt(tmp.array());
        if(position + LogConstant.DATA_OFFSET + logSize > loggerSize) {
            return null;
        }

        // 一条log大小的buffer
        ByteBuffer buffer = ByteBuffer.allocate(LogConstant.DATA_OFFSET + logSize);
        try {
            fileChannel.position(position);
            fileChannel.read(buffer);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buffer.array();
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, LogConstant.DATA_OFFSET, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, LogConstant.CHECKSUM_OFFSET, LogConstant.DATA_OFFSET));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /**
     * 读取日志拼接成的字节数组，将每一条日志的Data部分解析出来
     * @return
     */
    @Override
    public byte[] getNextlogData() {
        lock.lock();
        try {
            byte[] log = getNextLog();
            if(log == null) return null;
            return Arrays.copyOfRange(log, LogConstant.DATA_OFFSET, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将日志指针倒回至第一个日志的起始处（总校验和的结尾）
     */
    @Override
    public void rewind() {
        position = 4;
    }

    /**
     * 关闭整个日志文件
     */
    @Override
    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
}
