package org.minidb.backend.utils;

import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.exception.FileCannotRWException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class FileIOUtil {

    /**
     * 检查文件读写功能是否正常，并通过NIO操作文件
     * @param file
     * @return
     */
    public static FileResults fileHandle(File file) {
        // 检查文件读写功能是否正常
        if(!file.canRead() || !file.canWrite()) {
            throw new FileCannotRWException(MessageConstant.FILE_CANNOT_RW);
        }
        //通过NIO操作文件
        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new FileResults(fileChannel, randomAccessFile);
    }
}
