package org.minidb.backend.tm;

import org.minidb.backend.utils.Panic;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.TransactionConstant;
import org.minidb.common.exception.FileCannotRWException;
import org.minidb.common.exception.FileExistsException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public interface TransactionManager {
    //开启、提交、取消及关闭一个事务
    long beginTransaction();
    void commitTransaction(long xid);
    void abortTransaction(long xid);
    void closeTransaction();
    //事务的三种状态判断
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);

    /**
     * 在XID文件不存在时，创建XID文件和Transaction Manager
     * @param path
     * @return
     */
    static TransactionManagerImpl createTransactionManager(String path){
        File xidFile = new File(path+ TransactionConstant.XID_SUFFIX);

        // 检查文件是否已存在及读写功能正常与否
        try {
            if (!xidFile.createNewFile()) {
                throw new FileExistsException(MessageConstant.FILE_EXIST);
            }
        }catch (IOException e){
            Panic.panic(e);
            //throw new FileIOException(MessageConstant.FILE_IO_EXCEPTION);
        }

        if (!xidFile.canWrite() || !xidFile.canRead()) {
            throw new FileCannotRWException(MessageConstant.FILE_CANNOT_RW);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(xidFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        // 写空XID文件头
        ByteBuffer buffer = ByteBuffer.wrap(new byte[TransactionConstant.XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
            //throw new FileIOException(MessageConstant.FILE_IO_EXCEPTION);
        }

        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }

    /**
     * 在XID文件存在时，打开该文件并创建Transaction Manager
     * @param path
     * @return
     */
    static TransactionManagerImpl openTransactionManager(String path){
        File xidFile = new File(path+TransactionConstant.XID_SUFFIX);
        if(!xidFile.exists()) {
            throw new FileCannotRWException(MessageConstant.FILE_NOT_EXIST);
        }
        if(!xidFile.canRead() || !xidFile.canWrite()) {
            throw new FileCannotRWException(MessageConstant.FILE_CANNOT_RW);
        }

        FileChannel fileChannel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(xidFile, "rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(randomAccessFile, fileChannel);
    }
}
