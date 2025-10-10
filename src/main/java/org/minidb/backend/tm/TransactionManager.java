package org.minidb.backend.tm;

import org.minidb.backend.utils.FileIOUtil;
import org.minidb.backend.utils.Panic;
import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.TransactionConstant;
import org.minidb.common.exception.FileExistsException;
import org.minidb.common.exception.FileNotExistException;

import java.io.File;
import java.io.IOException;


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
        // 检查文件是否已存在
        try {
            if (!xidFile.createNewFile()) {
                throw new FileExistsException(MessageConstant.FILE_EXIST);
            }
        }catch (IOException e){
            Panic.panic(e);
        }

        FileResults fileResults = FileIOUtil.fileHandle(xidFile);
        // 写空XID文件头
        fileResults.WriteHeader(TransactionConstant.XID_HEADER_LENGTH);

        return new TransactionManagerImpl(fileResults);
    }

    /**
     * 在XID文件存在时，打开该文件并创建Transaction Manager
     * @param path
     * @return
     */
    static TransactionManagerImpl openTransactionManager(String path){
        File xidFile = new File(path+TransactionConstant.XID_SUFFIX);
        if(!xidFile.exists()) {
            throw new FileNotExistException(MessageConstant.FILE_NOT_EXIST);
        }
        return new TransactionManagerImpl(FileIOUtil.fileHandle(xidFile));
    }
}
