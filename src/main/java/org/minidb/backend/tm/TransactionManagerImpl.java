package org.minidb.backend.tm;

import org.minidb.backend.utils.Panic;
import org.minidb.common.Result.FileResults;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.TMConstant;
import org.minidb.backend.utils.Parser;
import org.minidb.common.exception.BadXidFileException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private long XIDCounter;
    private Lock counterLock;


    TransactionManagerImpl(FileResults fileResults) {
        this.fileChannel = fileResults.getFileChannel();
        this.randomAccessFile = fileResults.getRandomAccessFile();
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter() {
        long fileLength = 0;
        try {
            fileLength = randomAccessFile.length();
        } catch (IOException e1) {
            //Panic.panic(Error.BadXIDFileException);
            throw new BadXidFileException(MessageConstant.BAD_XID_FILE);
        }

        if(fileLength < TMConstant.XID_HEADER_LENGTH) {
            throw new BadXidFileException(MessageConstant.BAD_XID_FILE);
        }

        ByteBuffer buffer = ByteBuffer.allocate(TMConstant.XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.XIDCounter = Parser.parseLong(buffer.array());
        long end = getXIDPosition(this.XIDCounter + 1);
        if(end != fileLength) {
            throw new BadXidFileException(MessageConstant.BAD_XID_FILE);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXIDPosition(long xid) {
        return TMConstant.XID_HEADER_LENGTH + (xid-1) * TMConstant.XID_FIELD_SIZE;
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        long offset = getXIDPosition(xid);
        byte[] tmp = new byte[TMConstant.XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tmp);
        try {
            fileChannel.position(offset);
            fileChannel.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXIDPosition(xid);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[TMConstant.XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buffer.array()[0] == status;
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        XIDCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(XIDCounter));
        try {
            fileChannel.position(0);
            fileChannel.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开始一个事务，并返回XID
    public long beginTransaction() {
        counterLock.lock();
        try {
            long xid = XIDCounter + 1;
            updateXID(xid, TMConstant.TRANSACTION_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commitTransaction(long xid) {
        updateXID(xid, TMConstant.TRANSACTION_COMMITTED);
    }

    // 回滚XID事务
    public void abortTransaction(long xid) {
        updateXID(xid, TMConstant.TRANSACTION_ABORTED);
    }

    public boolean isActive(long xid) {
        if(xid == TMConstant.SUPER_XID) return false;
        return checkXID(xid, TMConstant.TRANSACTION_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == TMConstant.SUPER_XID) return true;
        return checkXID(xid, TMConstant.TRANSACTION_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == TMConstant.SUPER_XID) return false;
        return checkXID(xid, TMConstant.TRANSACTION_ABORTED);
    }

    public void closeTransaction() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
