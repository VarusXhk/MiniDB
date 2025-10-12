package org.minidb.backend.vm;

import org.minidb.backend.common.AbstractCache;
import org.minidb.backend.dm.DataManager;
import org.minidb.backend.tm.TransactionManager;
import org.minidb.backend.utils.Panic;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.constant.TMConstant;
import org.minidb.common.exception.ConcurrentUpdateException;
import org.minidb.common.exception.NullEntryException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        this.activeTransaction.put(TMConstant.SUPER_XID,
                Transaction.newTransaction(TMConstant.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry get2Cache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw new NullEntryException(MessageConstant.NULL_ENTRY);
        }
        return entry;
    }

    @Override
    protected void releaseByObj(Entry entry) {
        entry.remove();
    }

    /**
     * 读取指定事务（xid）对指定 UID 的数据
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.getFromCache(uid);
        } catch(Exception e) {
            if(e.equals(new NullEntryException(MessageConstant.NULL_ENTRY))) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.getEntryData();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 插入数据
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    /**
     * 删除指定 UID 的数据
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.getFromCache(uid);
        } catch(Exception e) {
            if(e.equals(new NullEntryException(MessageConstant.NULL_ENTRY))) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = new ConcurrentUpdateException(MessageConstant.CONCURRENT_UPDATE_ISSUE);
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getDeleteVTN() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = new ConcurrentUpdateException(MessageConstant.CONCURRENT_UPDATE_ISSUE);
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setDeleteVTN(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    /**
     * 开始一个新的事务
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.beginTransaction();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交指定事务
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commitTransaction(xid);
    }

    /**
     * 中止指定事务
     * @param xid
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 内部处理事务中止的逻辑
     * @param xid
     * @param autoAborted
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = this.activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abortTransaction(xid);
    }

    public void releaseEntry(Entry entry) {
        super.releaseReferenceByKey(entry.getUid());
    }

}
