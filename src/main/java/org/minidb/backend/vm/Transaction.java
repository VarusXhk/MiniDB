package org.minidb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import org.minidb.common.constant.TMConstant;
import org.minidb.common.constant.VMConstant;
import org.minidb.common.exception.BaseException;

/**
 * vm对一个事务的抽象
 */
public class Transaction {
    public long xid;
    // 标识事务的隔离等级
    public int level;
    // 存储当前事务快照的状态
    // 事务的快照是指在某一时刻对数据库状态的完整视图，记录了在特定时间点上所有事务的状态
    // Boolean 标识事务是否在快照中
    public Map<Long, Boolean> snapshot;
    public BaseException err;
    public boolean autoAborted;

    /**
     * 创建一个新的事务
     * @param xid
     * @param level
     * @param active
     * @return
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != VMConstant.READ_UNCOMMITTED) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断事务是否在快照中
     * @param xid
     * @return
     */
    public boolean isInSnapshot(long xid) {
        if(xid == TMConstant.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
