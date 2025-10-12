package org.minidb.backend.vm;

import org.minidb.backend.tm.TransactionManager;
import org.minidb.common.constant.VMConstant;


public class Visibility {

    /**
     * 判断是否存在版本跳跃
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long deleteVTN = e.getDeleteVTN();
        if(t.level == VMConstant.READ_UNCOMMITTED) {
            return false;
        } else {
            return tm.isCommitted(deleteVTN) && (deleteVTN > t.xid || t.isInSnapshot(deleteVTN));
        }
    }

    /**
     * 判断对于事务t来说，当前版本是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == VMConstant.READ_UNCOMMITTED) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long createVTN = e.getCreateVTN();
        long deleteVTN = e.getDeleteVTN();
        if(createVTN == xid && deleteVTN == 0) return true;

        if(tm.isCommitted(createVTN)) {
            if(deleteVTN == 0) return true;
            if(deleteVTN != xid) {
                if(!tm.isCommitted(deleteVTN)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long createVTN = e.getCreateVTN();
        long deleteVTN = e.getDeleteVTN();
        if(createVTN == xid && deleteVTN == 0) return true;

        if(tm.isCommitted(createVTN) && createVTN < xid && !t.isInSnapshot(createVTN)) {
            if(deleteVTN == 0) return true;
            if(deleteVTN != xid) {
                if(!tm.isCommitted(deleteVTN) || deleteVTN > xid || t.isInSnapshot(deleteVTN)) {
                    return true;
                }
            }
        }
        return false;
    }

}
