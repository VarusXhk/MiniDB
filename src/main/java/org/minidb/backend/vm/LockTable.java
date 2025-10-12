package org.minidb.backend.vm;

import org.minidb.common.constant.MessageConstant;
import org.minidb.common.exception.DeadLockException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，用于死锁检测
 */
public class LockTable {

    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> waitX; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    // 用于DFS检测
    private Map<Long, Integer> xidStamp;
    private int stamp;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        waitX = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 尝试为事务 xid 请求资源 uid 的锁
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 如果 uid 没有被任何事务持有，则将它分配给 xid
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 如果 uid 已经被其他事务持有，则将 xid 添加到等待列表中
            waitU.put(xid, uid);
            putIntoList(waitX, uid, xid);
            // 检查是否存在死锁，如果存在，移除xid并抛出 DeadLockException
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(waitX, uid, xid);
                throw new DeadLockException(MessageConstant.DEAD_LOCK);
            }
            // 如果没有死锁，创建一个新的 ReentrantLock，并将其返回
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放事务 xid 所持有的所有资源，同时更新相关的等待列表和持有列表，确保资源被正确释放
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(!l.isEmpty()) {
                    Long uid = l.removeFirst();
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个事务来占用资源 uid
     * 遍历等待该资源的事务列表，找到一个可以获得锁的事务，将其从等待状态转为持有状态，并释放对应的锁
     * @param uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list = waitX.get(uid);
        if(list == null) return;
        while(!list.isEmpty()) {
            long xid = list.removeFirst();
            if(waitLock.containsKey(xid)) {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(list.isEmpty()) waitX.remove(uid);
    }

    /**
     * 检测是否存在死锁
     * 使用深度优先搜索（DFS）遍历 x2u 图，检查是否存在环路，如果存在环路，则表明发生了死锁
     * @return
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 深度优先搜索，用于检测死锁
     * 记录访问的 xid 的时间戳，检查其是否已经访问过。
     * 递归访问该事务等待的 UID 持有的事务，如果回到之前访问的节点，说明存在死锁
     * @param xid
     * @return
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    /**
     * 找到 uid0 对应的列表，移除其中的 uid1
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if(list == null) return;
        list.remove(uid1);
        Iterator<Long> i = list.iterator();
        // 若uid0对应的列表只包含uid1，则在删除后将uid0对应的列表删除
        if(list.isEmpty()) {
            listMap.remove(uid0);
        }
    }

    /**
     * 将 uid1 添加到 uid0 对应的列表中
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).addFirst(uid1);
    }

    /**
     * 查找 uid0 对应的列表，并确定 uid1 是否存在于该列表listMap中
     * @param listMap
     * @param uid0
     * @param uid1
     * @return
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if(list == null) return false;
        for (long e : list) {
            if (e == uid1) {
                return true;
            }
        }
        return false;
    }

}
