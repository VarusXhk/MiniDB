package org.minidb.backend.dm.pageIndex;

import org.minidb.common.constant.PageConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public void PageIndex() {
        lock = new ReentrantLock();
        this.lists = new List[PageConstant.INTERVALS_NUMBER + 1];
        for (int i = 0; i < PageConstant.INTERVALS_NUMBER + 1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     *
     * @param pageNumber
     * @param freeSpace
     */
    public void add(int pageNumber, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / PageConstant.INTERVAL_SIZE;
            lists[number].add(new PageInfo(pageNumber, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     *
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / PageConstant.INTERVAL_SIZE;
            if(number < PageConstant.INTERVALS_NUMBER) number ++;
            while(number <= PageConstant.INTERVALS_NUMBER) {
                if(lists[number].isEmpty()) {
                    number ++;
                    continue;
                }
                return lists[number].removeFirst();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
