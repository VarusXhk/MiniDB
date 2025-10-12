package org.minidb.backend.common;

import org.minidb.backend.dm.dataItem.DataItem;
import org.minidb.common.constant.MessageConstant;
import org.minidb.common.exception.CacheFullException;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    // 实际缓存的全部资源
    private HashMap<Long, T> cache;
    // 缓存中某资源的引用个数
    private HashMap<Long, Integer> references;
    // 线程是否在获取资源，true表示正在获取资源
    private HashMap<Long, Boolean> getting;
    // 缓存的最大缓存资源数
    private int maxResource;
    // 缓存中资源的个数
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据给定的 key 获取缓存中的资源
     * @param key
     * @return
     * @throws Exception
     */
    protected T getFromCache(long key) throws Exception {
        // 判断资源的获取情况，总共分为四种
        while(true) {
            lock.lock();
            // 1. 资源在缓存中且资源正在被其他线程获取，进入持续的休眠等待
            if(getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 2. 资源在缓存中且无其他线程获取，直接返回
            if(cache.containsKey(key)) {
                T objectFromCache = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return objectFromCache;
            }

            // 3. 资源不在缓存中且缓存已满，线程准备从外部获取资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw new CacheFullException(MessageConstant.CACHE_FULL);
            }
            // 4. 资源不在缓存中且缓存未满，线程也准备从外部获取资源
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 从外部获取资源并放入缓存
        T objectFromOutside = null;
        try {
            objectFromOutside = get2Cache(key);
        } catch(Exception e) {
            lock.lock();
            // 出现未知异常，线程停止获取该资源
            try {
                count--;
                getting.remove(key);
            }finally { lock.unlock();}
            throw e;
        }

        lock.lock();
        try {
            getting.remove(key);
            cache.put(key, objectFromOutside);
            references.put(key, 1);
        }finally {
            lock.unlock();
        }

        return objectFromOutside;
    }

    /**
     * 根据资源的key，将资源的引用计数减一，为零则移除至外部
     * @param key
     */
    protected void releaseReferenceByKey(long key) {
        lock.lock();
        try {
            // 获取资源的引用数
            int reference = references.get(key)-1;
            // 此资源在缓存中只有一个引用
            if(reference == 0) {
                T obj = cache.get(key);
                releaseByObj(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                //将资源的引用计数减一，更新
                references.put(key, reference);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void closeCache() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                // 将资源移除至外部
                T obj = cache.get(key);
                releaseByObj(obj);
                // 从缓存中删除资源
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时，从外部获取资源并放入缓存中
     */
    protected abstract T get2Cache(long key) throws Exception;
    /**
     * 根据资源数据，将资源从缓存移除写入外部
     */
    protected abstract void releaseByObj(T obj);
}
