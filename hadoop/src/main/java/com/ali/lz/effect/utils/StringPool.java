package com.ali.lz.effect.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 字符串常量池实现
 * 
 * @author wxz
 * 
 */
public class StringPool {

    // 初始大小为 256k 个元素的 ConcurrentHashMap 总空间占用为 1050112 bytes
    private static final int MAX_STRING_POOL_SIZE = 256 * 1024;
    private static final float MAX_LOADFACTOR = 0.9f;
    private static final int THRESHOLD = (int) (MAX_STRING_POOL_SIZE * MAX_LOADFACTOR);

    private static Object lck = new Object();
    private static ConcurrentHashMap<String, String> internPool = new ConcurrentHashMap<String, String>(
            MAX_STRING_POOL_SIZE, MAX_LOADFACTOR);
    // 当前常量池中独立字符串计数，因为 ConcurrentHashMap.size() 方法开销较大，经微性能测试自行作此计数可以节省一半以上的时间
    private static AtomicInteger cnt = new AtomicInteger(0);

    /**
     * 将给定字符串放入常量池，尽量保证相同内容的字符串仅有一个实例。
     * 
     * @param s
     *            待处理字符串
     * @return 缓冲后字符串引用
     */
    public static String poolStr(String s) {
        poolCheck();

        String r = internPool.get(s);
        if (r == null) {
            synchronized (lck) {
                internPool.put(s, s);
                cnt.incrementAndGet();
            }
            r = s;
        }
        return r;
    }

    /**
     * 检查当前字符串常量池的大小，若大于阈值则创建一个新的常量池
     */
    private static void poolCheck() {
        if (cnt.get() >= THRESHOLD) {
            // 这里无法简单利用 AtomicInteger 的 CAS 操作消除加锁逻辑，因为计数自增操作和 CAS 操作不是原子的，而 CAS
            // 不能完成">="比较，故可能发生错过比较阈值的问题
            synchronized (lck) {
                // 上次检查到这里有一个小的竟态窗口，为了以防万一获得锁后要重新检查一下常量池大小
                if (cnt.get() >= THRESHOLD) {
                    ConcurrentHashMap<String, String> newPool = new ConcurrentHashMap<String, String>(
                            MAX_STRING_POOL_SIZE, MAX_LOADFACTOR);
                    internPool = newPool;
                    cnt.set(0);
                }
            }
        }
    }

}
