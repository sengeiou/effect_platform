package com.ali.lz.effect.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and expirationSecs * (1 +
 * 1 / (numBuckets-1)) to actually expire the message.
 * 
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 * 
 * The advantage of this design is that the expiration thread only locks the
 * object for O(1) time, meaning the object is essentially always available for
 * gets/puts.
 */
public class RotatingSet<T> {
    // this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public static interface ExpiredCallback<T> {
        public void expire(T key);
    }

    private LinkedList<HashSet<T>> _buckets;

    private ExpiredCallback<T> _callback;

    public RotatingSet(int numBuckets, ExpiredCallback<T> callback) {
        if (numBuckets < 2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<HashSet<T>>();
        for (int i = 0; i < numBuckets; i++) {
            _buckets.add(new HashSet<T>());
        }

        _callback = callback;
    }

    public RotatingSet(ExpiredCallback<T> callback) {
        this(DEFAULT_NUM_BUCKETS, callback);
    }

    public RotatingSet(int numBuckets) {
        this(numBuckets, null);
    }

    public Set<T> rotate() {
        Set<T> dead = _buckets.removeLast();
        _buckets.addFirst(new HashSet<T>());
        if (_callback != null) {
            Iterator<T> it = dead.iterator();
            while (it.hasNext())
                _callback.expire(it.next());
        }
        return dead;
    }

    public boolean contains(T key) {
        for (HashSet<T> bucket : _buckets) {
            if (bucket.contains(key)) {
                return true;
            }
        }
        return false;
    }

    public void add(T key) {
        Iterator<HashSet<T>> it = _buckets.iterator();
        HashSet<T> bucket = it.next();
        bucket.add(key);
        while (it.hasNext()) {
            bucket = it.next();
            bucket.remove(key);
        }
    }

    public Object remove(T key) {
        for (HashSet<T> bucket : _buckets) {
            if (bucket.contains(key)) {
                return bucket.remove(key);
            }
        }
        return null;
    }

    public int size() {
        int size = 0;
        for (HashSet<T> bucket : _buckets) {
            size += bucket.size();
        }
        return size;
    }
}
