package com.ali.lz.effect.utils;

import java.util.Collection;

public class CollectionUtil {
    public static Collection<Integer> mergeCollections(Collection<Integer> c1, Collection<Integer> c2) {
        if (c1 == null)
            return c2;
        else if (c2 == null)
            return c1;
        else {
            c1.addAll(c2);
            return c1;
        }
    }

}
