package com.ali.lz.effect.holotree;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.holotree.PTLogEntry;

public class PTLogEntryTest {
    private PTLogEntry log;

    @Before
    public void setUp() throws Exception {
        log = new PTLogEntry();
    }

    @After
    public void tearDown() throws Exception {
        log = null;
    }

    @Test
    public void testMatchedAndTypeID() {
        assertFalse(log.matched());
        assertEquals(log.getPType(), 0);
        assertEquals(log.getRType(), 0);

        log.setMatched(true);
        log.setPType(65535);
        assertTrue(log.matched());
        assertEquals(log.getPType(), 65535);

        log.setMatched(false);
        assertFalse(log.matched());

        log.setMatched(true);
        assertTrue(log.matched());
    }

    @Test
    public void testPutAndGet() {
        final int batchCount = 10000;
        for (int i = 0; i < batchCount; ++i)
            log.put((new Integer(i)).toString(), (new Integer(i + 119)).toString());
        for (int i = 0; i < batchCount; ++i) {
            String k = (new Integer(i)).toString();
            String v = (new Integer(i + 119)).toString();
            assertEquals(log.get(k), v);
            assertTrue(log.containsKey(k));
            assertTrue(log.containsValue(v));
        }
    }

    @Test
    public void testEmptyAndClear() {
        log.put("", "");
        log.put(null, null);
        assertFalse(log.isEmpty());

        log.clear();
        assertTrue(log.isEmpty());
        assertNull(log.get("Key"));
        assertNull(log.get("None"));
        assertNull(log.get(null));
    }

    @Test
    public void testRemoveAndSize() {
        log.put("", "1");
        assertEquals(log.size(), 1);

        log.put(null, null);
        assertEquals(log.size(), 2);

        log.put("", "");
        assertEquals(log.size(), 2);

        log.remove("");
        assertEquals(log.size(), 1);

        log.clear();
        assertEquals(log.size(), 0);

        final int batchCount = 10000;
        for (int i = 0; i < batchCount; ++i)
            log.put((new Integer(i)).toString(), (new Integer(i + 119)).toString());
        assertEquals(log.size(), batchCount);
    }
}
