/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.KeyValuePair;

/**
 * @author jiuling.ypf
 * 
 */
public class KeyValuePairTest {

    private KeyValuePair pair;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        pair = new KeyValuePair("attr1", "value1");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        pair = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.KeyValuePair#getKey()}.
     */
    @Test
    public void testGetKey() {
        assertEquals(pair.getKey(), "attr1");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.KeyValuePair#setKey(java.lang.String)}
     * .
     */
    @Test
    public void testSetKey() {
        pair.setKey("attr2");
        assertEquals(pair.getKey(), "attr2");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.KeyValuePair#getValue()}.
     */
    @Test
    public void testGetValue() {
        assertEquals(pair.getValue(), "value1");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.KeyValuePair#setValue(java.lang.String)}
     * .
     */
    @Test
    public void testSetValue() {
        pair.setValue("value2");
        assertEquals(pair.getValue(), "value2");
    }

}
