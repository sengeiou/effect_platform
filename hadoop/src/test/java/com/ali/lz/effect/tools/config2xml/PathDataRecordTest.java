/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.PathDataRecord;

/**
 * @author jiuling.ypf
 * 
 */
public class PathDataRecordTest {

    private PathDataRecord record = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        record = new PathDataRecord("name1", "http://lz.taobao.com", 1);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        record = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#getName()}.
     */
    @Test
    public void testGetName() {
        assertEquals(record.getName(), "name1");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#setName(java.lang.String)}
     * .
     */
    @Test
    public void testSetName() {
        record.setName("name2");
        assertEquals(record.getName(), "name2");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#getUrl()}.
     */
    @Test
    public void testGetUrl() {
        assertEquals(record.getUrl(), "http://lz.taobao.com");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#setUrl(java.lang.String)}
     * .
     */
    @Test
    public void testSetUrl() {
        record.setUrl("http://www.taobao.com");
        assertEquals(record.getUrl(), "http://www.taobao.com");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#getStep()}.
     */
    @Test
    public void testGetStep() {
        assertEquals(record.getStep(), 1);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.PathDataRecord#setStep(int)}.
     */
    @Test
    public void testSetStep() {
        record.setStep(2);
        assertEquals(record.getStep(), 2);
    }

}
