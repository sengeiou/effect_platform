/**
 * 
 */
package com.ali.lz.effect.tools.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.util.ConfigLoader;

/**
 * @author jiuling.ypf
 * 
 */
public class ConfigLoaderTest {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#initConf()} .
     */
    @Test
    public void testInitConf() {
        assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMysqlServerIp()} .
     */
    @Test
    public void testGetMysqlServerIp() {
        assertEquals(ConfigLoader.getMysqlServerIp(), "127.0.0.1,127.0.0.1");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMysqlServerPort()} .
     */
    @Test
    public void testGetMysqlServerPort() {
        assertEquals(ConfigLoader.getMysqlServerPort(), 3306);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMysqlDbName()} .
     */
    @Test
    public void testGetMysqlDbName() {
        assertEquals(ConfigLoader.getMysqlDbName(), "test");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMysqlDbUser()} .
     */
    @Test
    public void testGetMysqlDbUser() {
        assertEquals(ConfigLoader.getMysqlDbUser(), "root");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMysqlDbPass()} .
     */
    @Test
    public void testGetMysqlDbPass() {
        assertEquals(ConfigLoader.getMysqlDbPass(), "");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getWriteThreadNum()} .
     */
    @Test
    public void testGetWriteThreadNum() {
        assertEquals(ConfigLoader.getWriteThreadNum(), 5);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getWriteBufferSize()} .
     */
    @Test
    public void testGetWriteBufferSize() {
        assertEquals(ConfigLoader.getWriteBufferSize(), 1024 * 1024);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getFlushInterval()} .
     */
    @Test
    public void testGetFlushInterval() {
        assertEquals(ConfigLoader.getFlushInterval(), 1000);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getReadThreadNum()} .
     */
    @Test
    public void testGetReadThreadNum() {
        assertEquals(ConfigLoader.getReadThreadNum(), 5);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getScannerCache()} .
     */
    @Test
    public void testGetScannerCache() {
        assertEquals(ConfigLoader.getScannerCache(), 500);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getMemFlushSize()} .
     */
    @Test
    public void testGetMemFlushSize() {
        assertEquals(ConfigLoader.getMemFlushSize(), 64 * 1024 * 1024);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getTimeToLive()}.
     */
    @Test
    public void testGetTimeToLive() {
        assertEquals(ConfigLoader.getTimeToLive(), 90 * 24 * 60 * 60);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getResetTable()} .
     */
    @Test
    public void testGetResetTable() {
        assertEquals(ConfigLoader.getResetTable(), false);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getDataDate()} .
     */
    @Test
    public void testGetDataDate() {
        assertEquals(ConfigLoader.getDataDate(), 0);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getWorkMode()} .
     */
    @Test
    public void testGetWorkMode() {
        assertEquals(ConfigLoader.getWorkMode(), "offline");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOutputDirPrefix()}
     * .
     */
    @Test
    public void testGetXmlOutputDirPrefix() {
        assertEquals(ConfigLoader.getXmlOutputDirPrefix(), "/home/lz/effect_platform/conf/report_rt/");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOutputFileFormat()}
     * .
     */
    @Test
    public void testGetXmlOutputFileFormat() {
        assertEquals(ConfigLoader.getXmlOutputFileFormat(), "report_*.xml");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOfflineDirPrefix()}
     * .
     */
    @Test
    public void testGetXmlOfflineDirPrefix() {
        assertEquals(ConfigLoader.getXmlOfflineDirPrefix(), "/home/lz/effect_platform/conf/report/");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOfflineFileFormat()}
     * .
     */
    @Test
    public void testGetXmlOfflineFileFormat() {
        assertEquals(ConfigLoader.getXmlOfflineFileFormat(), "report_*.xml");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOverdueDirPrefix()}
     * .
     */
    @Test
    public void testGetXmlOverduePrefix() {
        assertEquals(ConfigLoader.getXmlOverdueDirPrefix(), "/home/lz/effect_platform/conf/overdue/");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.ConfigLoader#getXmlOverdueFileFormat()}
     * .
     */
    @Test
    public void testGetXmlOverdueFileFormat() {
        assertEquals(ConfigLoader.getXmlOverdueFileFormat(), "report_*.xml");
    }

}
