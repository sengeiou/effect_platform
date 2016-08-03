/**
 * 
 */
package com.ali.lz.effect.tools.data2mysql;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.hbase.data2mysql.MD5Digest;

/**
 * @author jiuling.ypf
 * 
 */
public class MD5DigestTest {

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
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MD5Digest#encode(java.lang.String)}
     * .
     */
    @Test
    public void testEncode() {
        final String srcStr = "0123456789";
        final String md5Str = "781e5e245d69b566979b86e28d23f2c7";
        String retStr = MD5Digest.encode(srcStr);
        assertEquals(md5Str, retStr);

        final String srcStr1 = "";
        final String md5Str1 = "d41d8cd98f00b204e9800998ecf8427e";
        String retStr1 = MD5Digest.encode(srcStr1);
        assertEquals(md5Str1, retStr1);

        final String srcStr2 = null;
        final String md5Str2 = "";
        String retStr2 = MD5Digest.encode(srcStr2);
        assertEquals(md5Str2, retStr2);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MD5Digest#checkExist(java.lang.String, java.lang.String)}
     * .
     */
    @Test
    public void testCheckExist() {
        final String srcStr = "0123456789";
        final String md5Str = "781e5e245d69b566979b86e28d23f2c7";
        boolean flag = MD5Digest.checkExist(srcStr, md5Str);
        assertTrue(flag);
        final String md5Str1 = "781e5e245d69b566979b86e28d23f2c8";
        flag = MD5Digest.checkExist(srcStr, md5Str1);
        assertFalse(flag);
    }

}
