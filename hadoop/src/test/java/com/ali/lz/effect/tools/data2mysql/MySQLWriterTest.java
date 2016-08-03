/**
 * 
 */
package com.ali.lz.effect.tools.data2mysql;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.hbase.data2mysql.MD5Digest;
import com.ali.lz.effect.tools.hbase.data2mysql.MySQLWriter;
import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * @author jiuling.ypf
 * 
 */
public class MySQLWriterTest {

    private IMocksControl control;
    private JDBCUtil mockJDBCUtil;
    private java.sql.Connection mockConnection;
    private java.sql.Statement mockStatement;
    private MySQLWriter rptDetailWriter;
    private MySQLWriter rptSummaryWriter;
    private MySQLWriter rptExtendAdWriter;
    private static final String[] tableNames = new String[] { "rpt_detail", "rpt_summary", "rpt_extend_ad" };

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        mockJDBCUtil = control.createMock(JDBCUtil.class);
        mockConnection = control.createMock(Connection.class);
        mockStatement = control.createMock(Statement.class);
        rptDetailWriter = new MySQLWriter(tableNames[0], mockJDBCUtil);
        rptSummaryWriter = new MySQLWriter(tableNames[1], mockJDBCUtil);
        rptExtendAdWriter = new MySQLWriter(tableNames[2], mockJDBCUtil);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        control = null;
        mockJDBCUtil = null;
        rptDetailWriter = null;
        rptSummaryWriter = null;
        rptExtendAdWriter = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MySQLWriter#insertRpt(java.util.concurrent.ConcurrentHashMap)}
     * .
     */
    @Test
    public void testInsertRptNull() {
        final ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = null;
        boolean flag = rptDetailWriter.insertRpt(hashMap);
        assertFalse(flag);
        flag = rptSummaryWriter.insertRpt(hashMap);
        assertFalse(flag);
        flag = rptExtendAdWriter.insertRpt(hashMap);
        assertFalse(flag);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MySQLWriter#insertRpt(java.util.concurrent.ConcurrentHashMap)}
     * .
     */
    @Test
    public void testInsertRptDetail() throws Exception {
        expect(mockJDBCUtil.getConnection(true)).andReturn(mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement);
        expect(mockStatement.execute("SET sql_mode=''")).andReturn(true);
        mockConnection.setAutoCommit(false);
        mockStatement
                .addBatch("INSERT IGNORE INTO rpt_detail(plan_id,day,dim,src_id,path_id,ind_103) VALUES(120,20120611,100,1,1,103)");
        int[] ret = {};
        expect(mockStatement.executeBatch()).andReturn(ret);
        mockConnection.commit();
        mockConnection.setAutoCommit(true);
        mockJDBCUtil.closeStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        final ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = new ConcurrentHashMap<byte[], Map<byte[], byte[]>>();
        Map<byte[], byte[]> valueMap = new HashMap<byte[], byte[]>();
        int day = 20120611;
        int userId = 10;
        int planId = 120;
        short dimOrAdId = 0;
        String dSrc = "1\u00021\u0003男装\u00032";
        byte[] rowkeyA = new byte[46];
        byte[] rowkeyB = new byte[14];
        byte[] day1 = Bytes.toBytes(day);
        Bytes.putBytes(rowkeyB, 0, day1, 0, day1.length);
        byte[] userId1 = Bytes.toBytes(userId);
        Bytes.putBytes(rowkeyB, 4, userId1, 0, userId1.length);
        byte[] planId1 = Bytes.toBytes(planId);
        Bytes.putBytes(rowkeyB, 8, planId1, 0, planId1.length);
        byte[] dimOrAdId1 = Bytes.toBytes(dimOrAdId);
        Bytes.putBytes(rowkeyB, 12, dimOrAdId1, 0, dimOrAdId1.length);
        Bytes.putBytes(rowkeyA, 0, rowkeyB, 0, rowkeyB.length);
        String md5 = MD5Digest.encode(dSrc);
        byte[] md51 = Bytes.toBytes(md5);
        Bytes.putBytes(rowkeyA, 14, md51, 0, md51.length);
        valueMap.clear();
        int indId0 = 103;
        String indIndex0 = "i" + indId0;
        String indValue = String.valueOf(indId0);
        valueMap.put(Bytes.toBytes(indIndex0), Bytes.toBytes(indValue));
        valueMap.put(Bytes.toBytes("src"), Bytes.toBytes(dSrc));
        hashMap.put(rowkeyA, valueMap);
        boolean flag = rptDetailWriter.insertRpt(hashMap);

        control.verify();
        control.reset();

        assertTrue(flag);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MySQLWriter#insertRpt(java.util.concurrent.ConcurrentHashMap)}
     * .
     */
    @Test
    public void testInsertRptSummary() throws Exception {
        expect(mockJDBCUtil.getConnection(true)).andReturn(mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement);
        expect(mockStatement.execute("SET sql_mode=''")).andReturn(true);
        mockConnection.setAutoCommit(false);
        mockStatement.addBatch("INSERT IGNORE INTO rpt_summary(plan_id,day,dim,ind_103) VALUES(120,20120611,100,103)");
        int[] ret = {};
        expect(mockStatement.executeBatch()).andReturn(ret);
        mockConnection.commit();
        mockConnection.setAutoCommit(true);
        mockJDBCUtil.closeStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        final ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = new ConcurrentHashMap<byte[], Map<byte[], byte[]>>();
        Map<byte[], byte[]> valueMap = new HashMap<byte[], byte[]>();
        int day = 20120611;
        int userId = 10;
        int planId = 120;
        short dimOrAdId = 0;
        byte[] rowkeyB = new byte[14];
        byte[] day1 = Bytes.toBytes(day);
        Bytes.putBytes(rowkeyB, 0, day1, 0, day1.length);
        byte[] userId1 = Bytes.toBytes(userId);
        Bytes.putBytes(rowkeyB, 4, userId1, 0, userId1.length);
        byte[] planId1 = Bytes.toBytes(planId);
        Bytes.putBytes(rowkeyB, 8, planId1, 0, planId1.length);
        byte[] dimOrAdId1 = Bytes.toBytes(dimOrAdId);
        Bytes.putBytes(rowkeyB, 12, dimOrAdId1, 0, dimOrAdId1.length);
        valueMap.clear();
        int indId0 = 103;
        String indIndex0 = "i" + indId0;
        String indValue = String.valueOf(indId0);
        valueMap.put(Bytes.toBytes(indIndex0), Bytes.toBytes(indValue));
        hashMap.put(rowkeyB, valueMap);
        boolean flag = rptSummaryWriter.insertRpt(hashMap);

        control.verify();
        control.reset();

        assertTrue(flag);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.data2mysql.MySQLWriter#insertRpt(java.util.concurrent.ConcurrentHashMap)}
     * .
     */
    @Test
    public void testInsertRptExtendAd() throws Exception {
        expect(mockJDBCUtil.getConnection(true)).andReturn(mockConnection);
        expect(mockConnection.createStatement()).andReturn(mockStatement);
        expect(mockStatement.execute("SET sql_mode=''")).andReturn(true);
        mockConnection.setAutoCommit(false);
        mockStatement
                .addBatch("INSERT IGNORE INTO rpt_extend_ad(plan_id,day,ad_id,ind_103) VALUES(120,20120611,'111',103)");
        int[] ret = {};
        expect(mockStatement.executeBatch()).andReturn(ret);
        mockConnection.commit();
        mockConnection.setAutoCommit(true);
        mockJDBCUtil.closeStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        final ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = new ConcurrentHashMap<byte[], Map<byte[], byte[]>>();
        Map<byte[], byte[]> valueMap = new HashMap<byte[], byte[]>();
        int day = 20120611;
        int userId = 10;
        int planId = 120;
        String adId = "111";
        byte[] rowkeyB = new byte[15];
        byte[] day1 = Bytes.toBytes(day);
        Bytes.putBytes(rowkeyB, 0, day1, 0, day1.length);
        byte[] userId1 = Bytes.toBytes(userId);
        Bytes.putBytes(rowkeyB, 4, userId1, 0, userId1.length);
        byte[] planId1 = Bytes.toBytes(planId);
        Bytes.putBytes(rowkeyB, 8, planId1, 0, planId1.length);
        byte[] adId1 = Bytes.toBytes(adId);
        Bytes.putBytes(rowkeyB, 12, adId1, 0, adId1.length);
        valueMap.clear();
        int indId0 = 103;
        String indIndex0 = "i" + indId0;
        String indValue = String.valueOf(indId0);
        valueMap.put(Bytes.toBytes(indIndex0), Bytes.toBytes(indValue));
        hashMap.put(rowkeyB, valueMap);
        boolean flag = rptExtendAdWriter.insertRpt(hashMap);

        control.verify();
        control.reset();

        assertTrue(flag);
    }

}
