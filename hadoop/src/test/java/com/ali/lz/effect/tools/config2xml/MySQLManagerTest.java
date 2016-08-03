/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.classextension.EasyMock.*;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.config2xml.MySQLManager;
import com.ali.lz.effect.tools.config2xml.PlanConfigResult;
import com.ali.lz.effect.tools.config2xml.PlanShareResult;
import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * @author jiuling.ypf
 * 
 */
public class MySQLManagerTest {

    private IMocksControl control;
    private JDBCUtil mockJDBCUtil;
    private java.sql.Connection mockConnection;
    private java.sql.PreparedStatement mockStatement;
    private java.sql.ResultSet mockResultSet;
    private MySQLManager dbManager;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        mockJDBCUtil = control.createMock(JDBCUtil.class);
        mockConnection = control.createMock(Connection.class);
        mockStatement = control.createMock(PreparedStatement.class);
        mockResultSet = control.createMock(ResultSet.class);
        dbManager = new MySQLManager(mockJDBCUtil);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        control = null;
        mockJDBCUtil = null;
        mockConnection = null;
        mockStatement = null;
        mockResultSet = null;
        dbManager = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#queryPlanShare()}.
     */
    @Test
    public void testQueryPlanShare() throws Exception {
        expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
        expect(mockConnection.prepareStatement("SELECT plan_id, type FROM plan_share")).andReturn(mockStatement);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true);
        expect(mockResultSet.getInt(1)).andReturn(120).times(1);
        expect(mockResultSet.getInt(2)).andReturn(0).times(1);
        expect(mockResultSet.next()).andReturn(true);
        expect(mockResultSet.getInt(1)).andReturn(121).times(1);
        expect(mockResultSet.getInt(2)).andReturn(1).times(1);
        expect(mockResultSet.next()).andReturn(false);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        List<PlanShareResult> list = dbManager.queryPlanShare();

        control.verify();
        control.reset();

        assertNotNull(list);
        assertEquals(2, list.size());

        int[] planId = { 120, 121 };
        int[] type = { 0, 1 };
        for (int i = 0; i < 2; i++) {
            assertEquals(list.get(i).getPlanId(), planId[i]);
            assertEquals(list.get(i).getType(), type[i]);
        }

    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#queryPlanConfig(java.util.List)}
     * .
     */
    @Test
    public void testQueryPlanConfig() throws Exception {
        expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
        expect(
                mockConnection
                        .prepareStatement("SELECT plan_id, user_id, effect_url, src_type, path_type, belong_id, ind_ids, period, expire_type, link_type FROM plan_config WHERE plan_id = ?"))
                .andReturn(mockStatement);
        mockStatement.setInt(1, 120);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true);
        expect(mockResultSet.getInt(1)).andReturn(120).times(1);
        expect(mockResultSet.getInt(2)).andReturn(10).times(1);
        expect(mockResultSet.getString(3)).andReturn("http://www.taobao.com").times(1);
        expect(mockResultSet.getString(4)).andReturn("101,102").times(1);
        expect(mockResultSet.getInt(5)).andReturn(0).times(1);
        expect(mockResultSet.getInt(6)).andReturn(1).times(1);
        expect(mockResultSet.getString(7)).andReturn("103,105").times(1);
        expect(mockResultSet.getInt(8)).andReturn(1).times(1);
        expect(mockResultSet.getInt(9)).andReturn(1).times(1);
        expect(mockResultSet.getString(10)).andReturn("1,2,3").times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockStatement.setInt(1, 121);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(false);
        mockJDBCUtil.closeResultSet(mockResultSet);

        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        List<PlanShareResult> shareConfigList = new ArrayList<PlanShareResult>();
        int[] planIds = { 120, 121, 122 };
        int[] types = { 1, 1, 2 };
        for (int i = 0; i < 3; i++) {
            shareConfigList.add(new PlanShareResult(planIds[i], types[i]));
        }

        List<PlanConfigResult> planConfigList = dbManager.queryPlanConfig(shareConfigList);

        control.verify();
        control.reset();

        assertNotNull(planConfigList);
        assertEquals(1, planConfigList.size());

        assertEquals(planConfigList.get(0).getPlanId(), 120);
        assertEquals(planConfigList.get(0).getUserId(), 10);
        assertEquals(planConfigList.get(0).getEffectUrl(), "http://www.taobao.com");
        assertEquals(planConfigList.get(0).getSrcType(), "101,102");
        assertEquals(planConfigList.get(0).getPathType(), 0);
        assertEquals(planConfigList.get(0).getBelongId(), 1);
        assertEquals(planConfigList.get(0).getIndIds(), "103,105");
        assertEquals(planConfigList.get(0).getPeriod(), 1);
        assertEquals(planConfigList.get(0).getExpireType(), 1);
        assertEquals(planConfigList.get(0).getLinkType(), "1,2,3");
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#querySrcCustomConfig(java.lang.String[])}
     * .
     */
    @Test
    public void testQuerySrcCustomConfig() throws Exception {
        expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
        expect(mockConnection.prepareStatement("SELECT src_url FROM src_custom_config WHERE src_id = ?")).andReturn(
                mockStatement);
        mockStatement.setInt(1, 10000);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getString(1)).andReturn("http://www.tmall.com").times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockStatement.setInt(1, 10001);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getString(1)).andReturn("http://www.juhuasuan.com").times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockStatement.setInt(1, 10002);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(false);
        mockJDBCUtil.closeResultSet(mockResultSet);

        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        final String[] srcTypes = { "10000", "10001", "10002" };

        List<String> srcCustomConfigList = dbManager.querySrcCustomConfig(srcTypes);

        control.verify();
        control.reset();

        assertNotNull(srcCustomConfigList);
        assertEquals(2, srcCustomConfigList.size());
        String[] srcUrls = { "http://www.tmall.com", "http://www.juhuasuan.com" };
        for (int i = 0; i < 2; i++) {
            assertEquals(srcCustomConfigList.get(i), srcUrls[i]);
        }
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#queryPathId(int)}.
     */
    @Test
    public void testQueryPathId() throws Exception {
        expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
        expect(mockConnection.prepareStatement("SELECT path_id FROM path_config WHERE plan_id = ? AND deleted = 0"))
                .andReturn(mockStatement);
        mockStatement.setInt(1, 120);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getInt(1)).andReturn(1).times(1);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getInt(1)).andReturn(2).times(1);
        expect(mockResultSet.next()).andReturn(false).times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        List<Integer> pathIdList = dbManager.queryPathId(120);

        control.verify();
        control.reset();

        assertNotNull(pathIdList);
        assertEquals(2, pathIdList.size());
        final int[] pathIds = { 1, 2 };
        for (int i = 0; i < 2; i++) {
            assertEquals(pathIdList.get(i).intValue(), pathIds[i]);
        }
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#queryPathConfig(int)}
     * .
     */
    @Test
    public void testQueryPathConfigInt() throws Exception {
        final int[] pathIds = { 1, 2, 3 };
        final String[] pathDatas = { "[{name:\"aaa\",url:\"http://www.taobao.com\",step:1}]",
                "[{name:\"bbb\",url:\"http://www.tmall.com\",step:2}]" };
        for (int i = 0; i < 3; i++) {
            expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
            expect(mockConnection.prepareStatement("SELECT data FROM path_config WHERE path_id = ? AND deleted = 0"))
                    .andReturn(mockStatement);
            mockStatement.setInt(1, pathIds[i]);
            expect(mockStatement.executeQuery()).andReturn(mockResultSet);
            if (i == 2) {
                expect(mockResultSet.next()).andReturn(false).times(1);
            } else {
                expect(mockResultSet.next()).andReturn(true).times(1);
                expect(mockResultSet.getString(1)).andReturn(pathDatas[i]).times(1);
            }
            mockJDBCUtil.closeResultSet(mockResultSet);
            mockJDBCUtil.closePreparedStatement(mockStatement);
            mockJDBCUtil.closeConnection(mockConnection);

            control.replay();
            String pathData = dbManager.queryPathConfig(pathIds[i]);
            if (i == 2) {
                assertNull(pathData);
            } else {
                assertNotNull(pathData);
                assertEquals(pathData, pathDatas[i]);
            }
            control.verify();
            control.reset();
        }
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#queryPathConfig(java.lang.String[])}
     * .
     */
    @Test
    public void testQueryPathConfigStringArray() throws Exception {
        final String[] pathIds = { "1", "2", "3" };
        final String[] pathDatas = { "[{name:\"aaa\",url:\"http://www.taobao.com\",step:1}]",
                "[{name:\"bbb\",url:\"http://www.tmall.com\",step:2}]" };

        expect(mockJDBCUtil.getConnection(false)).andReturn(mockConnection);
        expect(mockConnection.prepareStatement("SELECT data FROM path_config WHERE path_id = ? AND deleted = 0"))
                .andReturn(mockStatement);
        mockStatement.setInt(1, 1);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getString(1)).andReturn(pathDatas[0]).times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockStatement.setInt(1, 2);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(true).times(1);
        expect(mockResultSet.getString(1)).andReturn(pathDatas[1]).times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockStatement.setInt(1, 3);
        expect(mockStatement.executeQuery()).andReturn(mockResultSet);
        expect(mockResultSet.next()).andReturn(false).times(1);
        mockJDBCUtil.closeResultSet(mockResultSet);
        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        List<String> pathDataList = dbManager.queryPathConfig(pathIds);

        control.verify();
        control.reset();

        assertNotNull(pathDataList);
        assertEquals(2, pathDataList.size());
        assertEquals(pathDatas[0], pathDataList.get(0));
        assertEquals(pathDatas[1], pathDataList.get(1));
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.config2xml.MySQLManager#deletePlanShare(java.util.List)}
     * .
     */
    @Test
    public void testDeletePlanShare() throws Exception {
        int[] ret = {};
        expect(mockJDBCUtil.getConnection(true)).andReturn(mockConnection);
        expect(mockConnection.prepareStatement("DELETE FROM plan_share WHERE plan_id = ? AND type = ?")).andReturn(
                mockStatement);
        mockConnection.setAutoCommit(false);
        mockStatement.setInt(1, 120);
        mockStatement.setInt(2, 1);
        mockStatement.addBatch();
        mockStatement.setInt(1, 121);
        mockStatement.setInt(2, 0);
        mockStatement.addBatch();
        expect(mockStatement.executeBatch()).andReturn(ret);
        mockConnection.commit();
        mockConnection.setAutoCommit(true);
        mockJDBCUtil.closePreparedStatement(mockStatement);
        mockJDBCUtil.closeConnection(mockConnection);

        control.replay();

        List<PlanShareResult> list = new ArrayList<PlanShareResult>();
        list.add(new PlanShareResult(120, 1));
        list.add(new PlanShareResult(121, 0));
        boolean flag = dbManager.deletePlanShare(list);

        control.verify();
        control.reset();

        assertTrue(flag);
    }

}
