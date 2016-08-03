/**
 * 
 */
package com.ali.lz.effect.tools.util;

import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * @author jiuling.ypf
 * 
 */
public class JDBCUtilTest extends JDBCUtil {

    private IMocksControl control;
    private java.sql.Connection mockConnection;
    private java.sql.PreparedStatement mockPreStatement;
    private java.sql.Statement mockStatement;
    private java.sql.ResultSet mockResultSet;
    private JDBCUtil jdbcUtil;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        mockConnection = control.createMock(Connection.class);
        mockPreStatement = control.createMock(PreparedStatement.class);
        mockStatement = control.createMock(Statement.class);
        mockResultSet = control.createMock(ResultSet.class);
        jdbcUtil = new JDBCUtil();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.JDBCUtil#getConnection()}.
     */
    @Test
    public void testGetConnection() {
        assertTrue(true);
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.JDBCUtil#closeConnection(java.sql.Connection)}
     * .
     */
    @Test
    public void testCloseConnection() throws Exception {
        mockConnection.close();
        control.replay();
        jdbcUtil.closeConnection(mockConnection);
        control.verify();
        control.reset();
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.JDBCUtil#closePreparedStatement(java.sql.PreparedStatement)}
     * .
     */
    @Test
    public void testClosePreparedStatement() throws Exception {
        mockPreStatement.close();
        control.replay();
        jdbcUtil.closePreparedStatement(mockPreStatement);
        control.verify();
        control.reset();
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.JDBCUtil#closeStatement(java.sql.Statement)}
     * .
     */
    @Test
    public void testCloseStatement() throws Exception {
        mockStatement.close();
        control.replay();
        jdbcUtil.closeStatement(mockStatement);
        control.verify();
        control.reset();
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.util.JDBCUtil#closeResultSet(java.sql.ResultSet)}
     * .
     */
    @Test
    public void testCloseResultSet() throws Exception {
        mockResultSet.close();
        control.replay();
        jdbcUtil.closeResultSet(mockResultSet);
        control.verify();
        control.reset();
    }

}
