/**
 * 
 */
package com.ali.lz.effect.tools.table;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.lz.effect.tools.hbase.table.CreateTable;
import com.ali.lz.effect.tools.util.Constants;

/**
 * @author jiuling.ypf
 * 
 */
@Ignore
public class CreateTableTest {

    private IMocksControl control;
    private HBaseAdmin mockhbaseAdmin;

    private CreateTable createTable;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        mockhbaseAdmin = control.createMock(HBaseAdmin.class);
        createTable = new CreateTable(mockhbaseAdmin);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        control = null;
        mockhbaseAdmin = null;
        createTable = null;
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.table.CreateTable#doWork()}.
     */
    @Test
    public void testDoWorkOffline() throws Exception {
        for (int i = 0; i < Constants.OFFLINE_HBASE_TABLE_NAMES.length; i++) {
            expect(mockhbaseAdmin.tableExists(Constants.OFFLINE_HBASE_TABLE_NAMES[i])).andReturn(true);
            control.replay();
            boolean flag = createTable.doWork(Constants.OFFLINE_HBASE_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.table.CreateTable#doWork()}.
     */
    @Test
    public void testDoWorkOnline() throws Exception {
        for (int i = 0; i < Constants.ONLINE_HBASE_TABLE_NAMES.length; i++) {
            expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_TABLE_NAMES[i])).andReturn(true);
            control.replay();
            boolean flag = createTable.doWork(Constants.ONLINE_HBASE_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

    /**
     * Test method for
     * {@link com.ali.lz.effect.tools.hbase.table.CreateTable#doWork()}.
     */
    @Test
    public void testDoWorkOnlineIndex() throws Exception {
        for (int i = 0; i < Constants.ONLINE_HBASE_INDEX_TABLE_NAMES.length; i++) {
            expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i])).andReturn(true);
            control.replay();
            boolean flag = createTable.doWork(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

}
