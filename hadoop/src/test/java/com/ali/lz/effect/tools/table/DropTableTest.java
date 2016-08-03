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

import com.ali.lz.effect.tools.hbase.table.DropTable;
import com.ali.lz.effect.tools.util.Constants;

/**
 * @author jiuling.ypf
 * 
 */
@Ignore
public class DropTableTest {

    private IMocksControl control;
    private HBaseAdmin mockhbaseAdmin;

    private DropTable dropTable;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        mockhbaseAdmin = control.createMock(HBaseAdmin.class);
        dropTable = new DropTable(mockhbaseAdmin);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        control = null;
        mockhbaseAdmin = null;
        dropTable = null;
    }

    /**
     * Test method for {@link com.ali.lz.effect.tools.hbase.table.DropTable#doWork()}.
     */
    @Test
    public void testDoWorkOffline() throws Exception {
        for (int i = 0; i < Constants.OFFLINE_HBASE_TABLE_NAMES.length; i++) {
            if (i != Constants.OFFLINE_HBASE_TABLE_NAMES.length - 1) {
                expect(mockhbaseAdmin.tableExists(Constants.OFFLINE_HBASE_TABLE_NAMES[i])).andReturn(true);
                mockhbaseAdmin.disableTable(Constants.OFFLINE_HBASE_TABLE_NAMES[i]);
                mockhbaseAdmin.deleteTable(Constants.OFFLINE_HBASE_TABLE_NAMES[i]);
            } else {
                expect(mockhbaseAdmin.tableExists(Constants.OFFLINE_HBASE_TABLE_NAMES[i])).andReturn(false);
            }
            control.replay();
            boolean flag = dropTable.doWork(Constants.OFFLINE_HBASE_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

    /**
     * Test method for {@link com.ali.lz.effect.tools.hbase.table.DropTable#doWork()}.
     */
    @Test
    public void testDoWorkOnline() throws Exception {
        for (int i = 0; i < Constants.ONLINE_HBASE_TABLE_NAMES.length; i++) {
            if (i != Constants.ONLINE_HBASE_TABLE_NAMES.length - 1) {
                expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_TABLE_NAMES[i])).andReturn(true);
                mockhbaseAdmin.disableTable(Constants.ONLINE_HBASE_TABLE_NAMES[i]);
                mockhbaseAdmin.deleteTable(Constants.ONLINE_HBASE_TABLE_NAMES[i]);
            } else {
                expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_TABLE_NAMES[i])).andReturn(false);
            }
            control.replay();
            boolean flag = dropTable.doWork(Constants.ONLINE_HBASE_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

    /**
     * Test method for {@link com.ali.lz.effect.tools.hbase.table.DropTable#doWork()}.
     */
    @Test
    public void testDoWorkOnlineIndex() throws Exception {
        for (int i = 0; i < Constants.ONLINE_HBASE_INDEX_TABLE_NAMES.length; i++) {
            expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i])).andReturn(true);
            mockhbaseAdmin.disableTable(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i]);
            mockhbaseAdmin.deleteTable(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i]);
            control.replay();
            boolean flag = dropTable.doWork(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);

            expect(mockhbaseAdmin.tableExists(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i])).andReturn(false);
            control.replay();
            flag = dropTable.doWork(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i]);
            control.verify();
            control.reset();
            assertTrue(flag);
        }
    }

}
