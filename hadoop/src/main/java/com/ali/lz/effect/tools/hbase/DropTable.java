package com.ali.lz.effect.tools.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * 
 * @author jiuling.ypf 删除效果平台HBase表
 * 
 */

public class DropTable {

    private static final Log LOG = LogFactory.getLog(DropTable.class);

    private static final Configuration conf = HBaseConfiguration.create();

    private static final String[] tableNames = new String[] { "effect_rpt", "effect_rpt_sum", "effect_rpt_adclk" };

    public static void main(String args[]) {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            LOG.error(e);
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e);
        }

        for (String tableName : tableNames) {
            try {
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    LOG.info("table: " + tableName + " has been disable and deleted");
                } else {
                    LOG.error("table: " + tableName + " not exists, failed to delete");
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                LOG.equals(e);
            }
        }

    }
}
