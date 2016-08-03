package com.ali.lz.effect.tools.table;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.ali.lz.effect.tools.util.Constants;

/**
 * 删除效果平台HBase表
 * 
 * @author jiuling.ypf
 * 
 */

public class DropTable {

    private static final Log LOG = LogFactory.getLog(DropTable.class);

    private static final Configuration conf = HBaseConfiguration.create();

    private HBaseAdmin admin;

    public DropTable(HBaseAdmin admin) {
        this.admin = admin;
    }

    /**
     * 删除HBase表
     * 
     * @param tableName
     * @return
     */
    public boolean doWork(String tableName) {
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                LOG.info("table: " + tableName + " has been disable and deleted");
            } else {
                LOG.error("table: " + tableName + " not exists, failed to delete");
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e);
            return false;
        }
        return true;
    }

    /**
     * 删除月光宝盒HBase表
     * 
     * @param args
     */
    public static void main(String args[]) {
        if (args.length != 1) {
            System.out.println("Usage: com.etao.lz.effect.tools.table.DropTable [online/offline]");
            System.exit(1);
        }
        String workMode = args[0];
        if (!workMode.equals("offline") && !workMode.equals("online")) {
            System.out.println("Error: invalid work mode: " + workMode);
            System.exit(1);
        }
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            LOG.error(e);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            LOG.error(e);
        }
        DropTable dropTable = new DropTable(admin);
        String[] tableNames = (workMode.equals("offline")) ? Constants.OFFLINE_HBASE_TABLE_NAMES
                : Constants.ONLINE_HBASE_TABLE_NAMES;
        for (String tableName : tableNames) {
            dropTable.doWork(tableName);
        }
    }
}
