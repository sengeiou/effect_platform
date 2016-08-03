package com.ali.lz.effect.tools.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * 
 * @author jiuling.ypf 创建效果平台HBase表
 */

public class CreateTable {

    private static final Log LOG = LogFactory.getLog(CreateTable.class);

    private static final Configuration conf = HBaseConfiguration.create();

    private static final String[] tableNames = new String[] { "effect_rpt", "effect_rpt_sum", "effect_rpt_adclk" };

    private static final String columnFamilyName = "d";

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
        HTableDescriptor des = null;
        try {

            for (String tableName : tableNames) {
                des = new HTableDescriptor(tableName);
                HColumnDescriptor disc = new HColumnDescriptor(columnFamilyName);
                disc.setMaxVersions(1);
                disc.setTimeToLive(ConfigLoader.getTTL());
                disc.setInMemory(true);
                des.addFamily(disc);
                des.setMemStoreFlushSize(ConfigLoader.getMemFlushSize());

                // 不存在则直接创建
                if (!admin.tableExists(des.getNameAsString())) {
                    admin.createTable(des);
                    LOG.info("create table: " + des.getNameAsString() + " sucessfully");
                } else {
                    // 存在则根据配置决定是否删除后重建表
                    if (ConfigLoader.getResetTable()) {
                        if (admin.tableExists(des.getNameAsString())) {
                            admin.disableTable(des.getNameAsString());
                            admin.deleteTable(des.getNameAsString());
                            admin.createTable(des);
                            LOG.info("table: " + des.getNameAsString() + " already exists, delete and recreate it");
                        }
                    } else {
                        LOG.info("table: " + des.getNameAsString() + " already exists, not need to create");
                    }
                }
            }

        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e);
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e);
        }
    }
}
