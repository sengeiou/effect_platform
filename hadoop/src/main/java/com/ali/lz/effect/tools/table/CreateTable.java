package com.ali.lz.effect.tools.table;

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
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
//import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.Constants;

/**
 * 创建效果平台HBase表
 * 
 * @author jiuling.ypf
 * 
 */

public class CreateTable {

    private static final Log LOG = LogFactory.getLog(CreateTable.class);

    private static final Configuration conf = HBaseConfiguration.create();

    private HBaseAdmin admin;

    public CreateTable(HBaseAdmin admin) {
        this.admin = admin;
    }

    /**
     * 创建HBase表
     * 
     * @param tableName
     * @return
     */
    public boolean doWork(String tableName) {
        HTableDescriptor des = null;
        try {

            byte[][] splits = null;
            // TODO JUST HARD CODING HERE
            boolean isIndexTable = false;
            for (int i = 0; i < Constants.ONLINE_HBASE_INDEX_TABLE_NAMES.length; i++) {
                if (tableName.equals(Constants.ONLINE_HBASE_INDEX_TABLE_NAMES[i])) {
                    isIndexTable = true;
                    splits = new byte[][] { { (byte) 1 }, { (byte) 2 }, { (byte) 3 }, { (byte) 4 }, { (byte) 5 },
                            { (byte) 6 }, { (byte) 7 }, { (byte) 8 }, { (byte) 9 }, { (byte) 10 }, { (byte) 11 },
                            { (byte) 12 }, { (byte) 13 }, { (byte) 14 }, { (byte) 15 }, { (byte) 16 }, { (byte) 17 },
                            { (byte) 18 }, { (byte) 19 }, { (byte) 20 }, { (byte) 21 }, { (byte) 22 }, { (byte) 23 },
                            { (byte) 24 }, { (byte) 25 }, { (byte) 26 }, { (byte) 27 }, { (byte) 28 }, { (byte) 29 },
                            { (byte) 30 }, { (byte) 31 } };
                    break;
                }
            }

            HColumnDescriptor disc = new HColumnDescriptor(Constants.HBASE_FAMILY_NAME);
            disc.setMaxVersions(1);
            disc.setCompressionType(Algorithm.LZO); // 设置LZO压缩存储，减少磁盘IO开销，对读写性能都能所提高，以后默认建表都开启！
            disc.setBloomFilterType(BloomType.ROW); // 设置按行方式的BloomFilter，加快rowkey的检索过程！
            // disc.setDataBlockEncoding(DataBlockEncoding.DIFF); //
            // 设置前缀压缩（HBase 0.94开始支持），节省region server内存开销，以后默认建表都开启！

            if (isIndexTable) // 月光宝盒实时部分HBase索引表中仅保存1天数据
                disc.setTimeToLive(24 * 60 * 60);
            else
                // 默认月光宝盒的HBase表中保存90天数据（可配置）
                disc.setTimeToLive(ConfigLoader.getTimeToLive());
            disc.setInMemory(true);

            des = new HTableDescriptor(tableName);
            des.addFamily(disc);
            des.setMemStoreFlushSize(ConfigLoader.getMemFlushSize());

            if (!admin.tableExists(des.getNameAsString())) { // 不存在则直接创建
                if (splits == null)
                    admin.createTable(des);
                else
                    admin.createTable(des, splits);
                LOG.info("create table: " + des.getNameAsString() + " sucessfully");
            } else { // 存在则根据配置决定是否删除后重建表
                if (ConfigLoader.getResetTable()) {
                    if (admin.tableExists(des.getNameAsString())) {
                        admin.disableTable(des.getNameAsString());
                        admin.deleteTable(des.getNameAsString());
                        if (splits == null)
                            admin.createTable(des);
                        else
                            admin.createTable(des, splits);
                        LOG.info("table: " + des.getNameAsString() + " already exists, delete and recreate it");
                    }
                } else {
                    LOG.info("table: " + des.getNameAsString() + " already exists, do not need to create");
                }
            }
            return true;
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            LOG.error(e);
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            LOG.error(e);
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e);
            return false;
        }
    }

    /**
     * 创建月光宝盒HBase表
     * 
     * @param args
     */
    public static void main(String args[]) {
        if (args.length != 1) {
            System.out.println("Usage: com.etao.lz.effect.tools.table.CreateTable [online/offline]");
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
        CreateTable createTable = new CreateTable(admin);
        String[] tableNames = (workMode.equals("offline")) ? Constants.OFFLINE_HBASE_TABLE_NAMES
                : Constants.ONLINE_HBASE_TABLE_NAMES;
        for (String tableName : tableNames) {
            createTable.doWork(tableName);
        }
    }
}
