package com.ali.lz.effect.tools.data2mysql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.Constants;

/**
 * 
 * @author jiuling.ypf HBase写入类
 */
public class HBaseWriter {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(HBaseWriter.class);

    // 效果平台的HBase表名
    private String tableName;

    // 效果平台的HBase表的列簇名
    private static final byte[] columnFamily = Bytes.toBytes(Constants.HBASE_FAMILY_NAME);

    // HBase客户端配置对象
    private static final Configuration conf = HBaseConfiguration.create();

    // 用于选择HTable对象的随机数
    private static final Random rand = new Random();

    // 每张表的HTable客户端对象个数
    private static final int tableN = 5;

    // HTable写客户端对象
    private static HTable wTables[];

    // HTable读客户端对象
    private static HTable rTable;

    /**
     * 构造函数
     */
    public HBaseWriter(String tableName) {
        // 传递HBase表名
        this.tableName = tableName;
        // 初始化HTable对象
        initHTables();
        // 启动后台定时刷新线程
        startDaemon();
    }

    /**
     * 删除指定日期date，指定userId（analyzerId），指定planId的数据记录
     * 
     * @param date
     * @param userId
     * @param planId
     * @return
     */
    public boolean deleteBefore(String date, String userId, String planId) {
        if (rTable == null || wTables == null) {
            LOG.error("HTable initialized error, terminate program");
            return false;
        }

        ResultScanner rs = null;
        Result rr = new Result();
        Scan scan = new Scan();
        scan.addFamily(columnFamily);
        int dateInt = Integer.parseInt(date);
        int userIdInt = Integer.parseInt(userId);
        int planIdInt = Integer.parseInt(planId);
        byte[] startKey = new byte[12];
        byte[] endKey = new byte[12];

        Bytes.putInt(startKey, 0, dateInt);
        Bytes.putInt(startKey, 4, userIdInt);
        Bytes.putInt(startKey, 8, planIdInt);
        Bytes.putInt(endKey, 0, dateInt);
        Bytes.putInt(endKey, 4, userIdInt);
        Bytes.putInt(endKey, 8, planIdInt + 1);

        scan.setStartRow(startKey);
        scan.setStopRow(endKey);

        try {
            rs = rTable.getScanner(scan);
            if (rs != null) {
                rr = rs.next();
            } else {
                LOG.error("get scanner error, null ResultScanner object");
                return false;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error(e);
            e.printStackTrace();
            return false;
        }

        int num = 0;
        while (rr != null) {
            if (!rr.isEmpty()) {
                byte[] rowkey = rr.getRow();
                Delete del = new Delete(rowkey);
                int tmp = rand.nextInt(tableN);
                synchronized (wTables[tmp]) {
                    try {
                        wTables[tmp].delete(del);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        LOG.error("delete to hbase exception: " + e);
                        return false;
                    }
                }
            }
            try {
                rr = rs.next();
            } catch (IOException e) {
                LOG.error("get result error, e=" + e.getLocalizedMessage());
                return false;
            }
            num++;
        }
        // 最后保证刷新数据到HBase服务端
        flushAllTables();
        LOG.info("table: " + tableName + ", date: " + date + ", user_id: " + userId + ", plan_id: " + planId
                + ", total delete records: " + num);
        return true;
    }

    /**
     * 向HBase表中插入一行记录
     * 
     * @param rowkey
     * @param indsMap
     * @return
     */
    public boolean writeToTable(byte[] rowkey, Map<String, String> indsMap) {
        Put put = getPutForTable(rowkey, indsMap);
        if (wTables == null) {
            LOG.error("HTable initialized error, terminate program");
            return false;
        }
        int tmp = rand.nextInt(tableN);
        synchronized (wTables[tmp]) {
            try {
                wTables[tmp].put(put);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                LOG.error("write to hbase exception: " + e);
                return false;
            }
        }
        return true;
    }

    /**
     * 强制刷新所有已写到HTable客户端对象中的数据到HBase 注意：操作结束前一定要调用该函数强制刷新客户端未提交的数据！
     */
    public void flushAllTables() {
        for (int k = 0; k < tableN; k++) {
            synchronized (wTables[k]) {
                try {
                    wTables[k].flushCommits();
                } catch (IOException e) {
                    LOG.error("flush to hbase exception: " + e);
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 返回HBase表名
     * 
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 初始化客户端HTable对象，用户数据写入HBase
     * 
     * @return
     */
    private boolean initHTables() {
        boolean b = true;
        try {
            wTables = new HTable[tableN];
            for (int i = 0; i < tableN; i++) {
                wTables[i] = new HTable(conf, tableName);
                wTables[i].setWriteBufferSize(ConfigLoader.getWriteBufferSize());
                wTables[i].setAutoFlush(false);
            }
            rTable = new HTable(conf, tableName);
            rTable.setScannerCaching(50);
        } catch (IOException e) {
            LOG.error("initHTables error, e=" + e.getLocalizedMessage());
            b = false;
        }
        return b;
    }

    /**
     * 启动后台定时刷新（每隔1秒）线程，避免每次Put操作都提交到HBase
     */
    private void startDaemon() {
        for (int i = 0; i < tableN; i++) {
            final int tmp = i;
            Thread th = new Thread() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            sleep(1000);
                        } catch (InterruptedException e) {
                            LOG.error(e);
                            e.printStackTrace();
                        }
                        synchronized (wTables[tmp]) {
                            try {
                                wTables[tmp].flushCommits();
                            } catch (IOException e) {
                                LOG.error("flush to hbase exception: " + e);
                                e.printStackTrace();
                            }
                        }
                    }
                }
            };
            th.setDaemon(true);
            th.start();
        }
    }

    /**
     * 根据rowkey和indsMap生成Put对象
     * 
     * @param rowkey
     * @param indsMap
     * @return
     */
    private Put getPutForTable(byte[] rowkey, final Map<String, String> indsMap) {
        Put put = new Put(rowkey);
        Iterator<String> iter = indsMap.keySet().iterator();
        while (iter.hasNext()) {
            String indIndex = iter.next();
            String indValue = indsMap.get(indIndex);
            put.add(columnFamily, Bytes.toBytes(indIndex), Bytes.toBytes(indValue));
        }
        return put;
    }

    /**
     * 测试函数
     * 
     * @param args
     */
    public static void main(String args[]) {
        final String[] tableNames = new String[] { "effect_rpt", "effect_rpt_sum", "effect_rpt_adclk" };
        HBaseWriter hbaseWriter0 = new HBaseWriter(tableNames[0]);
        HBaseWriter hbaseWriter1 = new HBaseWriter(tableNames[1]);
        HBaseWriter hbaseWriter2 = new HBaseWriter(tableNames[2]);
        Map<String, String> indsMap0 = new HashMap<String, String>();
        Map<String, String> indsMap1 = new HashMap<String, String>();
        Map<String, String> indsMap2 = new HashMap<String, String>();
        int lastDayStamp = ((int) (System.currentTimeMillis() / 1000) - 24 * 3600);
        int[] timeStamp = { lastDayStamp - 3600, lastDayStamp - 1800, lastDayStamp, lastDayStamp + 1800,
                1339603200 + 3600 };
        int[] userId = { 10, 20, 30, 40, 50 };
        int[] planId = { 120, 121, 122, 123, 124 };
        short[] dimOrAdId = { 0, 1, 2, 3, 4 };
        String[] dSrc = { "1\u00021\u0003男装\u00032", "1\u00021\u0003男装\u00032", "1\u00022\u0003女装\u00032",
                "2\u00023\u0003女装\u00034", "3\u00022\u0003女装\u00034" };
        Random rand = new Random();
        for (int line = 0; line < 10000; line++) {
            byte[] rowkeyA = new byte[46];
            byte[] rowkeyB = new byte[14];
            byte[] timeStamp1 = Bytes.toBytes(timeStamp[rand.nextInt(5)]);
            Bytes.putBytes(rowkeyB, 0, timeStamp1, 0, timeStamp1.length);
            byte[] userId1 = Bytes.toBytes(userId[rand.nextInt(5)]);
            Bytes.putBytes(rowkeyB, 4, userId1, 0, userId1.length);
            byte[] planId1 = Bytes.toBytes(planId[rand.nextInt(5)]);
            Bytes.putBytes(rowkeyB, 8, planId1, 0, planId1.length);
            byte[] dimOrAdId1 = Bytes.toBytes(dimOrAdId[rand.nextInt(5)]);
            Bytes.putBytes(rowkeyB, 12, dimOrAdId1, 0, dimOrAdId1.length);
            Bytes.putBytes(rowkeyA, 0, rowkeyB, 0, rowkeyB.length);
            String md5 = MD5Digest.encode(dSrc[rand.nextInt(5)]);
            byte[] md51 = Bytes.toBytes(md5);
            Bytes.putBytes(rowkeyA, 14, md51, 0, md51.length);
            indsMap0.clear();
            indsMap1.clear();
            indsMap2.clear();
            int[] indId0 = { 103, 104, 105, 200, 201, 202, 203, 204, 205, 206 };
            int[] indId1 = { 103, 104, 105, 106, 107, 200, 201, 202, 203, 204 };
            int[] indId2 = { 100, 101, 102, 108 };
            for (int j = 0; j < 4; j++) {
                String indIndex0 = "i" + indId0[rand.nextInt(10)];
                String indIndex1 = "i" + indId1[rand.nextInt(10)];
                String indIndex2 = "i" + indId2[rand.nextInt(4)];
                String indValue = String.valueOf(rand.nextInt(10000));
                indsMap0.put(indIndex0, indValue);
                indsMap1.put(indIndex1, indValue);
                indsMap2.put(indIndex2, indValue);
            }
            indsMap0.put("src", dSrc[rand.nextInt(5)]);
            hbaseWriter0.writeToTable(rowkeyA, indsMap0);
            hbaseWriter1.writeToTable(rowkeyB, indsMap1);
            hbaseWriter2.writeToTable(rowkeyB, indsMap2);
        }
        hbaseWriter0.flushAllTables();
        hbaseWriter1.flushAllTables();
        hbaseWriter2.flushAllTables();
        LOG.info("finish to write to hbase");
    }
}
