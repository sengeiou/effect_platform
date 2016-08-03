package com.ali.lz.effect.tools.data2mysql;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.Constants;
import com.ali.lz.effect.tools.util.TimeUtil;

/**
 * 
 * @author jiuling.ypf HBase读取类
 * 
 */
public class HBaseReader {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(HBaseReader.class);

    // HBase表名
    private String tableName;

    // 效果平台的HBase表的列簇名
    private static final byte[] columnFamily = Bytes.toBytes(Constants.HBASE_FAMILY_NAME);

    // HBase客户端配置对象
    private static final Configuration conf = HBaseConfiguration.create();

    // HTable客户端对象
    private HTable rTable;

    private File flagFile;

    // 构造函数
    public HBaseReader(String tableName) {
        this.tableName = tableName;
        this.flagFile = null;
        initHTables();
    }

    // 构造函数
    public HBaseReader(String tableName, File flagFile) {
        this.tableName = tableName;
        this.flagFile = flagFile;
        initHTables();
    }

    /**
     * 离线报表使用
     * 
     * @return
     */
    public ConcurrentHashMap<byte[], Map<byte[], byte[]>> getEffectData() {
        long startS = System.currentTimeMillis();
        ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = new ConcurrentHashMap<byte[], Map<byte[], byte[]>>();
        ResultScanner rs = null;
        Result rr = new Result();
        Scan scan = new Scan();
        scan.addFamily(columnFamily);
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^(src|i\\d+)$"));
        scan.setFilter(filter);
        byte[] startKey = new byte[4];
        byte[] endKey = new byte[4];
        if (ConfigLoader.getDataDate() != 0) { // 如果配置文件中设置了日期选项，则查询指定日期的数据，并插入数据库
            int dataDate = ConfigLoader.getDataDate();
            int nextDate = TimeUtil.getNextday(dataDate);
            Bytes.putInt(startKey, 0, dataDate);
            Bytes.putInt(endKey, 0, nextDate);
        } else { // 否则默认时查询昨天的数据，然后直接插入数据库
            Bytes.putInt(startKey, 0, TimeUtil.getLastday());
            Bytes.putInt(endKey, 0, TimeUtil.getToday());
        }
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        try {
            rs = rTable.getScanner(scan);
            if (rs != null) {
                rr = rs.next();
            } else {
                LOG.error("get scanner error, null ResultScanner object");
                return hashMap;
            }
        } catch (IOException e) {
            if (flagFile != null)
                flagFile.delete();
            LOG.error(e);
            e.printStackTrace();
        }

        int num = 0;
        while (rr != null) {
            num++;
            if (!rr.isEmpty()) {
                byte[] rowkey = rr.getRow();
                NavigableMap<byte[], byte[]> qualifierMap = rr.getFamilyMap(columnFamily);
                hashMap.put(rowkey, qualifierMap);
            }
            try {
                rr = rs.next();
            } catch (IOException e) {
                if (flagFile != null)
                    flagFile.delete();
                LOG.error("get result error, e=" + e.getLocalizedMessage());
            }
        }

        LOG.info("time of scan table " + tableName + ": " + (System.currentTimeMillis() - startS)
                + " ms, total records: " + num);
        return hashMap;
    }

    /**
     * 实时报表使用
     * 
     * @return
     */
    public ConcurrentHashMap<byte[], Map<byte[], byte[]>> getEffectDataRt() {
        long startS = System.currentTimeMillis();
        ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap = new ConcurrentHashMap<byte[], Map<byte[], byte[]>>();
        ResultScanner rs = null;
        Result rr = new Result();
        Scan scan = new Scan();
        scan.addFamily(columnFamily);
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^(src|i\\d+)$"));
        scan.setFilter(filter);
        byte[] startKey = new byte[4];
        byte[] endKey = new byte[4];
        if (ConfigLoader.getDataDate() != 0) { // 如果配置文件中设置了日期选项，则查询指定日期的数据，并插入数据库
            int dataDate = ConfigLoader.getDataDate();
            int todayTs = TimeUtil.getDayTs(dataDate);
            int nextdayTs = TimeUtil.getNextdayTs(dataDate);
            Bytes.putInt(startKey, 0, todayTs);
            Bytes.putInt(endKey, 0, nextdayTs);
        } else { // 否则默认时查询昨天的数据，然后直接插入数据库
            int lastDay = TimeUtil.getLastday();
            int today = TimeUtil.getToday();
            Bytes.putInt(startKey, 0, TimeUtil.getDayTs(lastDay));
            Bytes.putInt(endKey, 0, TimeUtil.getDayTs(today));
        }
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        try {
            rs = rTable.getScanner(scan);
            if (rs != null) {
                rr = rs.next();
            } else {
                LOG.error("get scanner error, null ResultScanner object");
                return hashMap;
            }
        } catch (IOException e) {
            if (flagFile != null)
                flagFile.delete();
            LOG.error(e);
            e.printStackTrace();
        }

        int num = 0;
        while (rr != null) {
            num++;
            if (!rr.isEmpty()) {
                byte[] rowkey = rr.getRow();
                NavigableMap<byte[], byte[]> qualifierMap = rr.getFamilyMap(columnFamily);
                hashMap.put(rowkey, qualifierMap);
            }
            try {
                rr = rs.next();
            } catch (IOException e) {
                if (flagFile != null)
                    flagFile.delete();
                LOG.error("get result error, e=" + e.getLocalizedMessage());
            }
        }

        LOG.info("time of scan table " + tableName + ": " + (System.currentTimeMillis() - startS)
                + " ms, total records: " + num);
        return hashMap;
    }

    /**
     * 初始化客户端HTable对象，用户从HBase读取数据
     * 
     * @return
     */
    private boolean initHTables() {
        boolean b = true;
        try {
            rTable = new HTable(conf, tableName);
            rTable.setScannerCaching(ConfigLoader.getScannerCache());
        } catch (IOException e) {
            if (flagFile != null)
                flagFile.delete();
            LOG.error("initHTables error, e=" + e.getLocalizedMessage());
            b = false;
        }
        return b;
    }

    /**
     * 测试函数
     * 
     * @param args
     */
    public static void main(String args[]) {
        HBaseReader hbaseReader0 = new HBaseReader("effect_rpt");
        HBaseReader hbaseReader1 = new HBaseReader("effect_rpt_sum");
        HBaseReader hbaseReader2 = new HBaseReader("effect_rpt_adclk");
        ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap0 = hbaseReader0.getEffectData();
        ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap1 = hbaseReader1.getEffectData();
        ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap2 = hbaseReader2.getEffectData();
        LOG.info("effect_rpt hashmap size: " + hashMap0.size());
        LOG.info("effect_rpt_sum hashmap size: " + hashMap1.size());
        LOG.info("effect_rpt_adclk hashmap size: " + hashMap2.size());
    }

}
