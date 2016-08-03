package com.ali.lz.effect.holotree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.mortbay.log.Log;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import au.com.bytecode.opencsv.CSVReader;

import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloTree;
import com.ali.lz.effect.holotree.HoloTreeBuilder;
import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.holotree.SourceMeta;
import com.ali.lz.effect.holotree.URLMatcher;
import com.ali.lz.effect.holotree.GenericLoader.RawLogFmtDesc;
import com.ali.lz.effect.proto.StarLogProtos;
import com.ali.lz.effect.proto.StarLogProtos.FlowStarLog;
import com.clearspring.analytics.stream.cardinality.CountThenEstimate;
import com.flaptor.hist4j.AdaptiveHistogram;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;

public class MiscTestUtil {

    /**
     * 创建 HBase 本地集群
     * 
     * @return
     * @throws Exception
     */
    public static HBaseTestingUtility createLocalHBaseCluster() throws Exception {
        HBaseTestingUtility hbaseUtil = new HBaseTestingUtility();
        hbaseUtil.startMiniCluster();
        return hbaseUtil;
    }

    /**
     * 销毁 HBase 本地集群
     * 
     * @param hbaseUtil
     * @throws Exception
     */
    public static void shutdownLocalHBaseCluster(HBaseTestingUtility hbaseUtil) throws Exception {
        hbaseUtil.cleanupTestDir();
        hbaseUtil.shutdownMiniCluster();
    }

    /**
     * 将 HBase 本地集群的配置数据写入 hbase-site.xml 以供测试
     * 
     * @param hbaseUtil
     * @throws Exception
     */
    public static void writeLocalHBaseXml(HBaseTestingUtility hbaseUtil) throws Exception {
        String path = ClassLoader.getSystemResource("hbase-site.xml").getFile();
        OutputStream os = new FileOutputStream(path);
        hbaseUtil.getConfiguration().writeXml(os);
        os.close();
    }

    /**
     * 比对给定 HBase 表中内容是否符合预期，给定表中不允许出现预期数据以外的行
     * 
     * @param hbaseUtil
     * @param expPath
     * @param tab
     * @throws Exception
     */
    public static void assertHBaseContent(HBaseTestingUtility hbaseUtil, String expPath, HTable htable)
            throws Exception {
        loadHBaseDataAndAssert(hbaseUtil, expPath, htable, true);
    }

    /**
     * 比对给定 HBase 表中是否存在预期内容，给定表中可以出现预期数据以外的行
     * 
     * @param hbaseUtil
     * @param expPath
     * @param htable
     * @throws Exception
     */
    public static void assertHBaseContains(HBaseTestingUtility hbaseUtil, String expPath, HTable htable)
            throws Exception {
        loadHBaseDataAndAssert(hbaseUtil, expPath, htable, false);
    }

    /**
     * 加载 HBase 期望数据并对比
     * 
     * 期望数据结构为： rowkey: {family1: {col1: val1, ...}, family2: {col2: val2, ...},
     * ...}
     * 
     * @param hbaseUtil
     * @param expPath
     * @param htable
     * @param matchAll
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private static void loadHBaseDataAndAssert(HBaseTestingUtility hbaseUtil, String expPath, HTable htable,
            boolean matchAll) throws Exception {
        if (!expPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            expPath = ClassLoader.getSystemResource(expPath).getFile();
        }
        Yaml yaml = new Yaml();
        Map<String, Map<String, Map<String, Object>>> expData = (Map<String, Map<String, Map<String, Object>>>) yaml
                .load(new FileReader(expPath));

        int cnt = hbaseUtil.countRows(htable);
        if (matchAll) {
            assertEquals("incorrect hbase row number!", expData.size(), cnt);
        } else {
            assertTrue("not enough hbase row!", cnt >= expData.size());
        }

        for (Entry<String, Map<String, Map<String, Object>>> entry : expData.entrySet()) {
            String row = entry.getKey();
            Get getter = new Get(row.getBytes(Charset.forName("latin1")));
            Result recRs = htable.get(getter);
            assertTrue("hbase row not exist: " + row, !recRs.isEmpty());

            for (Entry<String, Map<String, Object>> familyEntry : entry.getValue().entrySet()) {
                String family = familyEntry.getKey();

                Map<byte[], byte[]> cols = recRs.getFamilyMap(Bytes.toBytes(family));
                for (Entry<String, Object> expCol : familyEntry.getValue().entrySet()) {
                    String col = expCol.getKey();
                    byte[] val = cols.get(Bytes.toBytes(col));
                    assertNotNull(expPath + ": column value is null", val);
                    assertEquals(expPath + ": column value incorrect", String.valueOf(expCol.getValue()),
                            Bytes.toString(val));
                }
            }
        }
    }

    /**
     * 将给定 HBase 表中所有内容转为 YAML 格式输出
     * 
     * @param htable
     */
    public static void dumpHBaseToYaml(HTable htable) {
        dumpHBaseToYamlExcludes(htable, new String[] {});
    }

    public static void dumpHBaseToYaml(HTable htable, Writer output) {
        dumpHBaseToYamlExcludes(htable, new String[] {}, output);
    }

    public static void dumpHBaseToYamlExcludes(HTable htable, String[] exclCols) {
        try {
            Set<String> exclColSet = new HashSet<String>();
            for (int i = 0; i < exclCols.length; i++) {
                exclColSet.add(exclCols[i]);
            }

            Scan scan = new Scan();
            ResultScanner rs = htable.getScanner(scan);
            Map<String, Map<String, Map<String, Object>>> map = new TreeMap<String, Map<String, Map<String, Object>>>();
            for (Result r : rs) {
                String row = new String(r.getRow(), Charset.forName("latin1"));
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> fcols = r.getNoVersionMap();

                Map<String, Map<String, Object>> fmap = new TreeMap<String, Map<String, Object>>();
                for (Entry<byte[], NavigableMap<byte[], byte[]>> fcol : fcols.entrySet()) {
                    String family = Bytes.toString(fcol.getKey());

                    Map<String, Object> cmap = new TreeMap<String, Object>();
                    for (Entry<byte[], byte[]> cols : fcol.getValue().entrySet()) {
                        String col = Bytes.toString(cols.getKey());
                        if (exclColSet.contains(family + ":" + col)) {
                            continue;
                        }
                        String val = Bytes.toString(cols.getValue());
                        cmap.put(col, val);
                    }
                    fmap.put(family, cmap);
                }
                map.put(row, fmap);
            }

            DumperOptions opts = new DumperOptions();
            opts.setAllowUnicode(false);
            opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
            Yaml yaml = new Yaml(new MyRepresenter(), opts);
            System.out.println(yaml.dump(map));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dumpHBaseToYamlExcludes(HTable htable, String[] exclCols, Writer output) {
        try {
            Set<String> exclColSet = new HashSet<String>();
            for (int i = 0; i < exclCols.length; i++) {
                exclColSet.add(exclCols[i]);
            }

            Scan scan = new Scan();
            ResultScanner rs = htable.getScanner(scan);
            Map<String, Map<String, Map<String, Object>>> map = new TreeMap<String, Map<String, Map<String, Object>>>();
            for (Result r : rs) {
                String row = new String(r.getRow(), Charset.forName("latin1"));
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> fcols = r.getNoVersionMap();

                Map<String, Map<String, Object>> fmap = new TreeMap<String, Map<String, Object>>();
                for (Entry<byte[], NavigableMap<byte[], byte[]>> fcol : fcols.entrySet()) {
                    String family = Bytes.toString(fcol.getKey());

                    Map<String, Object> cmap = new TreeMap<String, Object>();
                    for (Entry<byte[], byte[]> cols : fcol.getValue().entrySet()) {
                        String col = Bytes.toString(cols.getKey());
                        if (exclColSet.contains(family + ":" + col)) {
                            continue;
                        }
                        String val = Bytes.toString(cols.getValue());
                        cmap.put(col, val);
                    }
                    fmap.put(family, cmap);
                }
                map.put(row, fmap);
            }

            DumperOptions opts = new DumperOptions();
            opts.setAllowUnicode(false);
            opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
            Yaml yaml = new Yaml(new MyRepresenter(), opts);
            yaml.dump(map, output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 加载用于测试的日志数据，并创建对应的 PTLogEntry 对象
     * 
     * 日志数据为CSV格式
     * 
     * @param resPath
     * @return
     */
    public static List<PTLogEntry> loadLogFile(String resPath) throws Exception {
        if (!resPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            resPath = ClassLoader.getSystemResource(resPath).getFile();
        }

        List<PTLogEntry> res = new LinkedList<PTLogEntry>();
        CSVReader reader = new CSVReader(new FileReader(resPath));
        String[] f = reader.readNext();
        List<String[]> logLines = reader.readAll();
        reader.close();

        for (int i = 0; i < f.length; i++) {
            f[i] = f[i].trim();
        }

        for (String[] l : logLines) {
            for (int i = 0; i < l.length; i++) {
                l[i] = l[i].trim();
            }

            PTLogEntry logEntry = new PTLogEntry();
            for (int i = 0; i < f.length; i++) {
                if (f[i].equalsIgnoreCase("rtype")) {
                    logEntry.setRType(Integer.parseInt(l[i]));
                } else if (f[i].equalsIgnoreCase("ptype")) {
                    logEntry.setPType(Integer.parseInt(l[i]));
                } else if (f[i].equalsIgnoreCase("ts")) {
                    logEntry.put("ts", Long.parseLong(l[i]));
                } else {
                    logEntry.put(f[i], l[i]);
                }
            }
            res.add(logEntry);
        }

        return res;
    }

    /**
     * 测试数据加载为ProtoBuf格式
     * 
     * @param logPath
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.FlowStarLog> loadTestDataToPB(String logPath) throws Exception {
        // 灵石产生测试数据的^A分隔字段顺序
        final String testFields[] = { "log_version", "log_time", "ts", "url", "refer_url", "uid_mid", "shopid",
                "auctionid", "ip", "mid", "uid", "sid", "aid", "agent", "adid", "amid", "cmid", "pmid", "nmid", "nuid",
                "channelid" };
        Map<Integer, String> idxToPb = new HashMap<Integer, String>();
        for (int i = 0; i < testFields.length; i++) {
            idxToPb.put(i, testFields[i]);
        }
        RawLogFmtDesc fmtDesc = new RawLogFmtDesc("\001", "utf-8");
        return GenericLoader.genericLoadTestDataToPB(logPath, fmtDesc, idxToPb);
    }

    public interface LoadingListener {
        void onReadBrowser(StarLogProtos.FlowStarLog log);

        void onReadBusiness(StarLogProtos.BusinessStarLog log);
    };

    public static int loadBrowserDataPB(String logPath, LoadingListener listener) throws FileNotFoundException {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }
        StarLogProtos.FlowStarLog.Builder builder = StarLogProtos.FlowStarLog.newBuilder();
        List<FieldDescriptor> field = StarLogProtos.FlowStarLog.getDescriptor().getFields();
        // builder.

        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        while (scanner.hasNextLine()) {
            builder.clear();
            String line = scanner.nextLine();
            String l[] = line.split("\001");

            int i = 0;
            for (FieldDescriptor currField : field) {
                if (currField.getJavaType() == FieldDescriptor.JavaType.STRING) {
                    builder.setField(currField, l[i++]);
                } else if (currField.getJavaType() == FieldDescriptor.JavaType.INT) {
                    builder.setField(currField, Integer.parseInt(l[i++]));
                } else if (currField.getJavaType() == FieldDescriptor.JavaType.LONG) {
                    builder.setField(currField, Long.parseLong(l[i++]));
                } else {
                    i++; // Noreached!
                }
            }
            if (i > 0) {
                listener.onReadBrowser(builder.build());
            }
        }
        return 0;
    }

    public static int loadBusinessDataPB(String logPath, LoadingListener listener) throws FileNotFoundException {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }
        StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
        List<FieldDescriptor> field = StarLogProtos.BusinessStarLog.getDescriptor().getFields();

        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        while (scanner.hasNextLine()) {
            builder.clear();
            String line = scanner.nextLine();
            String l[] = line.split("\001");
            int i = 0;
            for (FieldDescriptor currField : field) {
                if (l.length < 24 && currField.getNumber() == 19) { // actual_total_fee
                    continue;
                }
                if (currField.getJavaType() == FieldDescriptor.JavaType.STRING) {
                    builder.setField(currField, l[i++]);
                } else if (currField.getJavaType() == FieldDescriptor.JavaType.INT) {
                    builder.setField(currField, Integer.parseInt(l[i++]));
                } else if (currField.getJavaType() == FieldDescriptor.JavaType.LONG) {
                    builder.setField(currField, Long.parseLong(l[i++]));
                } else {
                    i++; // Noreached!
                }
                if (i >= l.length)
                    break;
            }
            if (i > 0) {
                listener.onReadBusiness(builder.build());
            }
        }
        return 0;
    }

    /**
     * 测试数据加载为ProtoBuf格式，Business日志。
     * 
     * @param logPath
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.BusinessStarLog> loadBizTestDataToPB(String logPath) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
        List<StarLogProtos.BusinessStarLog> res = new LinkedList<StarLogProtos.BusinessStarLog>();
        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            while (scanner.hasNextLine()) {
                builder.clear();
                String line = scanner.nextLine();
                String l[] = line.split("\001");
                builder.setLogVersion(0); // version
                builder.setLogSrc("");
                builder.setOrderId(l[2]); // order_id
                builder.setParentId(l[3]); // parent_id
                builder.setBuyerId(l[4]); // user_id
                builder.setAuctionId(l[5]); // auction_id
                builder.setShopId(l[6]); // shop_id
                builder.setDiscountFee(0);
                builder.setAdjustFee(0);
                long gmvts = Long.parseLong(l[10]); // gmv_time_stamp
                long alits = Long.parseLong(l[14]); // alipay_time_stamp
                long uxts = (gmvts < 1300000000 ? alits : gmvts) * 1000L;
                builder.setOrderModifiedT(uxts);
                if (l[11].equals("0")) { // alipay_trade_num
                    builder.setPayStatus(1); // 未付款
                    builder.setIsPay(0);
                    // builder.setActualTotalFee(Long.parseLong(l[8])); //
                    // gmv_trade_amt
                    // builder.setPayTime("");
                    long num = Long.parseLong(l[9]); // gmv_auction_num
                    builder.setAuctionPrice(Long.parseLong(l[8]) / num); // gmv_trade_amt
                    builder.setBuyAmount(num);
                    // long ts = Long.parseLong(l[10]) * 1000; // gmv_time_stamp
                    // builder.setGmtCreate(formatter.format(new Date(ts)));
                    builder.setGmtCreate(formatter.format(new Date(uxts)));
                } else {
                    builder.setPayStatus(2); // 已付款
                    builder.setIsPay(1);
                    long num = Long.parseLong(l[13]); // alipay_auction_num
                    builder.setAuctionPrice(Long.parseLong(l[12]) / num); // alipay_trade_amt
                    builder.setBuyAmount(num);
                    builder.setActualTotalFee(Long.parseLong(l[12])); // alipay_trade_amt
                    // long ts = Long.parseLong(l[14]) * 1000; //
                    // alipay_time_stamp
                    // builder.setPayTime(formatter.format(new Date(ts)));
                    builder.setPayTime(formatter.format(new Date(uxts)));
                    // builder.setGmtCreate(""); // gmv_time_stamp
                }

                StarLogProtos.BusinessStarLog logEntry = builder.build();
                res.add(logEntry);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            scanner.close();
        }
        return res;
    }

    /**
     * s_dw_sh_etao_pay_detail表加载为ProtoBuf格式。
     * 
     * @param logPath
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.BusinessStarLog> loadExernalBizTestDataToPB(String logPath) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
        List<StarLogProtos.BusinessStarLog> res = new LinkedList<StarLogProtos.BusinessStarLog>();
        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        /**
         * <pre>
         * 0     thedate        string          日期
         * 1     buyer_id       string          回传卖家id
         * 2     buyer_email    string          回传卖家email
         * 3     seller_id      string          回传卖家id
         * 4     seller_email   string          回传卖家email
         * 5     trade_type     string          订单类型 1.S 支付宝担保交易 2.FP 快速支付 3.COD 货到付款
         * 6     nick_name      string          卖家名称
         * 7     channel_id     string          频道id（trade_track_info 按‘_’分隔第一个字段）
         * 8     act_id string          活动id（trade_track_info 按‘_’分隔前两个字段）
         * 9     item_id        string          回传商品id（即trade_track_info）
         * 10    gmt_create     string          创建时间
         * 11    gmt_payment    string          付款时间
         * 12    gmt_refund     string          退款时间
         * 13    is_order       string          是否订单
         * 14    is_pay string          是否付款
         * 15    is_refund      string          是否为退款订单
         * 16    refund_status  string          退款状态
         * 17    total_fee      string          总金额
         * 18    pid    string  外键    订单id
         * 19    com_name       string          商家公司全称
         * 20    gmt_create_ori string          下单时间（精确到时分秒）
         * 21    gmt_payment_ori        string          成交时间（精确到时分秒）
         * 22    gmt_refund_ori string          退款时间（精确到时分秒）
         * 23    trade_no       string          订单号
         * 24    auction_id     string          外部商品id
         * 25    trade_track_info       string          订单跟踪id
         * </pre>
         */
        try {
            while (scanner.hasNextLine()) {
                builder.clear();
                String line = scanner.nextLine();
                String l[] = line.split("\001");
                builder.setBuyerId(l[1]);
                builder.setSellerId(l[3]);
                builder.setGmtCreate(l[20]);
                if (l[14].equals("y")) {
                    builder.setIsPay(1);
                    builder.setPayStatus(2);
                    // builder.setPayTime(l[20]);
                } else {
                    builder.setIsPay(0);
                    builder.setPayStatus(1);
                    // builder.setGmtCreate(l[20]);
                }
                builder.setActualTotalFee(Long.parseLong(l[17]));
                builder.setTradeNo(l[23]);
                builder.setTradeTrackInfo(l[25]);
                builder.setAuctionPrice(Long.parseLong(l[17]));
                builder.setBuyAmount(1);
                builder.setPayTime(l[20]);
                StarLogProtos.BusinessStarLog logEntry = builder.build();
                res.add(logEntry);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            scanner.close();
        }
        return res;
    }

    /**
     * s_dw_sh_etao_pay_detail表加载为ProtoBuf格式。
     * 
     * @param logPath
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.BusinessStarLog> loadBizTestDataToPB2(String logPath) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
        List<StarLogProtos.BusinessStarLog> res = new LinkedList<StarLogProtos.BusinessStarLog>();
        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            /**
             * <pre>
             * [0]  gmv_trade_timestamp  bigint comment '拍下时间戳，到秒',
             * [1]  shop_id              bigint comment '店铺ID',
             * [2]  auction_id           bigint comment '宝贝ID',
             * [3]  user_id              bigint comment '买家ID',
             * [4]  ali_corp             bigint comment '成交店铺所在网站类型（0 未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算）',
             * [5]  gmv_trade_num        bigint comment '拍下笔数',
             * [6]  gmv_trade_amt        double comment '拍下金额',
             * [7]  gmv_auction_num      bigint comment '拍下件数',
             * [8]  alipay_trade_num     bigint comment '成交笔数',
             * [9]  alipay_trade_amt     double comment '成交金额',
             * [10] alipay_auction_num   bigint comment '成交件数',
             * [11] useful_extra
             * </pre>
             */
            while (scanner.hasNextLine()) {
                builder.clear();
                String line = scanner.nextLine();
                String l[] = line.split("\001");

                builder.setShopId(l[1]);
                builder.setAuctionId(l[2]);
                builder.setBuyerId(l[3]);
                long ts = Long.parseLong(l[0]);
                int buyAmount = Integer.parseInt(l[10]);
                if (buyAmount > 0) { // 已付款
                    builder.setIsPay(1);
                    builder.setPayStatus(2);
                    long totalFee = Long.parseLong(l[9]);
                    builder.setActualTotalFee(totalFee);
                    builder.setAuctionPrice(totalFee / buyAmount);
                    builder.setBuyAmount(buyAmount);
                    builder.setPayTime(formatter.format(new Date(ts * 1000L)));
                } else {
                    builder.setIsPay(0);
                    builder.setPayStatus(1);
                    long totalFee = Long.parseLong(l[6]);
                    int num = Integer.parseInt(l[7]);
                    builder.setAuctionPrice(num == 0 ? 0 : totalFee / num);
                    builder.setBuyAmount(num);
                    builder.setGmtCreate(formatter.format(new Date(ts * 1000L)));
                }
                StarLogProtos.BusinessStarLog logEntry = builder.build();
                res.add(logEntry);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            scanner.close();
        }
        return res;
    }

    public static List<StarLogProtos.FlowStarLog> loadTestDataToPB2(String logPath) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        StarLogProtos.FlowStarLog.Builder builder = StarLogProtos.FlowStarLog.newBuilder();
        List<StarLogProtos.FlowStarLog> res = new LinkedList<StarLogProtos.FlowStarLog>();
        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");
        try {
            /**
             * <pre>
             * [0] time_stamp           bigint comment '访问时间戳，到秒',
             * [1] url                  string comment 'url',
             * [2] refer_url            string comment 'refer_url',
             * [3] shop_id              string comment '店铺ID：如果当前访问为店铺内页面，则填写。否则为空字符串',
             * [4] auction_id           string comment '宝贝ID: 如果当前访问为宝贝页面，则填写。否则为空字符串',
             * [5] user_id              string comment '访客ID',
             * [6] cookie               string comment 'cookie用来标识一个访问用户（atpanle日志中用mid）',
             * [7] session              string comment '一次会话的标识（atpanle日志中用sid）',
             * [8] cookie2              string comment '用来计算uv使用（atpanle日志中用mid_uid, 其他情况直接使用cookie）',
             * [9] useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且?>??属计算中需要? 用字? ',
             * [10] extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
             * </pre>
             */
            while (scanner.hasNextLine()) {
                builder.clear();
                String line = scanner.nextLine();
                String l[] = line.split("\001");
                long ts = Long.parseLong(l[0]) * 1000L; // 毫秒
                builder.setTs(ts);
                builder.setUrl(l[1]);
                builder.setReferUrl(l[2]);
                builder.setShopid(l[3]);
                builder.setAuctionid(l[4]);
                String uid = l[5], mid = l[6];
                builder.setUid(uid);
                builder.setMid(mid);
                builder.setSid(l[7]);
                builder.setUidMid(l[8]);
                StarLogProtos.FlowStarLog logEntry = builder.build();
                res.add(logEntry);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            scanner.close();
        }
        return res;
    }

    /**
     * 在云端数据加载为ProtoBuf格式，Business日志。
     * 
     * @param logPath
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.BusinessStarLog> loadCloudBizLogToPB(String logPath) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
        List<StarLogProtos.BusinessStarLog> res = new LinkedList<StarLogProtos.BusinessStarLog>();
        Scanner scanner = new Scanner(new FileInputStream(logPath), "utf-8");

        try {
            while (scanner.hasNextLine()) {
                builder.clear();
                String line = scanner.nextLine();
                String l[] = line.split("\001");
                builder.setLogVersion(1); // version
                builder.setLogSrc("tc_biz_order"); // log_src
                builder.setDestType("biz_order"); // dest_type
                builder.setOrderId(l[2]); // order_id
                builder.setParentId(l[3]); // parent_id
                builder.setBuyerId(l[4]); // user_id
                builder.setAuctionId(l[5]); // auction_id
                builder.setShopId(l[6]); // shop_id
                builder.setDiscountFee(0);
                builder.setAdjustFee(0);
                builder.setGmtCreate(l[10]);
                builder.setAuctionPrice(Long.parseLong(l[8]) / Long.parseLong(l[9])); // gmv_trade_amt
                builder.setGmtCreate(l[10]); // gmv_time_stamp
                builder.setActualTotalFee(Long.parseLong(l[12])); // alipay_trade_amt
                builder.setPayTime(l[14]); // alipay_time_stamp
                if (l[11].equals("0")) { // alipay_trade_num
                    builder.setPayStatus(1); // 未付款
                    builder.setIsPay(0);
                    builder.setBuyAmount(Long.parseLong(l[9])); // gmv_auction_num
                } else {
                    builder.setPayStatus(2); // 已付款
                    builder.setIsPay(1);
                    builder.setBuyAmount(Long.parseLong(l[13])); // alipay_auction_num
                }
                long uxtm1 = Long.parseLong(l[10]);
                long uxtm2 = Long.parseLong(l[14]);
                builder.setOrderModifiedT((uxtm1 >= uxtm2 ? uxtm1 : uxtm2) * 1000L); // 毫秒

                StarLogProtos.BusinessStarLog logEntry = builder.build();
                res.add(logEntry);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            scanner.close();
        }
        return res;
    }

    /**
     * 全息森林对比断言
     * 
     * @param expPath
     * @param forest
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static void assertForestEqualsFile(String expPath, List<HoloTree> forest) throws Exception {
        if (!expPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            expPath = ClassLoader.getSystemResource(expPath).getFile();
        }

        Yaml yaml = new Yaml();
        List<List<Map<String, Object>>> expForest = (List<List<Map<String, Object>>>) yaml
                .load(new FileReader(expPath));
        assertEquals("total number of trees not match!", expForest.size(), forest.size());

        for (List<Map<String, Object>> expTree : expForest) {
            Map<Integer, Object> tmpMap = new HashMap<Integer, Object>();

            // 校验预期数据格式正确性
            for (Map<String, Object> node : expTree) {
                if (!(node.containsKey("id") && node.containsKey("p"))) {
                    fail("invalid expected tree data: missing required property 'id' or 'p'!");
                }
                Integer id = (Integer) node.get("id");
                if (tmpMap.containsKey(id)) {
                    fail("invalid expected tree data: 'id' is not unique: " + id);
                }
                tmpMap.put(id, node);
            }

            // 重建预期数据的树关系
            for (Map<String, Object> node : expTree) {
                Integer p = (Integer) node.get("p");

                if (p != -1 && !tmpMap.containsKey(p)) {
                    fail("invalid expected tree data: 'p' refer to invalid node: " + p);
                }

                if (!node.containsKey("_children")) {
                    node.put("_children", new LinkedList<Object>());
                }

                if (p != -1) {
                    Map<String, Object> pNode = (Map<String, Object>) tmpMap.get(p);
                    List<Object> children = (LinkedList<Object>) pNode.get("_children");
                    children.add(node);
                    node.put("_parent", pNode);
                } else {
                    node.put("_parent", null);
                }
            }
        }

        // 对比预期森林和给定森林
        for (int i = 0; i < expForest.size(); i++) {
            HoloTree tree = forest.get(i);
            List<Map<String, Object>> expTree = expForest.get(i);

            assertEquals("tree node number doesn't match!", expTree.size(), tree.size());

            int cnt = 0;
            for (Entry<Long, HoloTreeNode> entry : tree.entrySet()) {
                Map<String, Object> expNode = expTree.get(cnt);
                HoloTreeNode node = entry.getValue();

                // 树结构验证
                assertEquals("node children number doesn't match!", ((List<Object>) expNode.get("_children")).size(),
                        node.getChildren().size());
                assertTrue("node parent doesn't match!", (expNode.get("_parent") == null && node.getParent() == null)
                        || (expNode.get("_parent") != null && node.getParent() != null));

                // 属性验证
                if (expNode.containsKey("srp")) {
                    String srp = (String) expNode.get("srp");
                    assertEquals("serial root path doesn't match!", srp, node.getSerialRootPath());
                }
                if (expNode.containsKey("prp")) {
                    String prp = (String) expNode.get("prp");
                    assertEquals("ptype root path doesn't match!", prp, node.getPTypeRootPath());
                }
                if (expNode.containsKey("ep")) {
                    Boolean ep = (Boolean) expNode.get("ep");
                    assertEquals("isEffectPage doesn't match!", ep, node.isEffectPage());
                }
                if (expNode.containsKey("src")) {
                    Map<String, Map<String, Object>> expSrc = (Map<String, Map<String, Object>>) expNode.get("src");
                    Map<String, SourceMeta> src = node.getSources();

                    // 对期望来源集合为空进行验证
                    assertEquals("source emptiness doesn't match!", expSrc.isEmpty(), src.isEmpty());

                    // 验证给定来源标识存在
                    for (String srcId : expSrc.keySet()) {
                        assertTrue("expected source doesn't exist: " + srcId, src.containsKey(srcId));

                        Map<String, Object> expSrcMeta = expSrc.get(srcId);
                        SourceMeta srm = src.get(srcId);
                        if (expSrcMeta.containsKey("fts")) {
                            // yaml 反序列化后的整数类型会根据具体取值选择 Integer 或
                            // Long，故此处需要先判断实际类型再转换
                            Object val = expSrcMeta.get("fts");
                            long efts = -1;
                            if (val.getClass().equals(Integer.class)) {
                                efts = (Integer) val;
                            } else if (val.getClass().equals(Long.class)) {
                                efts = (Long) val;
                            } else {
                                fail("invalid value type of firstOpTS for source " + srcId);
                            }
                            assertEquals("firstOpTS for source " + srcId + " doesn't match!", efts * 1000, // NOTE:
                                    // Ts改为毫秒。
                                    srm.getFirstOpTS());
                        }
                        if (expSrcMeta.containsKey("lts")) {
                            // yaml 反序列化后的整数类型会根据具体取值选择 Integer 或
                            // Long，故此处需要先判断实际类型再转换
                            Object val = expSrcMeta.get("lts");
                            long elts = -1;
                            if (val.getClass().equals(Integer.class)) {
                                elts = (Integer) val;
                            } else if (val.getClass().equals(Long.class)) {
                                elts = (Long) val;
                            } else {
                                fail("invalid value type of lastOpTS for source " + srcId);
                            }
                            assertEquals("lastOpTS for source " + srcId + " doesn't match!", elts * 1000, // NOTE:
                                    // Ts改为毫秒。
                                    srm.getLastOpTS());
                        }
                        if (expSrcMeta.containsKey("pri")) {
                            int epri = (Integer) expSrcMeta.get("pri");
                            assertEquals("priority for source " + srcId + " doesn't match!", epri, srm.getPriority());
                        }

                        Object[][] pairs = { { "fep", "firstEP", srm.getFirstEP() },
                                { "lep", "lastEP", srm.getLastEP() } };
                        for (Object[] p : pairs) {
                            String k = (String) p[0];
                            String msg = (String) p[1];
                            HoloTreeNode ep = (HoloTreeNode) p[2];

                            if (expSrcMeta.containsKey(k)) {
                                Object val = expSrcMeta.get(k);
                                if (val == null) {
                                    assertEquals(msg + " for source " + srcId + " doesn't match!", val, ep);
                                } else {
                                    assertNotNull(msg + " for source " + srcId + " doesn't match!", ep);

                                    Map<String, Object> expEp = (Map<String, Object>) val;
                                    if (expEp.containsKey("srp")) {
                                        String srp = (String) expEp.get("srp");
                                        assertEquals("serial root path of " + msg + " for source " + srcId
                                                + " doesn't match!", srp, ep.getSerialRootPath());
                                    }
                                    if (expEp.containsKey("prp")) {
                                        String prp = (String) expEp.get("prp");
                                        assertEquals("ptype root path of " + msg + " for source " + srcId
                                                + " doesn't match!", prp, ep.getPTypeRootPath());
                                    }
                                }
                            }
                        }
                    }
                }

                cnt++;
            }
        }
    }

    /**
     * 自定义CSV建树数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @throws Exception
     */
    public static void treeBuilderDriver(String confPath, String logPath, String expPath) throws Exception {
        confPath = ClassLoader.getSystemResource(confPath).getFile();
        logPath = ClassLoader.getSystemResource(logPath).getFile();

        HoloConfig conf = new HoloConfig(confPath);
        HoloTreeBuilder htb = new HoloTreeBuilder(conf);
        List<PTLogEntry> logs = loadLogFile(logPath);
        for (PTLogEntry logEntry : logs) {
            htb.appendLog(logEntry);
        }
        List<HoloTree> forest = htb.getCurrentTrees();

        if (expPath != null) {
            expPath = ClassLoader.getSystemResource(expPath).getFile();
            assertForestEqualsFile(expPath, forest);
        } else {
            dumpHoloForestToYaml(forest);
        }
    }

    /**
     * 测试数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @throws Exception
     */
    public static void testDriver(String confPath, String logPath, String expPath) throws Exception {
        testDriver(confPath, logPath, expPath, true);
    }

    /**
     * 测试数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @param doPathMatch
     * @throws Exception
     */
    public static void testDriver(String confPath, String logPath, String expPath, boolean doPathMatch)
            throws Exception {
        confPath = ClassLoader.getSystemResource(confPath).getFile();
        logPath = ClassLoader.getSystemResource(logPath).getFile();

        HoloConfig conf = new HoloConfig(confPath);
        URLMatcher um = new URLMatcher(conf);
        HoloTreeBuilder htb = new HoloTreeBuilder(conf);
        htb.setDoPathMatch(doPathMatch);

        List<StarLogProtos.FlowStarLog> logs = loadTestDataToPB(logPath);
        for (StarLogProtos.FlowStarLog log : logs) {
            PTLogEntry logEntry = um.grep(log);
            htb.appendLog(logEntry);
        }
        List<HoloTree> forest = htb.getCurrentTrees();

        if (expPath != null) {
            expPath = ClassLoader.getSystemResource(expPath).getFile();
            assertForestEqualsFile(expPath, forest);
        } else {
            dumpHoloForestToYaml(forest);
        }
    }

    /**
     * 测试数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @throws Exception
     */
    public static void testDriver2(String confPath, String logPath, String expPath) throws Exception {
        confPath = ClassLoader.getSystemResource(confPath).getFile();
        logPath = ClassLoader.getSystemResource(logPath).getFile();

        HoloConfig conf = new HoloConfig(confPath);
        URLMatcher um = new URLMatcher(conf);
        HoloTreeBuilder htb = new HoloTreeBuilder(conf);

        List<StarLogProtos.FlowStarLog> logs = loadTestDataToPB2(logPath);
        for (StarLogProtos.FlowStarLog log : logs) {
            PTLogEntry logEntry = um.grep(log);
            htb.appendLog(logEntry);
        }
        List<HoloTree> forest = htb.getCurrentTrees();

        if (expPath != null) {
            expPath = ClassLoader.getSystemResource(expPath).getFile();
            assertForestEqualsFile(expPath, forest);
        } else {
            dumpHoloForestToYaml(forest);
        }
    }

    /**
     * 通用测试数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @param fmtDesc
     * @param idxToPb
     * @throws Exception
     */
    public static void testDriverEx(String confPath, String logPath, String expPath, GenericLoader.LogFmtDesc fmtDesc,
            Map<Integer, String> idxToPb) throws Exception {
        testDriverEx(confPath, logPath, expPath, fmtDesc, idxToPb, true);

    }

    /**
     * 通用测试数据驱动控制台
     * 
     * @param confPath
     * @param logPath
     * @param expPath
     * @param fmtDesc
     * @param idxToPb
     * @param doPathMatch
     * @throws Exception
     */
    public static void testDriverEx(String confPath, String logPath, String expPath, GenericLoader.LogFmtDesc fmtDesc,
            Map<Integer, String> idxToPb, boolean doPathMatch) throws Exception {
        confPath = ClassLoader.getSystemResource(confPath).getFile();
        logPath = ClassLoader.getSystemResource(logPath).getFile();

        HoloConfig conf = new HoloConfig(confPath);
        URLMatcher um = new URLMatcher(conf);
        HoloTreeBuilder htb = new HoloTreeBuilder(conf);
        htb.setDoPathMatch(doPathMatch);

        List<StarLogProtos.FlowStarLog> logs = GenericLoader.genericLoadTestDataToPB(logPath, fmtDesc, idxToPb);

        // 按照时戳从小到大排序以保证建树过程正常
        Collections.sort(logs, new Comparator<StarLogProtos.FlowStarLog>() {
            @Override
            public int compare(FlowStarLog o1, FlowStarLog o2) {
                return (int) (o1.getTs() - o2.getTs());
            }
        });

        for (StarLogProtos.FlowStarLog log : logs) {
            PTLogEntry logEntry = um.grep(log);
            htb.appendLog(logEntry);
        }
        List<HoloTree> forest = htb.getCurrentTrees();

        if (expPath != null) {
            expPath = ClassLoader.getSystemResource(expPath).getFile();
            assertForestEqualsFile(expPath, forest);
        } else {
            dumpHoloForestToYaml(forest);
        }
    }

    /**
     * 将给定的全息森林转换为 YAML 格式输出，以便进行测试数据固化
     * 
     * @param forest
     */
    public static void dumpHoloForestToYaml(List<HoloTree> forest) {
        DumperOptions opts = new DumperOptions();
        opts.setAllowUnicode(false);
        opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);

        Yaml yaml = new Yaml(new MyRepresenter(), opts);
        List<List<Map<String, Object>>> out = new LinkedList<List<Map<String, Object>>>();
        AdaptiveHistogram sh = new AdaptiveHistogram();

        for (HoloTree tree : forest) {
            List<Map<String, Object>> outTree = new LinkedList<Map<String, Object>>();

            sh.addValue(tree.size());

            for (Entry<Long, HoloTreeNode> entry : tree.entrySet()) {
                Map<String, Object> outNode = new HashMap<String, Object>();

                HoloTreeNode node = entry.getValue();
                String srp = node.getSerialRootPath();
                String[] sfs = srp.split("\\.");
                Integer id = Integer.parseInt(sfs[sfs.length - 1]);
                Integer p = -1;
                if (sfs.length > 1) {
                    // 序列root path多于一段，不是根节点
                    p = Integer.parseInt(sfs[sfs.length - 2]);
                }
                outNode.put("id", id);
                long ts = (Long) node.getPtLogEntry().get("ts");
                // XXX: dump 时将毫秒级时戳统一转换为秒级时戳
                outNode.put("ts", ts > 1300000000000L ? ts / 1000L : ts);
                outNode.put("p", p);
                outNode.put("srp", srp);

                String prp = node.getPTypeRootPath();
                outNode.put("prp", prp);

                Boolean ep = node.isEffectPage();
                outNode.put("ep", ep);

                Map<String, Map<String, Object>> src = new HashMap<String, Map<String, Object>>();
                Map<String, SourceMeta> srcMeta = node.getSources();
                for (Entry<String, SourceMeta> metaEntry : srcMeta.entrySet()) {
                    String srcId = metaEntry.getKey();
                    SourceMeta meta = metaEntry.getValue();

                    Map<String, Object> sm = new HashMap<String, Object>();
                    long fts = meta.getFirstOpTS();
                    long lts = meta.getLastOpTS();
                    // XXX: dump 时将毫秒级时戳统一转换为秒级时戳
                    sm.put("fts", fts > 1300000000000L ? fts / 1000L : fts);
                    sm.put("lts", lts > 1300000000000L ? lts / 1000L : lts);
                    sm.put("pri", meta.getPriority());

                    HoloTreeNode fep = meta.getFirstEP();
                    if (fep != null) {
                        Map<String, Object> fepm = new HashMap<String, Object>();
                        String fsrp = fep.getSerialRootPath();
                        String fprp = fep.getPTypeRootPath();
                        fepm.put("srp", fsrp);
                        fepm.put("prp", fprp);
                        sm.put("fep", fepm);
                    } else {
                        sm.put("fep", null);
                    }

                    HoloTreeNode lep = meta.getLastEP();
                    if (lep != null) {
                        Map<String, Object> lepm = new HashMap<String, Object>();
                        String lsrp = lep.getSerialRootPath();
                        String lprp = lep.getPTypeRootPath();
                        lepm.put("srp", lsrp);
                        lepm.put("prp", lprp);
                        sm.put("lep", lepm);
                    } else {
                        sm.put("lep", null);
                    }

                    src.put(srcId, sm);
                }
                outNode.put("src", src);

                outNode.put("url", node.getPtLogEntry().get("url"));
                outNode.put("refer_url", node.getPtLogEntry().get("refer_url"));
                outNode.put("ptype", node.getPtLogEntry().getPType());
                outNode.put("rtype", node.getPtLogEntry().getRType());

                outTree.add(outNode);
            }

            out.add(outTree);
        }

        System.out.println(yaml.dump(out));

        // 输出全息树大小统计信息
        System.out.println("# total tree no: " + forest.size());
        System.out.println("# tree size histogram:");
        long tcnt = 0;
        long prev = 0;
        for (int v = 1; v <= 10; v++) {
            long cnt = sh.getAccumCount(v);
            System.out.printf("#	tree size == %d: %d\n", v, cnt - prev);
            tcnt += cnt - prev;
            prev = cnt;
        }
        System.out.printf("#    tree size > 10: %d\n", forest.size() - tcnt);
    }

    public static int sizeOfObject(Object obj) throws Exception {
        Serializable ser = (Serializable) obj;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(ser);
        oos.close();
        return baos.size();
    }

    /**
     * 自定义 yaml 字符串序列化实现，避免默认实现将来源标识表达为 Base64 编码串
     * 
     * @author wxz
     * 
     */
    protected static class MyRepresenter extends Representer {
        public MyRepresenter() {
            this.representers.put(String.class, new RepresentString());
        }

        private class RepresentString implements Represent {
            @Override
            public Node representData(Object data) {
                Tag tag = Tag.STR;
                String value = data.toString();
                return representScalar(tag, value);
            }
        }
    }

    /**
     * 加载 Yaml文件
     * 
     * 期望数据结构为：rowkey: {family1: {col1: val1, ...}, family2: {col2: val2, ...},
     * ...}
     * 
     * @param expPath
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map<byte[], Map<byte[], Map<byte[], byte[]>>> loadYamlData(String expPath) {
        if (!expPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            expPath = ClassLoader.getSystemResource(expPath).getFile();
        }
        Yaml yaml = new Yaml();
        Map<byte[], Map<byte[], Map<byte[], byte[]>>> expData = null;
        try {
            expData = (Map<byte[], Map<byte[], Map<byte[], byte[]>>>) yaml.load(new FileReader(expPath));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            expData = null;
        }
        return expData;
    }

    public static List<StarLogProtos.FlowStarLog> loadYamlDataToBroswerLog(String expPath)
            throws InvalidProtocolBufferException {
        List<StarLogProtos.FlowStarLog> logs = new LinkedList<StarLogProtos.FlowStarLog>();
        Map<byte[], Map<byte[], Map<byte[], byte[]>>> expData = loadYamlData(expPath);
        for (Map<byte[], Map<byte[], byte[]>> lay1 : expData.values()) {
            for (Map<byte[], byte[]> lay2 : lay1.values()) {
                for (byte[] raw : lay2.values()) {
                    StarLogProtos.FlowStarLog log = StarLogProtos.FlowStarLog.parseFrom(raw);
                    logs.add(log);
                }
            }
        }
        return logs;
    }

    /**
     * 加载 Yaml文件后写入HBase
     * 
     * 期望数据结构为：rowkey: {family1: {col1: val1, ...}, family2: {col2: val2, ...},
     * ...}
     * 
     * @param expPath
     * @param hTable
     * @return
     */
    public static void loadYamlDataToHBase(String expPath, HTable hTable) {
        Map<byte[], Map<byte[], Map<byte[], byte[]>>> expData = loadYamlData(expPath);
        List<Put> putList = new ArrayList<Put>();
        if (expData != null) {
            for (Entry<byte[], Map<byte[], Map<byte[], byte[]>>> r : expData.entrySet()) {
                byte[] row = r.getKey();
                Put put = new Put(row);
                Map<byte[], Map<byte[], byte[]>> fcols = r.getValue();
                for (Entry<byte[], Map<byte[], byte[]>> fcol : fcols.entrySet()) {
                    byte[] family = fcol.getKey();
                    for (Entry<byte[], byte[]> cols : fcol.getValue().entrySet()) {
                        byte[] col = cols.getKey();
                        byte[] val = cols.getValue();
                        put.add(family, col, val);
                    }
                }
                putList.add(put);
            }
            try {
                hTable.put(putList);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * 将给定 HBase 表中所有内容（byte[]）转为 YAML 格式输出
     * 
     * @param rs
     */
    public static void dumpHBaseBytesToYaml(ResultScanner rs, String yamlPath) {
        dumpHBaseBytesToYamlExcludes(rs, new String[] {}, yamlPath);
    }

    /**
     * 将给定 HBase 表中所有内容（byte[]）转为 YAML 格式输出
     * 
     * @param rs
     * @param exclCols
     * @param yamlPath
     */
    public static void dumpHBaseBytesToYamlExcludes(ResultScanner rs, String[] exclCols, String yamlPath) {
        try {
            Set<byte[]> exclColSet = new HashSet<byte[]>();
            for (int i = 0; i < exclCols.length; i++) {
                exclColSet.add(Bytes.toBytes(exclCols[i]));
            }
            int num = 0;
            Map<byte[], Map<byte[], Map<byte[], byte[]>>> map = new HashMap<byte[], Map<byte[], Map<byte[], byte[]>>>();
            for (Result r : rs) {
                num++;
                byte[] row = r.getRow();
                int ts = Bytes.toInt(row, 1, 4);
                Log.info("######timestamp: " + ts + ", ######toString: "
                        + new DateTime((long) ts * 1000).toString("yyyy-MM-dd HH:mm:ss.SSS"));
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> fcols = r.getNoVersionMap();

                Map<byte[], Map<byte[], byte[]>> fmap = new HashMap<byte[], Map<byte[], byte[]>>();
                for (Entry<byte[], NavigableMap<byte[], byte[]>> fcol : fcols.entrySet()) {
                    byte[] family = fcol.getKey();

                    Map<byte[], byte[]> cmap = new HashMap<byte[], byte[]>();
                    for (Entry<byte[], byte[]> cols : fcol.getValue().entrySet()) {
                        byte[] col = cols.getKey();
                        if (exclColSet.contains(Bytes.add(family, Bytes.toBytes(":"), col))) {
                            continue;
                        }
                        byte[] val = cols.getValue();
                        cmap.put(col, val);
                    }
                    fmap.put(family, cmap);
                }
                map.put(row, fmap);
            }
            rs.close();
            Log.info("######total count: " + num);

            DumperOptions opts = new DumperOptions();
            opts.setAllowUnicode(false);
            opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
            Yaml yaml = new Yaml(opts);
            int index = yamlPath.lastIndexOf(File.separatorChar);
            String dirPath = yamlPath.substring(0, index);
            File dir = new File(dirPath);
            if (!dir.isDirectory()) { // 创建目录
                dir.mkdirs();
            }
            FileWriter fw = new FileWriter(yamlPath);
            fw.write(yaml.dump(map));
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将给定 HBase 表导出后的结果转为 YAML 格式输出
     * 
     * @param rs
     * @param yamlPath
     */
    public static void dumpHBaseBytesToYaml(Map<byte[], Map<byte[], Map<byte[], byte[]>>> map, String yamlPath) {
        try {
            DumperOptions opts = new DumperOptions();
            opts.setAllowUnicode(false);
            opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
            opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
            Yaml yaml = new Yaml(opts);
            int index = yamlPath.lastIndexOf(File.separatorChar);
            if (index != -1) {
                String dirPath = yamlPath.substring(0, index);
                File dir = new File(dirPath);
                if (!dir.isDirectory()) { // 创建必要的目录
                    dir.mkdirs();
                }
            }
            FileWriter fw = new FileWriter(yamlPath);
            fw.write(yaml.dump(map));
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // -------------------------------------------------------------------------
    private static void printResultRow(ArrayList<Object> line) {
        System.out.print("{");
        for (Object value : line) {
            if (value == null) {
                System.out.print("null, ");
                continue;
            }
            if (value instanceof Double) {
                System.out.print(value + ", ");
            } else if (value instanceof ArrayList<?>) {
                @SuppressWarnings("unchecked")
                ArrayList<Double> fract = ((ArrayList<Double>) value);
                if (fract.get(0).equals(fract.get(1)))
                    System.out.print("null, ");
                else
                    System.out.print(1.0 - fract.get(0) / fract.get(1) + ", ");
            } else if (value instanceof byte[]) {
                try {
                    CountThenEstimate uv = new CountThenEstimate((byte[]) value);
                    System.out.print(uv.cardinality() + ", ");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        System.out.println("},");
    }

    public static Map<String, ArrayList<ArrayList<Object>>> decodeResult(
            Map<String, ArrayList<ArrayList<Object>>> result) {
        Map<String, ArrayList<ArrayList<Object>>> rv = new HashMap<String, ArrayList<ArrayList<Object>>>();
        for (Map.Entry<String, ArrayList<ArrayList<Object>>> pair : result.entrySet()) {
            ArrayList<ArrayList<Object>> copied_ind = new ArrayList<ArrayList<Object>>();
            for (ArrayList<Object> ind : pair.getValue()) {
                ArrayList<Object> copied_dim = new ArrayList<Object>();
                for (Object dim : ind) {
                    if (dim == null || dim instanceof Double) {
                        copied_dim.add(dim);
                    } else if (dim instanceof ArrayList<?>) {
                        @SuppressWarnings("unchecked")
                        List<Double> fract = (ArrayList<Double>) dim;
                        if (fract.get(0).equals(fract.get(1)))
                            copied_dim.add(null);
                        else
                            copied_dim.add(1.0 - fract.get(0) / fract.get(1));
                    } else if (dim instanceof byte[]) {
                        try {
                            CountThenEstimate uv = new CountThenEstimate((byte[]) dim);
                            int value = ((Long) uv.cardinality()).intValue();
                            copied_dim.add(value == 0 ? null : value);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                copied_ind.add(copied_dim);
            }
            rv.put(pair.getKey(), copied_ind);
        }
        return rv;
    }
}
