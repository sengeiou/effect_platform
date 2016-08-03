package com.ali.lz.effect.tools.data2mysql;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * 效果平台数据库访问类
 * 
 * @author jiuling.ypf
 */
public class MySQLWriter {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(MySQLWriter.class);

    // 字段分隔符
    private static final String CTRL_B = "\u0002";
    private static final String CTRL_C = "\u0003";

    // 实时和离线效果计算时的MySQL表名后缀
    private static final String[] tableNameSuffix = new String[] { "detail", "summary", "extend_ad", "summary_bysrc" };

    // MySQL表名
    private String tableName;

    private JDBCUtil jdbcUtil = null;

    private File flagFile;

    /**
     * 构造函数
     */
    public MySQLWriter(String tableName, JDBCUtil jdbcUtil) {
        this.tableName = tableName;
        this.jdbcUtil = jdbcUtil;
        this.flagFile = null;
    }

    /**
     * 构造函数
     */
    public MySQLWriter(String tableName, JDBCUtil jdbcUtil, File flagFile) {
        this.tableName = tableName;
        this.jdbcUtil = jdbcUtil;
        this.flagFile = flagFile;
    }

    private static class CalculateEntry {

        public String[] getP() {
            return p;
        }

        public Calculator getCal() {
            return cal;
        }

        private String[] p;
        private Calculator cal;

        public CalculateEntry(String[] p, Calculator cal) {
            this.p = p;
            this.cal = cal;
        }
    }

    private static class SimpleDivideCalculator implements Calculator {

        public Object calculate(Object[] objects) {

            byte[] o1 = (byte[]) objects[0];
            byte[] o2 = (byte[]) objects[1];

            if (o1 == null || o2 == null) {
                return 0;
            }

            double dividend = Double.parseDouble(Bytes.toString(o1));
            double divisor = Double.parseDouble(Bytes.toString(o2));

            if (divisor == 0 || dividend == 0) {
                return 0;
            }

            return (dividend / divisor);
        }
    }

    // 存储复杂指标需要根据计算出结果的指标对的映射
    private final static Map<String, CalculateEntry> complicateIndMap = new HashMap<String, CalculateEntry>();
    // 存储的指标为需要计算复杂指标的因子
    private final static Set<String> indSet = new HashSet<String>();

    static {
        complicateIndMap.put("i241", new CalculateEntry(new String[] { "i239", "i240" }, new SimpleDivideCalculator())); // 效果页类型：含单品页面
        // 平均访问深度
        complicateIndMap.put("i330", new CalculateEntry(new String[] { "i328", "i329" }, new SimpleDivideCalculator())); // 效果页类型：含店铺页面
        // 平均访问深度

        complicateIndMap.put("i111", new CalculateEntry(new String[] { "i109", "i110" }, new SimpleDivideCalculator())); // 效果页引导总流量
        // 平均访问深度

        for (CalculateEntry cal : complicateIndMap.values()) {
            String[] p = cal.getP();
            if (p != null) {
                for (String key : p) {
                    indSet.add(key);
                }
            }
        }
    }

    // 指标Map，存储<指标列名， 值>, 存储的指标为需要计算复杂指标的因子
    private Map<String, byte[]> indicatorMap = new HashMap<String, byte[]>();

    private void feedIndicatorMap(Map<byte[], byte[]> columnMap) {
        for (Entry<byte[], byte[]> entry : columnMap.entrySet()) {
            String value = Bytes.toString(entry.getKey());
            if (indSet.contains(value)) {
                indicatorMap.put(value, entry.getValue());
            }
        }
    }

    /**
     * 更新复杂指标，需要根据其他指标计算得出，比如访问深度=pv/uv
     */
    private Object getCompInd(CalculateEntry calEntry) {

        String[] p = calEntry.getP();

        Object[] paras = new Object[p.length];

        int i = 0;
        for (String s : p) {
            byte[] para = indicatorMap.get(s);
            paras[i] = para;
            ++i;
        }

        return calEntry.getCal().calculate(paras);
    }

    /**
     * 根据HBase中的一行记录生成插入MySQL的SQL语句
     * 
     * @param rowkey
     * @param columnMap
     * @return
     */
    private String generateSQL(byte[] rowkey, Map<byte[], byte[]> columnMap) {
        int day = Bytes.toInt(rowkey, 0);
        if (ConfigLoader.getWorkMode().equals("online")) { // XXX:
            // 实时模式下需要将UNIX时间戳格式转换为yyyyMMdd时间格式（此处省略一万字...）
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss");
            Date dayDate = new Date((long) day * 1000);
            String nextDayDateStr = formatter.format(dayDate);
            String nextDayStr = nextDayDateStr.substring(0, 8);
            day = Integer.parseInt(nextDayStr);
        }
        int planId = Bytes.toInt(rowkey, 8);
        short dimId = 0;
        String AdId = "";
        StringBuffer sqlPrefix = new StringBuffer();
        String sqlMiddle = null;
        StringBuffer sqlSuffix = new StringBuffer();
        StringBuffer valuePrefix = new StringBuffer();
        String valueMiddle = null;
        StringBuffer valueSuffix = new StringBuffer();

        boolean isEffectRT = false;

        if (ConfigLoader.getWorkMode().equals("offline")) // 离线工作模式
            sqlPrefix.append("INSERT IGNORE INTO " + tableName + "(plan_id,day,");
        else if (ConfigLoader.getWorkMode().equals("online")) // 实时工作模式
        {
            sqlPrefix.append("REPLACE INTO " + tableName + "(plan_id,day,");
            if (!tableName.endsWith(tableNameSuffix[2])) {
                isEffectRT = true;
                feedIndicatorMap(columnMap);
                for (String key : complicateIndMap.keySet()) {
                    // 将实时没有计算的复杂指标放入map，方便之后遍历
                    columnMap.put(Bytes.toBytes(key), Bytes.toBytes(0));
                }
            }
        } else
            // 加载配置信息出错
            return "";
        valuePrefix.append("VALUES(");
        valuePrefix.append(planId);
        valuePrefix.append(",");
        valuePrefix.append(day);
        valuePrefix.append(",");
        if (tableName.endsWith(tableNameSuffix[2])) { // rpt_extend_ad
            sqlPrefix.append("ad_id");
            AdId = Bytes.toString(rowkey, 12, rowkey.length - 12);
            valuePrefix.append("'" + AdId + "'");
        } else { // rpt_summary & rpt_detail & rpt_summary_bysrc
            sqlPrefix.append("dim");
            dimId = Bytes.toShort(rowkey, 12, 2);
            valuePrefix.append(dimId + 100);
        }

        if (tableName.endsWith(tableNameSuffix[3])) { // rpt_summary_bysrc
            sqlPrefix.append(",src_id");
            int srcId = Bytes.toInt(rowkey, 14);
            valuePrefix.append(",");
            valuePrefix.append(srcId);
            sqlPrefix.append(",path_id");
            int pathId = Bytes.toInt(rowkey, 18);
            valuePrefix.append(",");
            valuePrefix.append(pathId);
        }

        Iterator<byte[]> iter = columnMap.keySet().iterator();
        while (iter.hasNext()) {
            byte[] qualifier = (byte[]) iter.next();
            byte[] value = (byte[]) columnMap.get(qualifier);
            String qualifierStr = Bytes.toString(qualifier);
            if (qualifierStr.equalsIgnoreCase("src") && tableName.endsWith(tableNameSuffix[0])) { // rpt_detail
                String valueStr = Bytes.toString(value);
                String[] valueStrB = valueStr.split(CTRL_B);
                if (valueStrB.length == 2) { // 识别来源（包括外投广告和SPM）
                    int ruleId = Integer.parseInt(valueStrB[0]);
                    String[] valueStrC = valueStrB[1].split(CTRL_C);
                    if (ruleId < 10000) { // 普通规则
                        int srcId = Integer.parseInt(valueStrC[0]);
                        sqlMiddle = ",src_id,path_id";
                        valueMiddle = "," + srcId + "," + ruleId;
                    } else if (ruleId >= 10000 && ruleId < 20000) { // 外投广告规则
                        String srcExt = valueStrC[0];
                        sqlMiddle = ",src_id,src_ext,path_id";
                        valueMiddle = "," + "100,'" + srcExt + "'," + (ruleId - 10000); // 外投广告来源ID（100）
                    } else if (ruleId >= 20000 && ruleId < 30000) { // SPM规则
                        String srcExt = valueStrC[0];
                        sqlMiddle = ",src_id,src_ext,path_id";
                        valueMiddle = "," + "103,'" + srcExt + "'," + (ruleId - 20000); // SPM来源ID（103）
                    } else {
                        LOG.error("invalid ruleId (>= 30000): " + ruleId + " found");
                        return "";
                    }
                } else if (valueStrB.length == 1) { // 未知来源
                    int ruleId = Integer.parseInt(valueStrB[0]);
                    sqlMiddle = ",path_id";
                    valueMiddle = "," + ruleId;
                } else { // 非法数据
                    LOG.error("d:src format error");
                    return "";
                }
            } else if (qualifierStr.startsWith("i") && !qualifierStr.endsWith("b") && !qualifierStr.endsWith("d")
                    && !qualifierStr.endsWith("n")) {
                String indId = qualifierStr.substring(1, qualifierStr.length());
                if (!isEffectRT || !complicateIndMap.containsKey(qualifierStr)) {
                    String indVal = Bytes.toString(value);
                    indVal = ((indVal == null || indVal.equals("")) ? "0.0" : indVal);
                    sqlSuffix.append(",ind_" + indId);
                    valueSuffix.append("," + indVal);
                } else {
                    CalculateEntry calEntry = complicateIndMap.get(qualifierStr);
                    Object result = getCompInd(calEntry);
                    String indVal = String.valueOf(result);
                    sqlSuffix.append(",ind_" + indId);
                    valueSuffix.append("," + indVal);
                }
            }
        }

        if (tableName.endsWith(tableNameSuffix[0]) && sqlMiddle != null && valueMiddle != null) { // rpt_detail
            return sqlPrefix.toString() + sqlMiddle + sqlSuffix.toString() + ") " + valuePrefix.toString()
                    + valueMiddle + valueSuffix.toString() + ")";
        } else { // rpt_summary & rpt_extend_ad & rpt_summary_bysrc
            return sqlPrefix.toString() + sqlSuffix.toString() + ") " + valuePrefix.toString() + valueSuffix.toString()
                    + ")";
        }
    }

    /**
     * 将HBase中的计算结果导入效果平台MySQL报表中
     * 
     * @param hashMap
     * @return
     */
    public boolean insertRpt(final ConcurrentHashMap<byte[], Map<byte[], byte[]>> hashMap) {
        if (hashMap == null) {
            LOG.error("hashMap is null while calling insertRpt, terminate to insert mysql");
            return false;
        }
        Connection conn = jdbcUtil.getConnection(true);
        Statement stmt = null;
        if (ConfigLoader.getWorkMode().equals("offline") && ConfigLoader.getDataDate() != 0) { // 如果是离线工作模式且配置了日期选项，则插入前需要删除旧数据！
            int dataDate = ConfigLoader.getDataDate();
            try {
                stmt = conn.createStatement();
                stmt.executeUpdate("delete from " + tableName + " where day = " + dataDate);
            } catch (SQLException e) {
                if (flagFile != null)
                    flagFile.delete();
                e.printStackTrace();
                return false;
            } finally {
                jdbcUtil.closeStatement(stmt);
            }
        }
        final int ROW_COUNT = 1000;
        int i = 0;
        try {
            stmt = conn.createStatement();
            stmt.execute("SET sql_mode=''");
            conn.setAutoCommit(false);
            Iterator<byte[]> iter = hashMap.keySet().iterator();
            while (iter.hasNext()) {
                byte[] rowkey = (byte[]) iter.next();
                Map<byte[], byte[]> columnMap = (Map<byte[], byte[]>) hashMap.get(rowkey);
                String sql = generateSQL(rowkey, columnMap);
                // LOG.info("======" + sql);
                if (!sql.equals("")) {
                    stmt.addBatch(sql);
                    if (++i % ROW_COUNT == 0) { // 每ROW_COUNT条记录批量提交一次
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                    }
                }
            }
            // 最后将所有未提交的记录批量提交
            stmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
            LOG.info("insert table " + tableName + ", total records: " + i);
            return true;
        } catch (SQLException e) {
            if (flagFile != null)
                flagFile.delete();
            e.printStackTrace();
            LOG.error(e);
            try { // 批量更新失败，则进行回滚操作
                if (!conn.isClosed()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException e1) {
                if (flagFile != null)
                    flagFile.delete();
                e1.printStackTrace();
                LOG.error(e1);
            }
            return false;
        } finally { // 关闭数据库连接
            jdbcUtil.closeStatement(stmt);
            jdbcUtil.closeConnection(conn);
        }
    }

    public static void main(String[] args) {
        MySQLWriter mysqlWriter = new MySQLWriter("test_summary", new JDBCUtil());
        ConfigLoader.loadConf(ClassLoader.getSystemResource("data2mysql.properties").getPath());
        byte[] rowkey = Bytes.add(Bytes.toBytes(100000), Bytes.toBytes(1000),
                Bytes.add(Bytes.toBytes(99), Bytes.toBytes((short) 2)));

        Map<byte[], byte[]> columnMap = new HashMap<byte[], byte[]>();
        columnMap.put(Bytes.toBytes("i105"), Bytes.toBytes("5.0"));
        columnMap.put(Bytes.toBytes("i103"), Bytes.toBytes("100.0"));
        columnMap.put(Bytes.toBytes("i107"), Bytes.toBytes("0.01"));
        columnMap.put(Bytes.toBytes("i107d"), Bytes.toBytes("99"));
        columnMap.put(Bytes.toBytes("i107n"), Bytes.toBytes("100"));
        columnMap.put(Bytes.toBytes("i106"), Bytes.toBytes("10.0"));
        columnMap.put(Bytes.toBytes("i239"), Bytes.toBytes("5"));
        columnMap.put(Bytes.toBytes("i240"), Bytes.toBytes("10.0"));
        columnMap.put(Bytes.toBytes("i328"), Bytes.toBytes("9"));
        columnMap.put(Bytes.toBytes("i329"), Bytes.toBytes("100.0"));
        columnMap.put(Bytes.toBytes("i999"), Bytes.toBytes("99"));
        columnMap.put(Bytes.toBytes("i998"), Bytes.toBytes("88"));
        System.out.println(mysqlWriter.generateSQL(rowkey, columnMap));

    }

}
