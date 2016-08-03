package com.ali.lz.effect.tools.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloConfig.Ind;
import com.ali.lz.effect.tools.hbase.*;
import com.ali.lz.effect.utils.Constants;

public class RptToHBase {
    private static final Log log = LogFactory.getLog(RptToHBase.class);
    private static Map<Integer, Integer> indexMap;
    private static Map<Integer, Integer> adClickMap;
    private static Map<Integer, List<Integer>> indMap;
    private static HBaseWriter hbaseWriter;
    private static final int rptFields = 87;
    private static final int rptSumFields = 86;
    private static final int rptSumBySrcFields = 88;

    /**
     * 读取配置文件，生成plan_id与计算指标的对应表
     * 
     * @param path
     *            配置文件所在的目录
     * @return Map<Integer, List<Integer>>
     */
    private static Map<Integer, List<Integer>> readXML(String path, String date) {
        File file = new File(path);
        Map<Integer, List<Integer>> index = new HashMap<Integer, List<Integer>>();
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File xmlFile : files) {
                log.info("xmlFileName: " + xmlFile.getName());
                if (xmlFile.getName().substring(0, 6).equals("report")) {
                    HoloConfig config = new HoloConfig();
                    try {
                        config.loadFile(xmlFile.getAbsolutePath());
                    } catch (ParserConfigurationException e) {
                        e.printStackTrace();
                        log.error(e);
                    } catch (SAXException e) {
                        e.printStackTrace();
                        log.error(e);
                    } catch (IOException e) {
                        e.printStackTrace();
                        log.error(e);
                    } catch (HoloConfigParserException e) {
                        e.printStackTrace();
                        log.error(e);
                    }
                    int plan_id = config.plan_id;
                    int analyzer_id = config.analyzer_id;
                    // 写表之前先删除此表该日的数据
                    if (hbaseWriter.deleteBefore(date, String.valueOf(analyzer_id), String.valueOf(plan_id))) {
                        log.info("Success to delete " + date + " hbase table " + hbaseWriter.getTableName());
                    } else {
                        log.error("Failed to delete " + date + " hbase table " + hbaseWriter.getTableName());
                    }

                    List<Integer> inds = new ArrayList<Integer>();
                    for (Ind ind : config.getEffect()) {
                        inds.add(ind.ind_id);
                    }
                    index.put(plan_id, inds);
                    log.debug("plan_id: " + inds.toString());
                }
            }
        } else {
            log.error("The xmlPath should be a directory: " + path);
            System.out.println("STATUS : FAILD");
            System.exit(1);
        }
        return index;
    }

    /**
     * 读文件
     * 
     * @param fileName
     * @return BufferedReader reader
     */
    private static BufferedReader readFile(String fileName) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error(e);
        }
        return reader;
    }

    /**
     * 生成src字段的MD5值
     * 
     * @param s
     * @return byte[16]
     */
    private static byte[] getMD5(String s) {
        byte[] btInput = s.getBytes();
        MessageDigest mdInst = null;
        try {
            mdInst = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error(e);
        }
        mdInst.update(btInput);
        byte[] md = mdInst.digest();
        return md;
    }

    /**
     * 生成写入hbase的row_key
     * 
     * @param fields
     * @param tableName
     * @return byte[] rowkey
     */
    private static byte[] getRowKey(String[] fields, String tableName) {
        byte[] date_ts;
        byte[] analyzer_id;
        byte[] planid;
        byte[] dim_id;
        byte[] md5;
        byte[] src_id;
        byte[] path_id;
        byte[] ad_id;
        byte[] rowkey;
        if (tableName.equals("effect_rpt")) {
            rowkey = new byte[30];
            date_ts = Bytes.toBytes(Integer.parseInt(fields[2]));
            log.debug("date_ts:" + fields[2] + " bytes:" + date_ts.length);
            analyzer_id = Bytes.toBytes(Integer.parseInt(fields[1]));
            log.debug("analyzer_id:" + fields[1] + " bytes:" + analyzer_id.length);
            planid = Bytes.toBytes(Integer.parseInt(fields[0]));
            log.debug("planid:" + fields[0] + " bytes:" + planid.length);
            dim_id = Bytes.toBytes(Short.parseShort(fields[3]));
            log.debug("dim_id:" + fields[3] + " bytes:" + dim_id.length);
            md5 = getMD5(fields[rptFields - 1]);
            rowkey = Bytes.add(Bytes.add(date_ts, analyzer_id, planid), dim_id, md5);
        } else if (tableName.equals("effect_rpt_sum")) {
            rowkey = new byte[14];
            date_ts = Bytes.toBytes(Integer.parseInt(fields[2]));
            log.debug("date_ts:" + fields[2] + " bytes:" + date_ts.length);
            analyzer_id = Bytes.toBytes(Integer.parseInt(fields[1]));
            log.debug("analyzer_id:" + fields[1] + " bytes:" + analyzer_id.length);
            planid = Bytes.toBytes(Integer.parseInt(fields[0]));
            log.debug("planid:" + fields[0] + " bytes:" + planid.length);
            dim_id = Bytes.toBytes(Short.parseShort(fields[3]));
            log.debug("dim_id:" + fields[3] + " bytes:" + dim_id.length);
            rowkey = Bytes.add(Bytes.add(date_ts, analyzer_id, planid), dim_id);
        } else if (tableName.equals("effect_rpt_sum_bysrc")) {
            rowkey = new byte[22];
            date_ts = Bytes.toBytes(Integer.parseInt(fields[2]));
            log.debug("date_ts:" + fields[2] + " bytes:" + date_ts.length);
            analyzer_id = Bytes.toBytes(Integer.parseInt(fields[1]));
            log.debug("analyzer_id:" + fields[1] + " bytes:" + analyzer_id.length);
            planid = Bytes.toBytes(Integer.parseInt(fields[0]));
            log.debug("planid:" + fields[0] + " bytes:" + planid.length);
            dim_id = Bytes.toBytes(Short.parseShort(fields[3]));
            log.debug("dim_id:" + fields[3] + " bytes:" + dim_id.length);
            path_id = Bytes.toBytes(Integer.parseInt(fields[rptSumBySrcFields - 2]));
            src_id = Bytes.toBytes(Integer.parseInt(fields[rptSumBySrcFields - 1]));
            rowkey = Bytes.add(Bytes.add(Bytes.add(date_ts, analyzer_id, planid), dim_id, src_id), path_id);
        } else {
            date_ts = Bytes.toBytes(Integer.parseInt(fields[2]));
            log.debug("date_ts:" + fields[2] + " bytes:" + date_ts.length);
            analyzer_id = Bytes.toBytes(Integer.parseInt(fields[1]));
            log.debug("analyzer_id:" + fields[1] + " bytes:" + analyzer_id.length);
            planid = Bytes.toBytes(Integer.parseInt(fields[0]));
            log.debug("planid:" + fields[0] + " bytes:" + planid.length);
            ad_id = Bytes.toBytes(fields[3]);
            log.debug("ad_id:" + fields[3] + " bytes:" + ad_id.length);
            rowkey = Bytes.add(Bytes.add(date_ts, analyzer_id, planid), ad_id);
        }
        return rowkey;
    }

    /**
     * 
     * @param path
     * @param indMap
     * @param mirrorMap
     *            指标ID与结果表字段的对应关系表
     * @param tableName
     * @return
     */
    private static boolean toHBase(String fileName, Map<Integer, List<Integer>> indMap,
            Map<Integer, Integer> mirrorMap, String tableName) {
        BufferedReader reader = readFile(fileName);
        Map<String, String> indsMap = new HashMap<String, String>();
        String line = "";
        long totalRecords = 0, outPutRecords = 0;
        try {
            while ((line = reader.readLine()) != null) {
                totalRecords += 1;
                String[] fields = line.split(Constants.CTRL_A, -1);
                if ((tableName.equals("effect_rpt") && fields.length != rptFields)
                        || (tableName.equals("effect_rpt_sum") && fields.length != rptSumFields)
                        || (tableName.equals("effect_rpt_sum_bysrc") && fields.length != rptSumBySrcFields)
                        || (tableName.equals("effect_rpt_adclk") && fields.length != 6)) {
                    log.warn("Discard record (wrong fields): " + line.toString());
                    continue;
                }
                String plan_id = fields[0];
                List<Integer> inds = new ArrayList<Integer>();
                inds = indMap.get(Integer.parseInt(plan_id));

                if (inds == null) {
                    log.warn("plan_id 为 " + plan_id + " 时，没有要计算的指标");
                    continue;
                }
                for (Integer ind : inds) {
                    if ((tableName.equals("effect_rpt") || tableName.equals("effect_rpt_sum") || tableName
                            .equals("effect_rpt_sum_bysrc")) && (ind == 101 || ind == 108)) {
                        continue;
                    }
                    if (tableName.equals("effect_rpt_adclk") && ind != 101 && ind != 108) {
                        continue;
                    }
                    if (mirrorMap.get(ind) != null) {
                        String value = fields[mirrorMap.get(ind)];
                        indsMap.put("i" + String.valueOf(ind), value);
                    }
                }
                if (tableName.equals("effect_rpt")) {
                    indsMap.put("src", fields[rptFields - 1]);
                    log.debug("effect_rpt:src :" + fields[rptFields - 1]);
                }
                byte[] rowkey = getRowKey(fields, tableName);
                if (hbaseWriter.writeToTable(rowkey, indsMap)) {
                    outPutRecords += 1;
                    log.debug("SUCCESS: insert record into " + tableName.toString() + " : " + rowkey.toString() + "--"
                            + indsMap.toString());
                } else
                    log.error("FAILD: insert record into " + tableName.toString() + " : " + rowkey.toString() + "--"
                            + indsMap.toString());
            }
            hbaseWriter.flushAllTables();
            reader.close();
            log.info("table:" + tableName + " inputRecords:" + String.valueOf(totalRecords));
            log.info("table:" + tableName + " toHbaseRecords:" + String.valueOf(outPutRecords));
            if (totalRecords != outPutRecords) {
                log.error("输入的记录数和入到HBase中的记录数不一致");
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    log.error(e);
                }
            }
        }
        return true;
    }

    /**
     * 离线数据入HBase主程序
     * 
     * @param args
     *            输入参数四个。包括源数据地址、xml配置文件地址、表名("effect_rpt","effect_rpt_sum",
     *            "effect_rpt_sum_bysrc" "effect_rpt_adclk")、日期
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        try {
            if (args.length != 4) {
                System.out.println("Invalid number of arguments");
                System.out.println("Usage : RptToHBase [dataFileName] [XmlPath] [TableName] [date]");
                log.error("Invalid number of arguments   Usage : RptToHBase [dataFileName] [XmlPath] [TableName] [date]");
                System.exit(1);
            }
            String dataFileName = args[0];
            String xmlPath = args[1];
            String tableName = args[2];
            String date = args[3];
            log.debug("dataFileName: " + dataFileName + "||" + "xmlPath: " + xmlPath + "||tableName: " + tableName);
            hbaseWriter = new HBaseWriter(tableName);
            indMap = readXML(xmlPath, date);
            log.debug("indMap.toString: " + indMap.toString());
            indexMap = GenerateMap.indexMap;
            log.debug("indexMap.toString: " + indexMap.toString());
            adClickMap = GenerateMap.adClickMap;
            log.debug("indexMap.toString: " + adClickMap.toString());
            Map<Integer, Integer> mirrorMap = tableName.equals("effect_rpt_adclk") ? adClickMap : indexMap;
            log.debug("mirrorMap.toString: " + mirrorMap.toString());
            if (toHBase(dataFileName, indMap, mirrorMap, tableName)) {
                System.out.println("STATUS : SUCCESS: " + tableName + " to hbase");
                log.info("STATUS : SUCCESS: " + tableName + " to hbase");
                System.exit(0);
            } else {
                System.out.println("STATUS : FAILD: " + tableName + " to hbase");
                log.error("STATUS : FAILD: " + tableName + " to hbase");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            System.out.println("STATUS : FAILD");
            System.exit(1);
        }
    }

}
