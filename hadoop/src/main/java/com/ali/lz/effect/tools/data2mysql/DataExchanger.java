package com.ali.lz.effect.tools.data2mysql;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.Constants;
import com.ali.lz.effect.tools.util.JDBCUtil;

/**
 * 将HBase计算结果导入MySQL库中
 * 
 * @author jiuling.ypf
 * 
 */
public class DataExchanger {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(DataExchanger.class);

    /**
     * 构造函数
     */
    public DataExchanger() {

    }

    /**
     * 数据交换过程
     */
    public void exchangeData() {
        final long startTime = System.currentTimeMillis();
        LOG.info("begin to exchange data from HBase to MySQL");
        final String[] hbaseTableNames = (ConfigLoader.getWorkMode().equals("offline")) ? Constants.OFFLINE_HBASE_TABLE_NAMES
                : Constants.ONLINE_HBASE_TABLE_NAMES;
        final String[] mysqlTableNames = (ConfigLoader.getWorkMode().equals("offline")) ? Constants.OFFLINE_MYSQL_TABLE_NAMES
                : Constants.ONLINE_MYSQL_TABLE_NAMES;
        if (hbaseTableNames.length != mysqlTableNames.length) {
            LOG.error("hbase table number doesn't match mysql table number! DataExchangeer tool can't work!");
            return;
        }
        for (int i = 0; i < hbaseTableNames.length; i++) {
            Thread th = new Thread(String.valueOf(i)) {
                public void run() {
                    int index = Integer.parseInt(this.getName());
                    String hbaseTableName = hbaseTableNames[index];
                    String mysqlTableName = mysqlTableNames[index];
                    File flagFile = new File(hbaseTableName + ".exist");
                    if (flagFile.exists()) {
                        LOG.error("another process is exchanging data from HBase table " + hbaseTableName
                                + " into MySQL table " + mysqlTableName);
                    } else {
                        boolean success = false;
                        try {
                            success = flagFile.createNewFile();
                        } catch (IOException e) {
                            e.printStackTrace();
                            success = false;
                        }
                        if (success) {
                            LOG.info("start one process to exchange data from HBase table " + hbaseTableName
                                    + " into MySQL table " + mysqlTableName);
                            HBaseReader reader = new HBaseReader(hbaseTableName, flagFile);
                            MySQLWriter writer = new MySQLWriter(mysqlTableName, new JDBCUtil(), flagFile);
                            ConcurrentHashMap<byte[], Map<byte[], byte[]>> hbaseData = null;
                            if (ConfigLoader.getWorkMode().equals("offline"))
                                hbaseData = reader.getEffectData();
                            else
                                hbaseData = reader.getEffectDataRt();
                            boolean flag = writer.insertRpt(hbaseData);
                            if (flag) {
                                LOG.info("finish to exchange data from HBase table " + hbaseTableName
                                        + " into MySQL table " + mysqlTableName + ", total time: "
                                        + (System.currentTimeMillis() - startTime) + " ms");
                            } else {
                                LOG.error("fail to exchange data from HBase table " + hbaseTableName
                                        + " into MySQL table " + mysqlTableName + ", total time: "
                                        + (System.currentTimeMillis() - startTime) + " ms");
                            }
                            flagFile.delete();
                        } else {
                            LOG.error("fail to create .exist flag file: file already exists, or an IO error occurrs");
                        }
                    }
                }
            };
            th.start();
        }
    }

    /**
     * 主函数入口
     * 
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.out.println("Invalid number of parameters: 1 or 2 parameters!");
            LOG.error("Invalid number of parameters: 1 or 2 parameters!");
            System.out.println("Usage: DataExchanger [yyyyMMdd] [propertiesFilePath=data2mysql.properties]");
            LOG.error("Usage: DataExchanger [yyyyMMdd] [propertiesFilePath=data2mysql.properties]");
            System.exit(1);
        }
        int dataDate = 0;
        try {
            dataDate = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid parameter for [yyyyMMdd]: " + args[0]);
            LOG.error("Invalid parameter for [yyyyMMdd]: " + args[0]);
            System.exit(1);
        }
        ConfigLoader.loadDate(dataDate);
        if (args.length == 2) {
            ConfigLoader.loadConf(args[1]);
        } else {
            String propsFileName = ClassLoader.getSystemResource("data2mysql.properties").getFile();
            ConfigLoader.loadConf(propsFileName);
        }
        DataExchanger exchanger = new DataExchanger();
        exchanger.exchangeData();
    }

}
