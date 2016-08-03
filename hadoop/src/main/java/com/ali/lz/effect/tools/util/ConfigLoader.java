package com.ali.lz.effect.tools.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * tool工具的配置加载类
 * 
 * @author jiuling.ypf
 * 
 */
public class ConfigLoader {

    private static final Log LOG = LogFactory.getLog(ConfigLoader.class);

    // //////////////////////////////////////////////////////
    // ////// config2xml和data2mysql的JDBC相关配置 ////////
    // //////////////////////////////////////////////////////

    /**
     * MySQL master/slave server ip, splited by comma symbol
     */
    private static String mysql_server_ip = "127.0.0.1,127.0.0.1";

    /**
     * MySQL master/slave server port
     */
    private static int mysql_server_port = 3306;

    /**
     * MySQL database name
     */
    private static String mysql_db_name = "test";

    /**
     * MySQL database user name
     */
    private static String mysql_db_user = "root";

    /**
     * MySQL database user password
     */
    private static String mysql_db_pass = "";

    /**
     * Tool work mode: online/offline
     */
    private static String work_mode = "offline";

    // //////////////////////////////////////////////////////
    // ////////// data2mysql相关配置 ////////////
    // //////////////////////////////////////////////////////

    /**
     * Concurrent HTable number for writing
     */
    private static int write_thread_num = 5;

    /**
     * HBase client write buffer size in mega bytes
     */
    private static int write_buffer_size = 1024 * 1024;

    /**
     * HBase client flush interval in milliseconds
     */
    private static int flush_interval = 1000;

    /**
     * Concurrent HTable number for reading
     */
    private static int read_thread_num = 5;

    /**
     * HBase client scanner caching record number
     */
    private static int scanner_cache = 500;

    /**
     * HBase Region MemStore size in mega bytes
     */
    private static long mem_flush_size = 64 * 1024 * 1024;

    /**
     * HBase table time to live in days
     */
    private static int time_to_live = 90 * 24 * 60 * 60;

    /**
     * Whether to reset table
     */
    private static boolean reset_table = false;

    /**
     * Process data of the specified date
     */
    private static int data_date = 0;

    // //////////////////////////////////////////////////////
    // ////////// config2xml相关配置 ////////////
    // //////////////////////////////////////////////////////

    /**
     * XML output directory prefix
     */
    private static String xml_output_dir_prefix = "/home/lz/effect_platform/conf/report_rt/";

    /**
     * XML output file format
     */
    private static String xml_output_file_format = "report_*.xml";

    /**
     * XML output directory prefix
     */
    private static String xml_offline_dir_prefix = "/home/lz/effect_platform/conf/report/";

    /**
     * XML output file format
     */
    private static String xml_offline_file_format = "report_*.xml";

    /**
     * XML output directory prefix
     */
    private static String xml_overdue_dir_prefix = "/home/lz/effect_platform/conf/overdue/";

    /**
     * XML output file format
     */
    private static String xml_overdue_file_format = "report_*.xml";

    /**
     * 加载日期，用于从HBase中读取指定日期的数据
     * 
     * @param dataDate
     *            格式：yyyyMMdd
     */
    public static void loadDate(int dataDate) {
        if (dataDate > 0) {
            data_date = dataDate;
            LOG.info("config value data_date = " + data_date);
        }
    }

    /**
     * 加载配置文件
     * 
     * @param fileName
     * @return
     */
    public static boolean loadConf(String fileName) {
        boolean b = true;

        Properties p = new Properties();
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                LOG.error("config file: " + fileName + " not exists");
                return false;
            }
            LOG.info("load config file: " + file.getAbsolutePath());
            FileReader fr = new FileReader(file);
            p.load(fr);
        } catch (IOException e) {
            e.printStackTrace();
            b = false;
        }

        try {
            if (!p.isEmpty()) {
                if (p.get("jdbc.mysql_server_ip") != null) {
                    mysql_server_ip = p.get("jdbc.mysql_server_ip").toString().trim();
                    LOG.info("config value jdbc.mysql_server_ip = " + mysql_server_ip);
                }
                if (p.get("jdbc.mysql_server_port") != null) {
                    mysql_server_port = Integer.parseInt(p.get("jdbc.mysql_server_port").toString().trim());
                    LOG.info("config value jdbc.mysql_server_port = " + mysql_server_port);
                }
                if (p.get("jdbc.mysql_db_name") != null) {
                    mysql_db_name = p.get("jdbc.mysql_db_name").toString().trim();
                    LOG.info("config value jdbc.mysql_db_name = " + mysql_db_name);
                }
                if (p.get("jdbc.mysql_db_user") != null) {
                    mysql_db_user = p.get("jdbc.mysql_db_user").toString().trim();
                    LOG.info("config value jdbc.mysql_db_user = " + mysql_db_user);
                }
                if (p.get("jdbc.mysql_db_pass") != null) {
                    mysql_db_pass = p.get("jdbc.mysql_db_pass").toString().trim();
                    LOG.info("config value jdbc.mysql_db_pass = " + mysql_db_pass);
                }
                if (p.get("data2mysql.write_thread_num") != null) {
                    write_thread_num = Integer.parseInt(p.get("data2mysql.write_thread_num").toString().trim());
                    LOG.info("config value data2mysql.write_thread_num = " + write_thread_num);
                }
                if (p.get("data2mysql.write_buffer_size") != null) {
                    write_buffer_size = Integer.parseInt(p.get("data2mysql.write_buffer_size").toString().trim());
                    write_buffer_size = write_buffer_size * 1024 * 1024;
                    LOG.info("config value data2mysql.write_buffer_size = " + write_buffer_size);
                }
                if (p.get("data2mysql.flush_interval") != null) {
                    flush_interval = Integer.parseInt(p.get("data2mysql.flush_interval").toString().trim());
                    LOG.info("config value data2mysql.flush_interval = " + flush_interval);
                }
                if (p.get("data2mysql.read_thread_num") != null) {
                    read_thread_num = Integer.parseInt(p.get("data2mysql.read_thread_num").toString().trim());
                    LOG.info("config value data2mysql.read_thread_num = " + read_thread_num);
                }
                if (p.get("data2mysql.scanner_cache") != null) {
                    scanner_cache = Integer.parseInt(p.get("data2mysql.scanner_cache").toString().trim());
                    LOG.info("config value data2mysql.scanner_cache = " + scanner_cache);
                }
                if (p.get("data2mysql.mem_flush_size") != null) {
                    mem_flush_size = Integer.parseInt(p.get("data2mysql.mem_flush_size").toString().trim());
                    LOG.info("config value data2mysql.mem_flush_size = " + mem_flush_size);
                    mem_flush_size = mem_flush_size * 1024 * 1024;
                }
                if (p.get("data2mysql.time_to_live") != null) {
                    time_to_live = Integer.parseInt(p.get("data2mysql.time_to_live").toString().trim());
                    time_to_live = time_to_live * 24 * 60 * 60;
                    LOG.info("config value data2mysql.time_to_live = " + time_to_live);
                }
                if (p.get("data2mysql.reset_table") != null) {
                    reset_table = Boolean.parseBoolean(p.get("data2mysql.reset_table").toString().trim());
                    LOG.info("config value data2mysql.reset_table = " + reset_table);
                }
                if (p.get("tool.work_mode") != null) {
                    work_mode = p.get("tool.work_mode").toString().trim();
                    if (!work_mode.equals("offline") && !work_mode.equals("online")) {
                        work_mode = "offline";
                        LOG.warn("config value tool.work_mode must be offline/online, change to default value");
                    }
                    LOG.info("config value tool.work_mode = " + work_mode);
                }
                if (p.get("config2xml.xml_output_dir_prefix") != null) {
                    xml_output_dir_prefix = p.get("config2xml.xml_output_dir_prefix").toString().trim();
                    LOG.info("config value config2xml.xml_output_dir_prefix = " + xml_output_dir_prefix);
                }
                if (p.get("config2xml.xml_output_file_format") != null) {
                    xml_output_file_format = p.get("config2xml.xml_output_file_format").toString().trim();
                    LOG.info("config value config2xml.xml_output_file_format = " + xml_output_file_format);
                }
                if (p.get("config2xml.xml_offline_dir_prefix") != null) {
                    xml_offline_dir_prefix = p.get("config2xml.xml_offline_dir_prefix").toString().trim();
                    LOG.info("config value config2xml.xml_offline_dir_prefix = " + xml_offline_dir_prefix);
                }
                if (p.get("config2xml.xml_offline_file_format") != null) {
                    xml_offline_file_format = p.get("config2xml.xml_offline_file_format").toString().trim();
                    LOG.info("config value config2xml.xml_offline_file_format = " + xml_offline_file_format);
                }
                if (p.get("config2xml.xml_overdue_dir_prefix") != null) {
                    xml_overdue_dir_prefix = p.get("config2xml.xml_overdue_dir_prefix").toString().trim();
                    LOG.info("config value config2xml.xml_overdue_dir_prefix = " + xml_overdue_dir_prefix);
                }
                if (p.get("config2xml.xml_overdue_file_format") != null) {
                    xml_overdue_file_format = p.get("config2xml.xml_overdue_file_format").toString().trim();
                    LOG.info("config value config2xml.xml_overdue_file_format = " + xml_overdue_file_format);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            b = false;
        }

        return b;
    }

    public static String getMysqlServerIp() {
        return mysql_server_ip;
    }

    public static int getMysqlServerPort() {
        return mysql_server_port;
    }

    public static String getMysqlDbName() {
        return mysql_db_name;
    }

    public static String getMysqlDbUser() {
        return mysql_db_user;
    }

    public static String getMysqlDbPass() {
        return mysql_db_pass;
    }

    public static int getWriteThreadNum() {
        return write_thread_num;
    }

    public static int getWriteBufferSize() {
        return write_buffer_size;
    }

    public static int getFlushInterval() {
        return flush_interval;
    }

    public static int getReadThreadNum() {
        return read_thread_num;
    }

    public static int getScannerCache() {
        return scanner_cache;
    }

    public static long getMemFlushSize() {
        return mem_flush_size;
    }

    public static int getTimeToLive() {
        return time_to_live;
    }

    public static boolean getResetTable() {
        return reset_table;
    }

    public static int getDataDate() {
        return data_date;
    }

    public static String getWorkMode() {
        return work_mode;
    }

    public static String getXmlOutputDirPrefix() {
        return xml_output_dir_prefix;
    }

    public static String getXmlOutputFileFormat() {
        return xml_output_file_format;
    }

    public static String getXmlOfflineDirPrefix() {
        return xml_offline_dir_prefix;
    }

    public static String getXmlOfflineFileFormat() {
        return xml_offline_file_format;
    }

    public static String getXmlOverdueDirPrefix() {
        return xml_overdue_dir_prefix;
    }

    public static String getXmlOverdueFileFormat() {
        return xml_overdue_file_format;
    }

}
