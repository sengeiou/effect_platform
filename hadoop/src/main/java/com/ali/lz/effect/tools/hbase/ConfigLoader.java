package com.ali.lz.effect.tools.hbase;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author jiuling.ypf HBase客户端配置加载类
 * 
 */
public class ConfigLoader {

    private static final Log Log = LogFactory.getLog(ConfigLoader.class);

    /**
     * hbase region mem store size
     */
    private static long mem_flush_size = 64 * 1024 * 1024;

    /**
     * concurrent htable number
     */
    private static int write_thread = 5;

    /**
     * hbase client cache
     */
    private static int client_cache = 1024 * 1024;

    /**
     * hbase client flush interval
     */
    private static int flush_interval = 1000;

    /**
     * time to live for hbase table
     */
    private static int ttl = 90 * 24 * 60 * 60;

    /**
     * reset table
     */
    private static boolean reset_table = false;

    /**
     * process data path
     */
    private static String process_path = "/home/jiuling.ypf/data";

    public static boolean initConf() {
        boolean b = true;

        Properties p = new Properties();
        try {
            File file = new File(".");

            Log.info("File + getCanonicalPath=" + file.getCanonicalPath());
            Log.debug("File + getAbsolutePath=" + file.getAbsolutePath());
            Log.debug("System.getProperty user.dir=" + System.getProperty("user.dir"));
            String confFile = file.getCanonicalPath() + "/conf.properties";

            FileReader fr = new FileReader(confFile);
            p.load(fr);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            b = b && false;
        }

        try {
            if (!p.isEmpty()) {

                if (p.get("mem_flush_size") != null) {
                    mem_flush_size = Integer.parseInt(p.get("mem_flush_size").toString().trim());
                    System.out.println("config value mem_flush_size = " + mem_flush_size);
                    Log.debug("config value mem_flush_size = " + mem_flush_size);
                    mem_flush_size = mem_flush_size * 1024 * 1024;
                }

                if (p.get("write_thread") != null) {
                    write_thread = Integer.parseInt(p.get("write_thread").toString().trim());

                    System.out.println("config value threadN = " + write_thread);
                    Log.debug("config value threadN = " + write_thread);
                }

                if (p.get("client_cache") != null) {
                    client_cache = Integer.parseInt(p.get("client_cache").toString().trim());
                    client_cache = client_cache * 1024 * 1024;

                    System.out.println("config value client_cache = " + client_cache);
                    Log.debug("config value client_cache = " + client_cache);
                }

                if (p.get("flush_interval") != null) {
                    flush_interval = Integer.parseInt(p.get("flush_interval").toString().trim());
                    System.out.println("config value flush_interval = " + flush_interval);
                    Log.debug("config value flush_interval = " + flush_interval);
                }

                if (p.get("ttl") != null) {
                    ttl = Integer.parseInt(p.get("ttl").toString().trim());

                    ttl = ttl * 24 * 60 * 60;
                    System.out.println("config value ttl = " + ttl);
                    Log.debug("config value ttl = " + ttl);
                }

                if (p.get("reset_table") != null) {
                    reset_table = Boolean.parseBoolean(p.get("reset_table").toString().trim());

                    System.out.println("config value reset_table = " + reset_table);
                    Log.debug("config value reset_table = " + reset_table);
                }

                if (p.get("process_path") != null) {
                    process_path = p.get("process_path").toString().trim();
                    System.out.println("config value process_path = " + process_path);
                    Log.debug("config value process_path = " + process_path);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            b = b && false;
        }

        return b;
    }

    public static long getMemFlushSize() {
        return mem_flush_size;
    }

    public static int getWriteThread() {
        return write_thread;
    }

    public static int getClientCache() {
        return client_cache;
    }

    public static int getFlushInterval() {
        return flush_interval;
    }

    public static int getTTL() {
        return ttl;
    }

    public static boolean getResetTable() {
        return reset_table;
    }

    public static String getProcessPath() {
        return process_path;
    }

}
