package com.ali.lz.effect.utils;

import java.util.regex.Pattern;

public class Constants {

    public static boolean DEBUG = false;
    public static String CTRL_A = "\u0001";
    public static String CTRL_B = "\u0002";
    public static String CTRL_C = "\u0003";
    public static String CTRL_D = "\u0004";
    public static String CTRL_E = "\u0005";
    public static String CTRL_F = "\u0006";
    public static String CTRL_G = "\u0007";
    public static String TAB = "\t";
    public static String NEWLINE = "\n";
    public static String NULL = "\\N";
    public static String HJLJ_LOGKEY = "logkey";
    public static String MERGE_PLANID = "0";

    public static Pattern tmallPattern = Pattern.compile("^(http|https)://([^/]*\\.)?tmall\\.com.*");
    public static Pattern taobaoPattern = Pattern.compile("^(http|https)://([^/]*\\.)?taobao\\.com.*");
    public static Pattern etaoPattern = Pattern.compile("^(http|https)://([^/]*\\.)?etao\\.com.*");
    public static Pattern juPattern = Pattern.compile("^(http|https)://ju\\.taobao\\.com.*");

    public static final Pattern FIELD_REPLACE_PATTERN = Pattern.compile("\\r|\\n|\\t|\\001");

    public static final String CONFIG_FILE_PATH = "config_paths";

    public enum AliCorpUrl {
        TAOBAO_URL(1, "(http|https)://([^/]*\\.)?taobao\\.com.*"), TMALL_URL(2,
                "(http|https)://([^/]*\\.)?tmall\\.com.*"), ETAO_URL(3, "(http|https)://([^/]*\\.)?etao\\.com.*"), JU_URL(
                4, "(http|https)://ju\\.taobao\\.com.*");

        private int id;
        private String url;

        private AliCorpUrl(int id, String url) {
            this.id = id;
            this.url = url;
        }

        public int getId() {
            return this.id;
        }

        public String getUrl() {
            return this.url;
        }
    }

    public enum ELogType {
        ACCESS_LOG(0), GMV_LOG(1), COLLECT_LOG(2), CART_LOG(3);

        private int logType;

        private ELogType(int logType) {
            this.logType = logType;
        }

        public int getLogType() {
            return this.logType;
        }
    }

    public enum ELogColumnNum {
        ACCESS_LOG_COLUMN_NUM(11), GMV_LOG_COLUMN_NUM(11), COLLECT_LOG_COLUMN_NUM(7), CART_LOG_COLUMN_NUM(6), HOLOTREE_LOG_COLUMN_NUM(
                18);

        private int columnNum;

        private ELogColumnNum(int columnNum) {
            this.columnNum = columnNum;
        }

        public int getColumnNum() {
            return this.columnNum;
        }

    }

    public enum EGroupingKeyType {
        ACCESS_LOG("A_"), GMV_OWNERSHIP("G_"), COLLECT_OWNERSHIP("COL_"), CART_OWNERSHIP("CART_");
        private String prefix;

        private EGroupingKeyType(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return this.prefix;
        }
    }
}
