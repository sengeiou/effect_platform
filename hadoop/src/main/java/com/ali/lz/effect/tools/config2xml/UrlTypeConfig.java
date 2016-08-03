package com.ali.lz.effect.tools.config2xml;

/**
 * URL规则配置类
 * 
 * @author jiuling.ypf
 * 
 */
public class UrlTypeConfig {

    /**
     * 匹配优先级，正整数，数值越大表示优先级越低，优先级数值相同时以先出现的优先级高， 当有多条规则匹配成功时选择优先级最高的规则
     */
    public static int PRIORITY = 10;

    /**
     * 自定义URL ID的起始ID号
     */
    public static int TYPE_ID = 10000;

    /**
     * 以下是预设类型的来源识别规则
     */

    public static int NUM = 16;

    public static final int[] DEFAULT_IDS = { 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113,
            114, 115 };

    public static final String[] DEFAULT_NAMES = { "市场外投媒体", "联盟广告", "Tanx", "SPM", "直通车", "淘宝客", "钻展", "定价ECPM",
            "自助CPT", "辛巴克CPT", "淘网址", "富媒体icast", "超级麦霸", "淘代码", "旺旺广告", "etao" };

    public static final String[] DEFAULT_MATCH_FIELDS = { "url", "url", "url", "url", "url", "url", "url", "url",
            "url", "url", "referer", "url", "referer", "url", "url", "referer" };

    public static final String[] DEFAULT_TARGET_TYPES = { "referer", "referer", "referer", "referer", "referer",
            "referer", "referer", "referer", "referer", "referer", "referer", "referer", "referer", "referer",
            "referer", "referer" };

    public static final String[] DEFAULT_MATCH_REGEXPS = { "tb_market_id=", "pid=mm_", "ali_trackid=13_",
            "spm=([^&.]+(?:\\.[^&.]+){3})(?:\\.|&|$)", "ali_trackid=1_", "ali_trackid=2(:|%3A)", "ali_trackid=3_",
            "ali_trackid=12_", "ali_trackid=8_", "ali_trackid=9_", "tao123\\.com", "ali_trackid=11(&|$)",
            "\\/go\\/act\\/other\\/maiba", "ali_trackid=7_", "ali_trackid=10_", "etao\\.com" };

    public static final String[] DEFAULT_MATCH_PROP_FIELDS = { "", "", "", "spm", "", "", "", "", "", "", "", "", "",
            "", "", "" };

    public static final String[] DEFAULT_MATCH_PROP_VALUES = { "", "", "", "$1", "", "", "", "", "", "", "", "", "",
            "", "", "" };

    public static final String[] DEFAULT_EXTRACT_REGEXPS = { "", "", "", "", "ali_refid=[^:]+:\\w+:\\d+:([^:]*):", "",
            "", "", "", "", "", "", "", "", "", "" };

    public static final String[] DEFAULT_EXTRACT_PROP_FIELDS = { "", "", "", "", "p4p_keyword", "", "", "", "", "", "",
            "", "", "", "", "" };

    public static final String[] DEFAULT_EXTRACT_PROP_VALUES = { "", "", "", "", "$1", "", "", "", "", "", "", "", "",
            "", "", "" };

}
