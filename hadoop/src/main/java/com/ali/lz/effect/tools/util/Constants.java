/**
 * 
 */
package com.ali.lz.effect.tools.util;

/**
 * @author jiuling.ypf
 * 
 */
public interface Constants {

    // 月光宝盒离线部分HBase表名
    public static final String[] OFFLINE_HBASE_TABLE_NAMES = new String[] { "effect_rpt", "effect_rpt_sum",
            "effect_rpt_adclk", "effect_rpt_sum_bysrc" };

    // 月光宝盒实时部分HBase表名
    public static final String[] ONLINE_HBASE_TABLE_NAMES = new String[] { "effect_rt_rpt", "effect_rt_rpt_sum",
            "effect_rt_rpt_adclk", "effect_rt_rpt_sum_bysrc" };

    // 月光宝盒实时部分HBase索引表名
    public static final String[] ONLINE_HBASE_INDEX_TABLE_NAMES = new String[] { "effect_rt_traffic_index" };

    // 月光宝盒离线部分MySQL表名
    public static final String[] OFFLINE_MYSQL_TABLE_NAMES = new String[] { "rpt_detail", "rpt_summary",
            "rpt_extend_ad", "rpt_summary_bysrc" };

    // 月光宝盒实时部分MySQL表名
    public static final String[] ONLINE_MYSQL_TABLE_NAMES = new String[] { "rpt_rt_detail", "rpt_rt_summary",
            "rpt_rt_extend_ad", "rpt_rt_summary_bysrc" };

    // 月光宝盒 HBase Family名
    public static final String HBASE_FAMILY_NAME = "d";

    // 计划共享表状态：审核完毕
    public static final int PLAN_SHARE_TYPE_COMPLETE = 1;

    // 计划共享表状态：禁用或者删除
    public static final int PLAN_SHARE_TYPE_FORBIDDEN = 2;

}
