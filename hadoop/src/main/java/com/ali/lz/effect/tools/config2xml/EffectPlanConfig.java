/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

/**
 * @author jiuling.ypf
 * 
 */
public class EffectPlanConfig {

    /**
     * 分析方案格式版本，int值，后续更改分析方案格式时此版本号会更新，要求新版本的计算逻辑兼容老版本的分析方案格式 XXX: 版本变更历史:
     * 2012-06-28月光宝盒V1.0阶段VER设置为1. 2012-08-16月光宝盒V2.0阶段VER升级为2.
     */
    public static int VER = 2;

    /**
     * 方案结果刷新周期，以秒计，int值，目前仅对实时分析有效，离线分析固定按天刷新
     */
    public static int UPDATE_INTERVAL = 60;

    /**
     * 拆分规则，按照目前的需求，硬性定义了以下几种： ali -
     * 按阿里集团内公司拆分，其参数可以是以下列表中的一个或多个（以逗号分隔），选择多个时是指将这些公司作为整体看待，它们之间的跳转不会导致路径树拆分
     * etao - 一淘 taobao - 淘宝 tmall - 天猫 jhs - 聚划算 none - 只按默认路径树最大大小拆分，默认为该规则
     */
    public static String TREE_SPLIT = "none";

    /**
     * 归属计算方法，可以为以下几种: first - 归属至从源头开始首个来源(即离效果发生处最远的来源) last -
     * 归属至从源头开始最后一个来源(即离效果发生处最近的来源) equal - 所有踩中的来源均分效果 all - 所有踩中的来源同时得到相同效果
     */
    public static String[] ATTR_CALC = new String[] { "first", "last", "equal", "all" };

}
