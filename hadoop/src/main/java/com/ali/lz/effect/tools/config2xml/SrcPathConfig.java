package com.ali.lz.effect.tools.config2xml;

public class SrcPathConfig {

    /**
     * 归属优先级，正整数，数值越大表示优先级越低，当某个效果踩中多个来源路径时选择优先级最高的来源进行归属
     */
    public static int PRIORITY = 10;

    /**
     * 本条规则匹配的最大来源路径实例数量及截断判定指标，超过该数量时将按指定效果指标从大到小保留 top-k
     * 条，指定的效果指标在最后计算的效果指标中必须出现； 未指定效果指标时，将按照独立来源出现频度从大到小保留 top-k 条，不指定时默认为 1000
     * 条；
     */
    public static int LIMIT_NUM = 1000;

    /**
     * Limit下的effect_id选项默认值
     */
    public static int LIMIT_EFFECT_ID = 0;

    /**
     * 默认路径的ID
     */
    public static int DEFAULT_PATH_ID_BASE = 0;

    /**
     * 外投广告的路径ID起始值范围
     */
    public static int OUTER_ADS_PATH_ID_BASE = 10000;

    /**
     * SPM广告的路径ID起始值范围
     */
    public static int SPM_ADS_PATH_ID_BASE = 20000;

}
