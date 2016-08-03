package com.ali.lz.effect.tools.hbase;

import java.util.HashMap;
import java.util.Map;

public class GenerateMap {
    /**
     * 生成指标ID与结果表字段的对应关系表 月光宝盒指标体系详细文档见redmine
     * http://red.lzdp.us/projects/effect
     * -platform/wiki/_Effect_Sprint2_Design_index
     */
    @SuppressWarnings("serial")
    public final static Map<Integer, Integer> indexMap = new HashMap<Integer, Integer>() {
        {
            // /////////////// 效果页相关
            put(103, 4); // 效果页_PV
            put(104, 5); // 效果页_UV
            put(105, 6); // 效果页_点击PV
            put(106, 7); // 效果页_点击UV
            put(107, 8); // 效果页_跳失率

            // /////////////// 单品链接引导
            // 单品
            put(200, 9); // 单品_PV
            put(201, 10); // 单品_UV
            put(202, 11); // 单品GMV_UV
            put(203, 13); // 单品GMV_件数
            put(204, 14); // 单品GMV_笔数
            put(205, 12); // 单品GMV_金额
            put(206, 15); // 单品ALIPAY_UV
            put(207, 17); // 单品ALIPAY_件数
            put(208, 18); // 单品ALIPAY_笔数
            put(209, 16); // 单品ALIPAY_金额
            // 单品同店
            put(211, 19); // 单品同店_PV
            put(212, 20); // 单品同店_UV
            put(213, 21); // 单品同店_IPV
            put(214, 22); // 单品同店_IUV
            put(215, 23); // 单品同店GMV_UV
            put(216, 25); // 单品同店GMV_件数
            put(217, 26); // 单品同店GMV_笔数
            put(218, 24); // 单品同店GMV_金额
            put(219, 27); // 单品同店ALIPAY_UV
            put(220, 29); // 单品同店ALIPAY_件数
            put(221, 30); // 单品同店ALIPAY_笔数
            put(222, 28); // 单品同店ALIPAY_金额

            // /////////////// 单店链接引导
            // 单店
            put(300, 31); // 单店_PV
            put(301, 32); // 单店_UV
            put(302, 33); // 单店_IPV
            put(303, 34); // 单店_IUV
            put(304, 35); // 单店GMV_UV
            put(305, 37); // 单店GMV_件数
            put(306, 38); // 单店GMV_笔数
            put(307, 36); // 单店GMV_金额
            put(308, 39); // 单店ALIPAY_UV
            put(309, 41); // 单店ALIPAY_件数
            put(310, 42); // 单店ALIPAY_笔数
            put(311, 40); // 单店ALIPAY_金额

            // //////////////// 其他链接引导
            // 单品
            put(401, 43); // 单品_PV
            put(402, 44); // 单品_UV
            put(403, 45); // 单品GMV_UV
            put(404, 47); // 单品GMV_件数
            put(405, 48); // 单品GMV_笔数
            put(406, 46); // 单品GMV_金额
            put(407, 49); // 单品ALIPAY_UV
            put(408, 51); // 单品ALIPAY_件数
            put(409, 52); // 单品ALIPAY_笔数
            put(410, 50); // 单品ALIPAY_金额
            // 单品同店
            put(411, 53); // 单品同店_PV
            put(412, 54); // 单品同店_UV
            put(413, 55); // 单品同店_IPV
            put(414, 56); // 单品同店_IUV
            put(415, 57); // 单品同店GMV_UV
            put(416, 59); // 单品同店GMV_件数
            put(417, 60); // 单品同店GMV_笔数
            put(418, 58); // 单品同店GMV_金额
            put(419, 61); // 单品同店ALIPAY_UV
            put(420, 63); // 单品同店ALIPAY_件数
            put(421, 64); // 单品同店ALIPAY_笔数
            put(422, 62); // 单品同店ALIPAY_金额
            // 单店
            put(423, 65); // 单店_PV
            put(424, 66); // 单店_UV
            put(425, 67); // 单店_IPV
            put(426, 68); // 单店_IUV
            put(427, 69); // 单店GMV_UV
            put(428, 71); // 单店GMV_件数
            put(429, 72); // 单店GMV_笔数
            put(430, 70); // 单店GMV_金额
            put(431, 73); // 单店ALIPAY_UV
            put(432, 75); // 单店ALIPAY_件数
            put(433, 76); // 单店ALIPAY_笔数
            put(434, 74); // 单店ALIPAY_金额

            // 效果页引导站外成交
            put(112, 77); // 站外GMV_UV
            put(113, 79); // 站外GMV_笔数
            put(114, 78); // 站外GMV_金额
            put(115, 80); // 站外ALIPAY_UV
            put(116, 82); // 站外ALIPAY_笔数
            put(117, 81); // 站外ALIPAY_金额

            // 效果页引导总流量
            put(109, 83); // 总PV
            put(110, 84); // 总UV
            put(111, 85); // 平均访问深度

        }
    };

    @SuppressWarnings("serial")
    public final static Map<Integer, Integer> adClickMap = new HashMap<Integer, Integer>() {
        {
            // //////////////// 广告位
            put(101, 4); // 广告点击量
            put(108, 5); // 广告点击人数
        }
    };

}
