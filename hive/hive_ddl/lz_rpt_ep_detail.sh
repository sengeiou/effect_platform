#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：feiqiong
# 邮箱：feiqiong.dpf@taobao.com
# 日期：20120611
# 更新：20120807
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_rpt_ep_detail;
create external table lz_rpt_ep_detail 
(
    plan_id    bigint    comment '推广计划ID',
    analyzer_id bigint,
    day string,
    dim bigint     comment '未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算',
    effect_pv double,
    effect_uv double,
    effect_click_pv double,
    effect_click_uv double,
    effect_bounce_rate double,
    direct_item_pv    double   comment '单品pv（后面类推）',
    direct_item_uv double,
    direct_item_gmv_uv double,
    direct_item_gmv_amt    double,
    direct_item_gmv_auction_num    double,
    direct_item_gmv_trade_num    double,
    direct_item_alipay_uv double,
    direct_item_alipay_amt    double,
    direct_item_alipay_auction_num    double,
    direct_item_alipay_trade_num    double,
    direct_itemshop_pv    double   comment '单品同店pv（后面类推）',
    direct_itemshop_uv double,
    direct_itemshop_ipv    double,
    direct_itemshop_iuv double,
    direct_itemshop_gmv_uv double,
    direct_itemshop_gmv_amt    double,
    direct_itemshop_gmv_auction_num    double,
    direct_itemshop_gmv_trade_num    double,
    direct_itemshop_alipay_uv double,
    direct_itemshop_alipay_amt    double,
    direct_itemshop_alipay_auction_num    double,
    direct_itemshop_alipay_trade_num    double,
    direct_shop_pv    double   comment '单店pv（后面类推）',
    direct_shop_uv double,
    direct_shop_ipv    double,
    direct_shop_iuv double,
    direct_shop_gmv_uv double,
    direct_shop_gmv_amt    double,
    direct_shop_gmv_auction_num    double,
    direct_shop_gmv_trade_num    double,
    direct_shop_alipay_uv double,
    direct_shop_alipay_amt    double,
    direct_shop_alipay_auction_num    double,
    direct_shop_alipay_trade_num    double,
    other_item_pv    double   comment '其他链接引导单品pv（后面类推）',
    other_item_uv double,
    other_item_gmv_uv double,
    other_item_gmv_amt    double,
    other_item_gmv_auction_num    double,
    other_item_gmv_trade_num    double,
    other_item_alipay_uv double,
    other_item_alipay_amt    double,
    other_item_alipay_auction_num    double,
    other_item_alipay_trade_num    double,
    other_itemshop_pv    double   comment '其他链接引导单品同店pv（后面类推）',
    other_itemshop_uv double,
    other_itemshop_ipv    double,
    other_itemshop_iuv double,
    other_itemshop_gmv_uv double,
    other_itemshop_gmv_amt    double,
    other_itemshop_gmv_auction_num    double,
    other_itemshop_gmv_trade_num    double,
    other_itemshop_alipay_uv double,
    other_itemshop_alipay_amt    double,
    other_itemshop_alipay_auction_num    double,
    other_itemshop_alipay_trade_num    double,
    other_shop_pv    double   comment '其他链接引导单店pv（后面类推）',
    other_shop_uv double,
    other_shop_ipv    double,
    other_shop_iuv double,
    other_shop_gmv_uv double,
    other_shop_gmv_amt    double,
    other_shop_gmv_auction_num    double,
    other_shop_gmv_trade_num    double,
    other_shop_alipay_uv double,
    other_shop_alipay_amt    double,
    other_shop_alipay_auction_num    double,
    other_shop_alipay_trade_num    double,
    outside_gmv_uv    double,
    outside_gmv_amt   double,
    outside_gmv_trade_num double,
    outside_alipay_uv    double,
    outside_alipay_amt   double,
    outside_alipay_trade_num double,
    pv double,
    uv double,
    avg_pv double,
    src    string   comment '来源路径实例，可作为ID'
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as textfile
location '${hive_rpt_dir}/lz_rpt_ep_detail'
;
alter table lz_rpt_ep_detail set serdeproperties('serialization.null.format' = '');
EOF
