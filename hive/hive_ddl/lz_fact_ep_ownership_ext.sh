#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：feiqiong
# 邮箱：feiqiong.dpf@taobao.com
# 日期：20120611
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_ownership_ext;
create external table lz_fact_ep_ownership_ext 
(
    analyzer_id         bigint      comment '制定推广计划的用户ID',
    plan_id             bigint      comment '推广计划ID',
    src                 string      comment '来源路径实例，可作为ID',
    dim                 bigint      comment '1 淘宝 2 天猫 3 一淘 4 聚划算',
    day                 string      comment 'day',
    path_id             bigint      comment 'path_id',
    src_id              bigint      comment 'src_id',
    auction_id          string      comment '宝贝ID，非宝贝页为空',
    user_id             string      comment '用户ID，没有为空',
    visit_id             string      comment '用来计算uv使用（aplus日志中用visit_id, 其他情况直接使用cookie）',
    is_effect_page      bigint      comment '是否为效果页 1为true',
    ref_is_effect_page  bigint      comment '是否为效果页下一跳 1为true',
    is_leaf             bigint      comment '是否为树的叶子节点 1为true',
    jump_num            bigint      comment '引导宝贝相对效果页跳数',
    index_type          bigint      comment '指标类型标识(0非店铺, 1单品, 2单品同店, 3单店, 4淘外成交(仅etao才有), 5单品其他, 6单店其他)',
    pv                  double      comment 'pv',
    gmv_amt             double      comment '拍下金额',
    gmv_auction_num     double      comment '拍下商品件数',
    gmv_trade_num       double      comment '拍下笔数',
    alipay_amt          double      comment '成交金额',
    alipay_auction_num  double      comment '成交商品件数',
    alipay_trade_num    double      comment '成交笔数',
    item_collect_num    double      comment '收藏宝贝数',
    shop_collect_num    double      comment '收藏店铺数',
    cart_auction_num    double      comment '购物车宝贝数'
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_ownership_ext'
;
EOF
