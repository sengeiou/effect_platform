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
drop table lz_fact_ep_ownership;
create external table lz_fact_ep_ownership 
(
    index_root_path     string      comment '路径列表，用.分割',
    ts                  string      comment '时间戳',
    analyzer_id         bigint      comment '制定推广计划的用户ID',
    plan_id             bigint      comment '推广计划ID',
    src                 string      comment '来源路径实例，可作为ID',
    url                 string      comment 'url',
    refer_url           string      comment 'refer_url',
    shop_id             string      comment '店铺ID，非店内页面为空',
    auction_id          string      comment '宝贝ID，非宝贝页为空',
    user_id             string      comment '用户ID，没有为空',
    ali_corp            bigint      comment '0 未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算',
    cookie              string      comment 'cookie用来标识一个访问用户（aplus日志中用mid）',
    session             string      comment '一次会话的标识（aplus日志中用sid）',
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
    cart_auction_num    double      comment '购物车宝贝数',
    useful_extra        string      comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且归属计算中需要使用字段',
    extra               string      comment '扩展字段，使用ctrl+B分割。为需要携带字段',
    src_useful_extra    string    comment '扩展字段，继承自效果页的useful_extra'
)
partitioned by (dt string, type string, planId bigint)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_ownership'
;
EOF
