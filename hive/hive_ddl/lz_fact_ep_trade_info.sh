#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：nanjia.lj
# 邮箱：nanjia.lj@taobao.com
# 日期：20120606
# ------------------------------------------------------------------------
# 功能：月经宝盒-成交表
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_trade_info;
create external table lz_fact_ep_trade_info
(
   gmv_trade_timestamp  bigint comment '拍下时间戳，到秒', 
   shop_id              bigint comment '店铺ID',
   auction_id           bigint comment '宝贝ID',
   user_id              bigint comment '买家ID',
   ali_corp             bigint comment '成交店铺所在网站类型（0 未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算）',
   gmv_trade_num        bigint comment '拍下笔数',
   gmv_trade_amt        double comment '拍下金额',
   gmv_auction_num      bigint comment '拍下件数',
   alipay_trade_num     bigint comment '成交笔数',
   alipay_trade_amt     double comment '成交金额',
   alipay_auction_num   bigint comment '成交件数',
   useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且归属计算中需要使用字段',
   extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
)
partitioned by (dt string, round string, logsrc string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_trade_info';

EOF
