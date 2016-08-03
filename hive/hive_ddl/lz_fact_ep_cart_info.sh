#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：shicheng
# 邮箱: shicheng@taobao.com
# 日期：20120716
# ------------------------------------------------------------------------
# 功能：月光宝盒-购物车表
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_cart_info;
create external table lz_fact_ep_cart_info
(
   cart_timestamp  bigint comment '拍下时间戳，到秒', 
   shop_id              bigint comment '店铺ID',
   auction_id           bigint comment '宝贝ID',
   user_id              string comment '买家ID',
   cookie               string comment 'aplus cna',
   ali_corp             bigint comment '成交店铺所在网站类型（0 未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算）',
   cart_num             bigint comment '收藏次数',
   useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且归属计算中需要使用字段',
   extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
)
partitioned by (dt string, logsrc string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_cart_info';

EOF
