#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date: 2012-10-22
# ------------------------------------------------------------------------
# Description: 无线日志访问树
# Input: lz_fact_ep_browse_log, lz_dim_ep_wireless_yyz
# Onput: lz_fact_ep_wireless_tree
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_wireless_tree;
create external table lz_fact_ep_wireless_tree
(
   ts                   string comment '时间戳',
   platform_id          bigint comment '无线平台类型：html5/wap',
   index_root_path      string comment '路径列表，用.分割',
   url                  string comment 'url',
   refer                string comment 'refer',
   shop_id              string comment '店铺ID，非店内页面为空',
   auction_id           string comment '宝贝ID，非宝贝页为空',
   user_id              string comment '用户ID，没有为空',
   is_effect_page       bigint comment '标识活动页',
   refer_is_effect_page bigint comment '标识活动页点击',
   plan_id              bigint comment '标识url所属的活动id',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   position_id          string comment '位置id（spm）',
   cookie               string
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_wireless_tree';

EOF
