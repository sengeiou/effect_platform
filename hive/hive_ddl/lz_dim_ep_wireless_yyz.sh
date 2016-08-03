#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 无线一阳指生成活动维表
# Input: wireless_dim_yyz_page
# Onput: lz_dim_ep_wireless_yyz
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_dim_ep_wireless_yyz;
create external table lz_dim_ep_wireless_yyz
(
   plan_id              bigint comment '无线活动id',
   title                string comment '无线活动名称',
   url                  string comment '无线活动url',
   creater              string comment '无线活动创建人花名',
   gmt_create           string comment '无线活动创建时间',
   gmt_end              string comment '无线活动结束时间'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_dim_dir}/lz_dim_ep_wireless_yyz';

EOF
