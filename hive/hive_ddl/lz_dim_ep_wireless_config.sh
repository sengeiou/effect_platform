#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 无线一阳指活动维表，增加了月光宝盒规则属性
# Input: datax 
# Onput: lz_dim_ep_wireless_config
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_dim_ep_wireless_config;
create external table lz_dim_ep_wireless_config
(
   act_id               bigint comment '活动id',
   rule_type            bigint comment '规则类型',
   rule_name            string comment '无线活动名称',
   rule                 string comment '无线活动规则',
   start_time           bigint comment '开始时间戳',
   end_time             bigint comment '结束时间戳',
   cal_status           bigint comment '规则状态：0：停止计算, 1:正在计算',
   ctime                bigint comment '创建时间',
   update_time          bigint comment '修改时间',
   status               bigint comment '状态字段（处理特殊情况），默认是1，不展示该规则时是0',
   act_type             bigint comment '活动类型（1:一阳指, 2:TMS, 3:自定义）',
   time_type            bigint comment '设置时间计算类型，0是自定义开始和结束时间，1是永久',
   yyz_act_id           bigint comment '一阳指活动id，一阳指专用'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_dim_dir}/lz_dim_ep_wireless_config';

EOF
