#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date: 2012-10-22
# ------------------------------------------------------------------------
# Description: 无线活动效果汇总表
# Input: lz_fact_ep_wireless_ownership
# Onput: lz_rpt_ep_wireless_summary
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_rpt_ep_wireless_summary;
create external table lz_rpt_ep_wireless_summary
(
   day                  string comment '日期',
   platform_id          bigint comment '无线平台类型：html5/wap',
   plan_id              bigint comment '活动id',
   count                bigint comment '下钻到detail表中对应的条数',
   effect_pv            bigint comment '效果页总pv',
   effect_uv            bigint comment '效果页总uv',
   direct_ipv           bigint comment '直接ipv',
   direct_iuv           bigint comment '直接iuv',
   direct_gmv_uv        bigint comment '直接下单uv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_uv     bigint comment '直接成交uv',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_ipv            bigint comment '引导ipv',
   guide_iuv            bigint comment '引导iuv',
   guide_gmv_uv         bigint comment '引导下单uv',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_uv      bigint comment '引导成交uv',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_wireless_summary';

EOF
