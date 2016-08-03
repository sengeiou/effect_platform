#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date: 2012-10-22
# ------------------------------------------------------------------------
# Description: 无线活动效果归属表
# Input: lz_fact_ep_wireless_tree, lz_fact_ep_trade_info
# Onput: lz_fact_ep_wireless_ownership
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_wireless_ownership;
create external table lz_fact_ep_wireless_ownership
(
   platform_id          bigint comment '无线平台类型：html5/wap',
   auction_id           string comment '宝贝ID，非宝贝页为空',
   shop_id              string comment '店铺ID，非店内页面为空',
   user_id              string comment '用户ID，没有为空',
   cookie               string,
   is_effect_page       bigint comment '标识活动页',
   refer_is_effect_page bigint comment '标识活动页点击',
   plan_id              bigint comment '标识url所属的活动id',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   position_id          string comment '位置id（spm），无线效果中暂时留空',
   effect_pv            bigint comment '活动页pv',
   direct_ipv           bigint comment '直接ipv',
   guide_ipv            bigint comment '引导ipv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_wireless_ownership';

EOF
