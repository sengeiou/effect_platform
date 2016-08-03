#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner: feiqiong.dpf
# Email: feiqiong.dpf@taobao.com
# Date: 2012-11-21
# ------------------------------------------------------------------------
# Description: etao效果-分来源汇总表
# Input: lz_fact_ep_etao_ownership
# Onput: lz_rpt_ep_etao_summary_bysrc_dataportal
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_rpt_ep_etao_summary_bysrc_dataportal;

create external table lz_rpt_ep_etao_summary_bysrc_dataportal
(
   day                                  string comment '日期',
   lp_src                               bigint comment '访问来源',
   effect_pv                            double comment '一跳pv',
   effect_uv                            double comment '一跳uv',
   effect_click_pv                      double comment '二跳pv',
   effect_click_uv                      double comment '二跳uv',
   click_rate                           double comment '二跳率',
   bounce_rate                          double comment '跳失率',
   etao_pv                              double comment 'etao总pv',
   etao_uv                              double comment 'etao总uv',
   etao_avg_pv                          double comment 'etao访问深度',
   direct_item_gmv_uv                   double comment 'etao直接下单uv',
   direct_item_gmv_trade_num            double comment 'etao直接下单笔数',
   direct_item_gmv_amt                  double comment 'etao直接下单金额',
   direct_item_alipay_uv                double comment 'etao直接成交uv',
   direct_item_alipay_trade_num         double comment 'etao直接成交笔数',
   direct_item_alipay_amt               double comment 'etao直接成交金额',
   direct_itemshop_gmv_uv               double comment 'etao间接下单uv',
   direct_itemshop_gmv_trade_num        double comment 'etao间接下单笔数',
   direct_itemshop_gmv_amt              double comment 'etao间接下单金额',
   direct_itemshop_alipay_uv            double comment 'etao间接成交uv',
   direct_itemshop_alipay_trade_num     double comment 'etao间接成交笔数',
   direct_itemshop_alipay_amt           double comment 'etao间接成交金额',
   outside_gmv_uv                       double comment 'etao站外下单uv',
   outside_gmv_trade_num                double comment 'etao站外下单笔数',
   outside_gmv_amt                      double comment 'etao站外下单金额',
   outside_alipay_uv                    double comment 'etao站外成交uv',
   outside_alipay_trade_num             double comment 'etao站外成交笔数',
   outside_alipay_amt                   double comment 'etao站外成交金额',
   cps_new_uv                           double comment 'etao返利新用户下单uv',
   cps_new_trade_num                    double comment 'etao返利新用户下单笔数',
   cps_new_amt                          double comment 'etao返利新用户下单金额',
   cps_old_uv                           double comment 'etao返利老用户下单uv',
   cps_old_trade_num                    double comment 'etao返利老用户下单笔数',
   cps_old_amt                          double comment 'etao返利老用户下单金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_etao_summary_bysrc_dataportal';
alter table lz_rpt_ep_etao_summary_bysrc_dataportal set serdeproperties('serialization.null.format' = '');

EOF
