#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner: feiqiong.dpf
# Email: feiqiong.dpf@taobao.com
# Date: 2012-11-21
# ------------------------------------------------------------------------
# Description: etao效果汇总表
# Input: lz_fact_ep_etao_ownership, lz_rpt_ep_etao_summary_bysrc
# Onput: lz_rpt_ep_etao_summary
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_rpt_ep_etao_summary;

create external table lz_rpt_ep_etao_summary
(
   day                                  string comment '日期',
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
   cps_old_amt                          double comment 'etao返利老用户下单金额',
   total_cps_uv                         double comment '返利下单总uv',
   total_cps_trade_num                  double comment '返利下单总笔数',
   total_cps_amt                        double comment '返利下单总金额',
   total_gmv_uv                         double comment '下单总uv',
   total_gmv_trade_num                  double comment '下单总笔数',
   total_gmv_amt                        double comment '下单总金额',
   total_alipay_uv                      double comment '成交总uv',
   total_alipay_trade_num               double comment '成交总笔数',
   total_alipay_amt                     double comment '成交总金额',
   count                                bigint comment 'count'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_etao_summary';
alter table lz_rpt_ep_etao_summary set serdeproperties('serialization.null.format' = '');

EOF
