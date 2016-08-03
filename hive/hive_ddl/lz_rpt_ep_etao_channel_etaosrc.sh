#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner: feiqiong.dpf
# Email: feiqiong.dpf@taobao.com
# Date: 2012-11-21
# ------------------------------------------------------------------------
# Description: etao频道效果etao来源明细报表
# Input: lz_fact_ep_etao_channel_ownership
# Onput: lz_rpt_ep_etao_channel_etaosrc
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_rpt_ep_etao_channel_etaosrc;

create external table lz_rpt_ep_etao_channel_etaosrc
(
   day                                  string comment '日期',
   channel_id                           bigint comment '频道id',
   src_domain_name                      string comment '来源页面二级域名',
   effect_pv                            double comment '一跳pv',
   effect_uv                            double comment '一跳uv',
   effect_click_pv                      double comment '二跳pv',
   effect_click_uv                      double comment '二跳uv',
   click_rate                           double comment '二跳率',
   bounce_rate                          double comment '跳失率',
   channel_pv                           double comment '频道总pv',
   channel_uv                           double comment '频道总uv',
   avg_pv                               double comment '频道访问深度',
   direct_item_gmv_uv                   double comment '直接下单uv',
   direct_item_gmv_trade_num            double comment '直接下单笔数',
   direct_item_gmv_amt                  double comment '直接下单金额',
   direct_item_alipay_uv                double comment '直接成交uv',
   direct_item_alipay_trade_num         double comment '直接成交笔数',
   direct_item_alipay_amt               double comment '直接成交金额',
   direct_itemshop_gmv_uv               double comment '间接下单uv',
   direct_itemshop_gmv_trade_num        double comment '间接下单笔数',
   direct_itemshop_gmv_amt              double comment '间接下单金额',
   direct_itemshop_alipay_uv            double comment '间接成交uv',
   direct_itemshop_alipay_trade_num     double comment '间接成交笔数',
   direct_itemshop_alipay_amt           double comment '间接成交金额',
   direct_shop_gmv_uv                   double comment '店铺下单uv',
   direct_shop_gmv_trade_num            double comment '店铺下单笔数',
   direct_shop_gmv_amt                  double comment '店铺下单金额',
   direct_shop_alipay_uv                double comment '店铺成交uv',
   direct_shop_alipay_trade_num         double comment '店铺成交笔数',
   direct_shop_alipay_amt               double comment '店铺成交金额',
   outside_gmv_uv                       double comment '站外下单uv',
   outside_gmv_trade_num                double comment '站外下单笔数',
   outside_gmv_amt                      double comment '站外下单金额',
   outside_alipay_uv                    double comment '站外成交uv',
   outside_alipay_trade_num             double comment '站外成交笔数',
   outside_alipay_amt                   double comment '站外成交金额',
   cps_new_uv                           double comment '返利新用户下单uv',
   cps_new_trade_num                    double comment '返利新用户下单笔数',
   cps_new_amt                          double comment '返利新用户下单金额',
   cps_old_uv                           double comment '返利老用户下单uv',
   cps_old_trade_num                    double comment '返利老用户下单笔数',
   cps_old_amt                          double comment '返利老用户下单金额',
   total_cps_uv                         double comment '返利下单总uv',
   total_cps_trade_num                  double comment '返利下单总笔数',
   total_cps_amt                        double comment '返利下单总金额',
   total_gmv_uv                         double comment '下单总uv',
   total_gmv_trade_num                  double comment '下单总笔数',
   total_gmv_amt                        double comment '下单总金额',
   total_alipay_uv                      double comment '成交总uv',
   total_alipay_trade_num               double comment '成交总笔数',
   total_alipay_amt                     double comment '成交总金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_etao_channel_etaosrc';
alter table lz_rpt_ep_etao_channel_etaosrc set serdeproperties('serialization.null.format' = '');

EOF
