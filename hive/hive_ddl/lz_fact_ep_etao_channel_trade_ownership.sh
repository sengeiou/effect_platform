#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 汇总成交效果，计算cookie+来源明细粒度的频道成交效果
# Input: lz_fact_ep_etao_channel_inside_ownership, 
#        lz_fact_ep_etao_channel_outside_ownership,
#        lz_fact_ep_etao_channel_cps_ownership
# Onput: lz_fact_ep_etao_channel_trade_ownership
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_etao_channel_trade_ownership;
create external table lz_fact_ep_etao_channel_trade_ownership
(
   refer_channel_id                     bigint comment 'refer频道id',
   channel_src                          bigint comment '频道来源类型',
   adid                                 string comment 'adid',
   tb_market_id                         string comment 'tb_market_id',
   refer_site                           string comment '从refer url中解析的站点二级域名',
   site_id                              string comment '参数tb_lm_id对应的第一个值',
   ad_id                                string comment '参数tb_lm_id对应的第一个之后的值',
   apply                                string comment 'apply参数值',
   t_id                                 string comment 't_id参数值',
   linkname                             string comment 'linkname参数值',
   pub_id                               string comment '联盟外投pid',
   pid_site_id                          string comment '联盟外投网站id',
   adzone_id                            string comment '联盟外投广告位id',
   keyword                              string comment '搜索关键词',
   src_domain_name_level1               string comment '来源页面一级域名',
   src_domain_name_level2               string comment '来源页面二级域名',
   user_id                              string comment '用户ID，没有为空',
   direct_item_gmv_trade_num            double comment '直接下单笔数',
   direct_item_gmv_amt                  double comment '直接下单金额',
   direct_item_alipay_trade_num         double comment '直接成交笔数',
   direct_item_alipay_amt               double comment '直接成交金额',
   direct_itemshop_gmv_trade_num        double comment '间接下单笔数',
   direct_itemshop_gmv_amt              double comment '间接下单金额',
   direct_itemshop_alipay_trade_num     double comment '间接成交笔数',
   direct_itemshop_alipay_amt           double comment '间接成交金额',
   direct_shop_gmv_trade_num            double comment '店铺引导下单笔数',
   direct_shop_gmv_amt                  double comment '店铺引导下单金额',
   direct_shop_alipay_trade_num         double comment '店铺引导成交笔数',
   direct_shop_alipay_amt               double comment '店铺引导成交金额',
   outside_gmv_trade_num                double comment '站外下单笔数',
   outside_gmv_amt                      double comment '站外下单金额',
   outside_alipay_trade_num             double comment '站外成交笔数',
   outside_alipay_amt                   double comment '站外成交金额',
   cps_new_trade_num                    double comment '返利新用户下单笔数',
   cps_new_amt                          double comment '返利新用户下单金额',
   cps_old_trade_num                    double comment '返利老用户下单笔数',
   cps_old_amt                          double comment '返利老用户下单金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_channel_trade_ownership';

EOF
