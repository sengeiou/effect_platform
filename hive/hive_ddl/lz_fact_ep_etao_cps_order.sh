#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner: feiqiong.dpf        
# Email: feiqiong.dpf@taobao.com
# Date:2012/09/17
# ------------------------------------------------------------------------ 
# Description:etao cps返利订单表
# Input:s_dw_etao_cps_order
# Onput:lz_fact_ep_etao_cps_order
# ------------------------------------------------------------------------ 
# ChangeLog:
########################################################################## 

$Hive <<EOF
drop table lz_fact_ep_etao_cps_order;
create external table lz_fact_ep_etao_cps_order
(
   source               string  comment '来源:1-亿起发。2-淘客。3-成果联盟。4-一淘联盟',
   trade_no             string  comment '交易ID',
   trade_track_info     string  comment 'trade_track_info',
   shop_id              string  comment '店铺ID',
   is_settle            string  comment '是否返利 1 是 0 否',
   seller_id            string  comment '卖家ID',
   user_id              string  comment '买家userid',
   gmv_num              double  comment '创建订单笔数',
   gmv_amt              double  comment '创建订单金额',
   buytime              bigint  comment '订单创建时间',
   checktime            bigint  comment '订单确认时间',
   is_new               string  comment '是否新购买用户 1为是, 0 为否'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_cps_order';

EOF
