#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 由etao访问树和站外成交记录计算cookie+来源明细粒度的站外成交效果
# Input: lz_fact_ep_etao_tree, lz_fact_ep_etao_outside_order
# Onput: lz_fact_ep_etao_outside_ownership
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_etao_outside_ownership;
create external table lz_fact_ep_etao_outside_ownership
(
   trade_track_info             string comment 'trade_track_info',
   user_id                      string comment '用户ID，没有为空',
   lp_src                       bigint comment 'LP来源类型',
   cookie                       string comment '用来计算uv使用（visit_id or cookie）',
   refer_is_lp                  bigint comment '父节点是否为LP节点',
   lp_domain_name               string comment 'lp页面二级域名',
   adid                         string comment 'adid',
   tb_market_id                 string comment 'tb_market_id',
   refer_site                   string comment '从refer url中解析的站点二级域名',
   site_id                      string comment '参数tb_lm_id对应的第一个值',
   ad_id                        string comment '参数tb_lm_id对应的第一个之后的值',
   apply                        string comment 'apply参数值',
   t_id                         string comment 't_id参数值',
   linkname                     string comment 'linkname参数值',
   pub_id                       string comment '联盟外投pid',
   pid_site_id                  string comment '联盟外投网站id',
   adzone_id                    string comment '联盟外投广告位id',
   keyword                      string comment '搜索关键词',
   src_domain_name_level1       string comment '来源页面一级域名',
   src_domain_name_level2       string comment '来源页面二级域名',
   outside_gmv_trade_num        double comment '站外下单笔数',
   outside_gmv_amt              double comment '站外下单金额',
   outside_alipay_trade_num     double comment '站外成交笔数',
   outside_alipay_amt           double comment '站外成交金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_outside_ownership';

EOF
