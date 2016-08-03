#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 由etao访问树计算来源明细粒度的pv
# Input: lz_fact_ep_etao_tree
# Onput: lz_fact_ep_etao_visit_effect
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_etao_visit_effect;
create external table lz_fact_ep_etao_visit_effect
(
   lp_src                       bigint comment 'LP来源类型',
   lp_domain_name               string comment 'lp页面二级域名',
   is_lp                        bigint comment '是否为LP节点',
   refer_is_lp                  bigint comment '父节点是否为LP节点',
   is_etao                      bigint comment 'url是否为etao',
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
   cookie                       string comment '用来计算uv使用',
   user_id                      string comment 'user_id',
   pv                           double comment 'pv'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_visit_effect';

EOF
