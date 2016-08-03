#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 由etao访问树计算频道来源明细粒度的pv
# Input: lz_fact_ep_etao_tree
# Onput: lz_fact_ep_etao_channel_visit_effect
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_etao_channel_visit_effect;
create external table lz_fact_ep_etao_channel_visit_effect
(
   is_channel_lp                        bigint comment '是否频道页一跳',
   refer_is_channel_lp                  bigint comment '用来判断是否为频道页二跳',
   channel_id                           bigint comment '频道id',
   channel_src                          bigint comment '频道来源类型',
   refer_channel_src                    bigint comment 'refer频道来源类型',
   refer_channel_id                     bigint comment 'refer频道id',
   channel_adid                         string comment 'adid',
   channel_tb_market_id                 string comment 'tb_market_id',
   channel_refer_site                   string comment '从refer url中解析的站点二级域名',
   channel_site_id                      string comment '参数tb_lm_id对应的第一个值',
   channel_ad_id                        string comment '参数tb_lm_id对应的第一个之后的值',
   channel_apply                        string comment 'apply参数值',
   channel_t_id                         string comment 't_id参数值',
   channel_linkname                     string comment 'linkname参数值',
   channel_pub_id                       string comment '联盟外投pid',
   channel_pid_site_id                  string comment '联盟外投网站id',
   channel_adzone_id                    string comment '联盟外投广告位id',
   channel_keyword                      string comment '搜索关键词',
   channel_src_domain_name_level1       string comment '来源页面一级域名',
   channel_src_domain_name_level2       string comment '来源页面二级域名',
   ref_channel_adid                     string comment 'adid',
   ref_channel_tb_market_id             string comment 'tb_market_id',
   ref_channel_refer_site               string comment '从refer url中解析的站点二级域名',
   ref_channel_site_id                  string comment '参数tb_lm_id对应的第一个值',
   ref_channel_ad_id                    string comment '参数tb_lm_id对应的第一个之后的值',
   ref_channel_apply                    string comment 'apply参数值',
   ref_channel_t_id                     string comment 't_id参数值',
   ref_channel_linkname                 string comment 'linkname参数值',
   ref_channel_pub_id                   string comment '联盟外投pid',
   ref_channel_pid_site_id              string comment '联盟外投网站id',
   ref_channel_adzone_id                string comment '联盟外投广告位id',
   ref_channel_keyword                  string comment '搜索关键词',
   ref_channel_src_domain_name_level1   string comment '来源页面一级域名',
   ref_channel_src_domain_name_level2   string comment '来源页面二级域名',
   cookie                               string comment '用来计算uv使用(visit_id or cookie)',
   user_id                              string comment 'user_id',
   pv                                   double comment 'pv'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_channel_visit_effect';

EOF
