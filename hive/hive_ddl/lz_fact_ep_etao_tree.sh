#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-10-22
# ------------------------------------------------------------------------
# Description: 由url或refer为etao的访问日志构建的etao访问树
# Input: lz_fact_ep_browse_log, lz_dim_ep_etao_config
# Onput: lz_fact_ep_etao_tree
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_fact_ep_etao_tree;
create external table lz_fact_ep_etao_tree
(
   ts                                   bigint comment '时间戳',
   is_etao                              bigint comment 'url是否为etao',
   refer_is_etao                        bigint comment 'refer是否为etao节点',
   is_lp                                bigint comment '是否为LP节点',
   refer_is_lp                          bigint comment '父节点是否为LP节点',
   lp_src                               bigint comment 'LP来源类型',
   lp_domain_name                       string comment 'lp页面二级域名',
   lp_adid                              string comment 'adid',
   lp_tb_market_id                      string comment 'tb_market_id',
   lp_refer_site                        string comment '从refer url中解析的站点二级域名',
   lp_site_id                           string comment '参数tb_lm_id对应的第一个值',
   lp_ad_id                             string comment '参数tb_lm_id对应的第一个之后的值',
   lp_apply                             string comment 'apply参数值',
   lp_t_id                              string comment 't_id参数值',
   lp_linkname                          string comment 'linkname参数值',
   lp_pub_id                            string comment '联盟外投pid',
   lp_pid_site_id                       string comment '联盟外投网站id',
   lp_adzone_id                         string comment '联盟外投广告位id',
   lp_keyword                           string comment '搜索关键词',
   lp_src_domain_name_level1            string comment '来源页面一级域名',
   lp_src_domain_name_level2            string comment '来源页面二级域名',
   channel_id                           bigint comment '频道id',
   refer_channel_id                     bigint comment 'refer的频道id',
   is_channel_lp                        bigint comment '是否频道页一跳',
   refer_is_channel_lp                  bigint comment '用来判断是否为频道页二跳',
   channel_src                          bigint comment '频道来源类型',
   refer_channel_src                    bigint comment 'refer的频道来源类型',
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
   url                                  string comment 'url',
   refer                                string comment 'refer',
   shop_id                              string comment '店铺ID，非店内页面为空',
   auction_id                           string comment '宝贝ID，非宝贝页为空',
   user_id                              string comment '用户ID，没有为空',
   cookie                               string comment '用来计算uv使用',
   trade_track_info                     string comment '计算返利和站外成交的关联字段',
   index_root_path                      string comment '从根节点至当前节点的路径序列，以.分割',
   session                              string comment 'session',
   ipv_ref_url                          string comment '宝贝页refer二级域名'
)
partitioned by (dt string, nodetype string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_etao_tree';

EOF
