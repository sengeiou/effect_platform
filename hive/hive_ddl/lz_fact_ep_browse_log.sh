#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：nanjia
# 邮箱：nanjia.lj@taobao.com
# 日期：20120604
# ------------------------------------------------------------------------
# 功能：月光宝盒--浏览信息表
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_browse_log;
create external table lz_fact_ep_browse_log 
(
   time_stamp           bigint comment '访问时间戳，到秒',
   url                  string comment 'url',
   refer_url            string comment 'refer_url',
   shop_id              string comment '店铺ID：如果当前访问为店铺内页面，则填写。否则为空字符串',
   auction_id           string comment '宝贝ID: 如果当前访问为宝贝页面，则填写。否则为空字符串',
   user_id              string comment '访客ID',
   cookie               string comment 'cookie用来标识一个访问用户（aplus日志中用mid）',
   session              string comment '一次会话的标识（aplus日志中用sid）',
   visit_id              string comment '用来计算uv使用（aplus日志中用visit_id, 其他情况直接使用cookie）',
   useful_extra         string comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且归属计算中需要使用字段',
   extra                string comment '扩展字段，使用ctrl+B分割。为需要携带字段'
)
partitioned by (dt string, logsrc string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_browse_log'
;



EOF
