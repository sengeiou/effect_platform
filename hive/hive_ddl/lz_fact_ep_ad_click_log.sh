#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：nanjia
# 邮箱：nanjia.lj@taobao.com
# 日期：20120604
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_ad_click_log;
create external table lz_fact_ep_ad_click_log 
(
   log_version          bigint,
   thedate              string,
   time_stamp           string,
   url                  string,
   refer_url            string,
   uid_mid              string,
   shop_id              string,
   auction_id           string,
   ip                   string,
   mid                  string,
   uid                  string,
   sid                  string,
   aid                  string,
   agent                string,
   adid                 string,
   amid                 string,
   cmid                 string,
   pmid                 string,
   nmid                 string,
   nuid                 string,
   channelid            string
)
partitioned by (dt string, logsrc string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_ad_click_log'
;



EOF
