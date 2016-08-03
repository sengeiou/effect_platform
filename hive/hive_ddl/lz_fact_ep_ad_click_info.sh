#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：shicheng
# 邮箱：shicheng@taobao.com
# 日期：20120611
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_ad_click_info;
create external table lz_fact_ep_ad_click_info 
(
   analyzer_id          bigint,
   plan_id              bigint,
   adid                 string,
   cookie               string,
   pv                   float
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_ad_click_info'
;
EOF
