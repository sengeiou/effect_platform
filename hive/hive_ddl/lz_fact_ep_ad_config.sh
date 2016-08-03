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
drop table lz_fact_ep_ad_config;
create external table lz_fact_ep_ad_config 
(
    ad_id                   string,
    ad_site_name            string,
    ad_page_name            string,
    ad_position_name        string,
    ad_creative_name        string,
    ad_activity_name        string,
    ad_activity_id          string
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_ad_config'
;
EOF
