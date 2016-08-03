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
drop table lz_rpt_ep_ad_click_info;
create external table lz_rpt_ep_ad_click_info 
(
    plan_id    bigint    comment '推广计划ID',
    analyzer_id bigint,
    the_date           string,
    adid                 string,
    pv                   float,
    uv                   float
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as textfile
location '${hive_rpt_dir}/lz_rpt_ep_ad_click_info'
;
alter table lz_rpt_ep_ad_click_info set serdeproperties('serialization.null.format' = '');
EOF
