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
drop table lz_rpt_ep_spm_info;
create external table lz_rpt_ep_spm_info 
(
    spm_id    string    comment 'spm中每个字段的ID',
    position    bigint  comment 'spm中每个字段的位置：1/2/3/4',
    title   string
)
partitioned by (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as textfile
location '${hive_rpt_dir}/lz_rpt_ep_spm_info'
;
alter table lz_rpt_ep_spm_info set serdeproperties('serialization.null.format' = '');
EOF
