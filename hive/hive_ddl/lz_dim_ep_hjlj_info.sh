#!/bin/bash
source $HOME/linezing/config/set_env.conf

##########################################################################
# Owner: 
# Email: 
# Date: 
# ------------------------------------------------------------------------
# Description: 黄金令箭信息维表，只有在该表中的黄金令箭埋点才会计算效果
# Input:
# Onput:
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
drop table lz_dim_ep_hjlj_info;
create external table lz_dim_ep_hjlj_info
(
    thedate             bigint comment '日期',
    logkey              string comment '黄金令箭',
    biz_name            string comment '业务描述',
    page_name           string comment '所在页面',
    gokeys              string comment '黄金令箭业务关键key'
)
partitioned by  (dt string, logsrc string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as textfile
location '${hdfs_base_dir}/hive/dim/lz_dim_ep_hjlj_info';

EOF

