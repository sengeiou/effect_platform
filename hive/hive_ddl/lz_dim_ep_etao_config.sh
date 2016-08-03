#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：
# 邮箱：@taobao.com
# 日期：
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_dim_ep_etao_config;

create external table lz_dim_ep_etao_config
(
   plan_id              bigint,
   user_id              bigint,
   report_name          string,
   report_type          bigint,
   channel_type         bigint,
   channel_id           bigint,
   channel_name         string,
   channel_rule         string,
   src_type             string,
   ind_ids              string,
   expire_type          bigint,
   expire_date          bigint,
   status               bigint,
   ctime                bigint,
   mtime                bigint,
   approval             string
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_dim_dir}/lz_dim_ep_etao_config';

EOF
