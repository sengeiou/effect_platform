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
drop table lz_dim_ep_etao_channel_config;

create external table lz_dim_ep_etao_channel_config
(
   channel_name         string,
   channel_rule         string,
   channel_type         bigint,
   channel_id           bigint
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_dim_dir}/lz_dim_ep_etao_channel_config';

EOF
