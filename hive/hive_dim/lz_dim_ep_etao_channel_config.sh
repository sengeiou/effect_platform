#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# != 0 ];then
    ydate=$1
fi
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
insert overwrite table lz_dim_ep_etao_channel_config partition(dt=${ydate}) 
select
   channel_name,
   channel_rule,
   channel_type,
   channel_id 
from lz_dim_ep_etao_config
where dt=${ydate} and channel_type=1
group by
   channel_name,
   channel_rule,
   channel_type,
   channel_id 
;
EOF
if [ $? -ne 0 ];then
    exit 2
fi

