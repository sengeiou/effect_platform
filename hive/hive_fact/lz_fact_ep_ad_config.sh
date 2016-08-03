#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# == 1 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# 作者：shicheng
# 邮箱：shicheng@taobao.com
# 日期：2012-06-11
# ------------------------------------------------------------------------
# 功能：
# 上游：r_act_media_adid_site
# ------------------------------------------------------------------------
#
##########################################################################

$Hive <<EOF
insert overwrite table lz_fact_ep_ad_config partition (dt='${ydate}')
select
    adid,
    site_name,
    page_name,
    resource_name,
    channel_name,
    act_name,
    act_id
from ds_act_ddf_adid 
where pt='${ydate}000000'
;
EOF
