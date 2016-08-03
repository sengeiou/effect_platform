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
# 上游：lz_fact_ep_ad_click_info_temp 
# ------------------------------------------------------------------------
#
##########################################################################

$Hive <<EOF
set hive.exec.compress.output=false;
insert overwrite table lz_rpt_ep_ad_click_info partition (dt='${ydate}')
select
    plan_id,
    analyzer_id,
    ${ydate},
    adid,
    sum(pv) as pv,
    count(0) as uv
from lz_fact_ep_ad_click_info
where dt='${ydate}'
group by
    analyzer_id,
    plan_id,
    adid 
;
EOF
