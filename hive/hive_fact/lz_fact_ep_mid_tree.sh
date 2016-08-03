#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
runner_job=0
if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    runner_job=$2    
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# 作者：shicheng
# 邮箱：shicheng@taobao.com
# 日期：2012-06-11
# ------------------------------------------------------------------------
# 功能：
# 上游：lz_fact_ep_ad_click_log
# ------------------------------------------------------------------------
#
##########################################################################

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

mid_path=${hadoop_base_dir}/effect_platform_mid/${ydate}

config_list=${config_temp_dir}/*/*/report_*.xml
echo $config_list

counter=0
for config_file in $config_list
do
counter=`expr $counter + 1`
file_name=`echo ${config_file} | awk -F/ '{print $NF}'`
plan_id=`echo $file_name | awk -F_ '{print $NF}' | awk -F. '{print $1}'`

$Hive <<EOF
alter table lz_fact_ep_mid_tree drop partition (dt=${ydate}, planId=${plan_id} );
alter table lz_fact_ep_mid_tree add partition (dt=${ydate}, planId=${plan_id}) location '${mid_path}/1/none/${plan_id}';
EOF
done 


exit 0
