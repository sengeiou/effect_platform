#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
period=11
if [ $# == 1 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# 作者：yiyun
# 邮箱：yiyun@taobao.com
# 日期：2012-06-16
# ------------------------------------------------------------------------
# 功能：
# 上游：lz_rpt_ep_ad_click_info
# ------------------------------------------------------------------------
#
##########################################################################

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

src="${hive_rpt_dir}/lz_rpt_ep_ad_click_info/dt=${ydate}"
dest_dir="$rank_temp_dir/rpt/lz_rpt_ep_ad_click_info/${ydate}/"
dest_file="$dest_dir/lz_rpt_ep_ad_click_info"
#xmls=${config_temp_dir}/*/*/report_*.xml
xmls_path=${config_temp_dir}/$period/none/

rm -rf $dest_dir
mkdir -p $dest_dir

echo $Hadoop dfs -getmerge $src $dest_file
$Hadoop dfs -getmerge $src $dest_file

if [ $? -ne 0 ];then
    exit 1
fi

echo java -cp $HOME/linezing/effect_platform/hadoop/conf/:$HOME/linezing/effect_platform/hadoop/target/lib/*:$HOME/linezing/effect_platform/hadoop/target/EffectPlatform.jar com.ali.lz.effect.tools.hbase.RptToHBase $dest_file $xmls_path effect_rpt_adclk ${ydate}

java -cp $HOME/linezing/effect_platform/hadoop/conf/:$HOME/linezing/effect_platform/hadoop/target/lib/*:$HOME/linezing/effect_platform/hadoop/target/EffectPlatform.jar com.ali.lz.effect.tools.hbase.RptToHBase $dest_file $xmls_path effect_rpt_adclk ${ydate}

if [ $? -ne 0 ];then
    exit 1
fi
