#!/bin/bash
##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012/10/21
# ------------------------------------------------------------------------
# Description:无线一阳指配置load
# Input: lz_dim_ep_wireless_yyz
# Output:
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
if [ $# -eq 1 ];then
    ydate=$1
fi
source $HOME/linezing/effect_platform/config/set_env.conf

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi


src="${hive_dim_dir}/lz_dim_ep_wireless_yyz/dt=${ydate}"
dest_dir="$rank_temp_dir/${ydate}"
dest_file="$dest_dir/lz_dim_ep_wireless_yyz/lz_dim_ep_wireless_yyz.dat"

rm -rf $dest_dir
mkdir -p $dest_dir/lz_dim_ep_wireless_yyz

echo $Hadoop fs -text $src/* > $dest_file
$Hadoop fs -text $src/* > $dest_file

if [ $? -ne 0 ]
then
    exit 1;
fi

echo touch $dest_dir/lz_dim_ep_wireless_yyz.finish
touch $dest_dir/lz_dim_ep_wireless_yyz.finish

if [ $? -ne 0 ]
then
    exit 1;
fi
exit 0;
