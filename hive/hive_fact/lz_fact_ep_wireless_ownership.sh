#!/bin/bash
##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012/10/25
# ------------------------------------------------------------------------
# Description:无线日志建树染色及成交归属
# Input:lz_fact_ep_browse_log, lz_fact_ep_trade_info, lz_dim_ep_wireless_config
# Output:lz_fact_ep_wireless_tree, lz_fact_ep_wireless_ownership
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
# 选择MR任务执行阶段:
# 0表示依次执行染色建树和效果归属两个阶段任务，1表示仅执行染色建树任务
runner_job=0
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300
# 成交数据周期：
# 11（1天gmv，1天alipay），13（1天gmv，3天alipay），
# 17（1天gmv，7天alipay），33（3天gmv，3天alipay）
period=11

if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    runner_job=$2
elif [ $# == 3 ];then
    ydate=$1
    runner_job=$2
    reduce_num=$3    
elif [ $# == 4 ];then
    ydate=$1
    runner_job=$2
    reduce_num=$3    
    period=$4
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

input=${hive_fact_dir}/lz_fact_ep_browse_log/dt=${ydate}/logsrc=wireless/*
mid_path=${hadoop_base_dir}/lz_fact_ep_wireless_tree/${ydate}
gmv_path=${hive_fact_dir}/lz_fact_ep_trade_info/dt=${ydate}/round=${period}/logsrc=wireless/*
#gmv_path=/group/tbads/sds/linezing/effect_platform/hive/fact/lz_fact_ep_trade_info/dt=${ydate}/round=${period}/logsrc=wireless/*
output=${hadoop_base_dir}/lz_fact_ep_wireless_ownership/${ydate}
config_path=${hive_dim_dir}/lz_dim_ep_wireless_config/dt=${ydate}/
#config_path=/group/tbads/sds/linezing/effect_platform/hive/dim/lz_dim_ep_wireless_config/dt=${ydate}/
Launcher=com.ali.lz.effect.ownership.wireless.EffectWirelessRunner
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
#LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar,${EP_HADOOP_DIR}/lib/commons-collections-3.1.jar,\
#${EP_HADOOP_DIR}/lib/recollection-0.1.jar

$Hadoop dfs -rmr $mid_path
$Hadoop dfs -rmr $output

echo $Hadoop jar $MAIN_JAR $Launcher $input $output \
$config_path 1 $reduce_num $mid_path gmv=$gmv_path runner_job=$runner_job $jobid $joblevel 

$Hadoop jar $MAIN_JAR $Launcher $input $output \
$config_path 1 $reduce_num $mid_path gmv=$gmv_path runner_job=$runner_job $jobid $joblevel 

if [ $? -ne 0 ];then
    exit 1
fi
$Hive <<EOF
alter table lz_fact_ep_wireless_tree drop partition (dt=${ydate});

alter table lz_fact_ep_wireless_tree add partition (dt=${ydate}) location '$mid_path';

alter table lz_fact_ep_wireless_ownership drop partition (dt=${ydate});

alter table lz_fact_ep_wireless_ownership add partition (dt=${ydate}) location '$output';
EOF
         
if [ $? -ne 0 ];then
    exit 1
fi
