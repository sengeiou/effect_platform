#!/bin/bash
##########################################################################
# Owner:nanjia.lj
# Email:nanjia.lj@taobao.com
# Date:2013/01/29
# ------------------------------------------------------------------------
# Description:无线客户端日志染色及成交归属
# Input:lz_fact_ep_browse_log, lz_fact_ep_trade_info
# Output:lz_fact_ep_client_ownership
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=500
# 成交数据周期：
# 11（1天gmv，1天alipay），13（1天gmv，3天alipay），
period=11

if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    reduce_num=$2    
elif [ $# == 3 ];then
    ydate=$1
    reduce_num=$2    
    period=$3
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

input=/group/tbads/sds/linezing/effect_platform/hive/fact/lz_fact_ep_browse_log/dt=${ydate}/logsrc=client/*
gmv_path=/group/tbads/sds/linezing/effect_platform/hive/fact/lz_fact_ep_trade_info/dt=${ydate}/round=${period}/logsrc=wireless/*
output=${hadoop_base_dir}/lz_fact_ep_client_ownership/${ydate}
Launcher=com.ali.lz.effect.ownership.wirelssclient.EffectClientRunner
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
#LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar,${EP_HADOOP_DIR}/lib/commons-collections-3.1.jar,\
#${EP_HADOOP_DIR}/lib/jdom-1.1.jar

config_file=${HOME}/linezing/effect_platform/config/client_config.xml

$Hadoop dfs -rmr $output

echo $Hadoop jar -files ${config_file} $MAIN_JAR $Launcher $input $output \
1 $reduce_num gmv=$gmv_path $jobid $joblevel 

$Hadoop jar -files ${config_file} $MAIN_JAR $Launcher $input $output \
1 $reduce_num gmv=$gmv_path $jobid $joblevel 

if [ $? -ne 0 ];then
    exit 1
fi
$Hive <<EOF
alter table lz_fact_ep_client_ownership drop partition (dt=${ydate});
alter table lz_fact_ep_client_ownership add partition (dt=${ydate}) location '$output';
EOF
         
if [ $? -ne 0 ];then
    exit 1
fi
