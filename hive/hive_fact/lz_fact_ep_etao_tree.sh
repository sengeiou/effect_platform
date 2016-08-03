#!/bin/bash
##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012/11/25
# ------------------------------------------------------------------------
# Description: Build etao visit tree
# Input:lz_fact_ep_browse_log, lz_dim_ep_etao_config
# Output:lz_fact_ep_etao_tree 
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300

if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    reduce_num=$2    
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

input=${hive_fact_dir}/lz_fact_ep_browse_log/dt=${ydate}/logsrc=aplus/*
#input=/group/tbads/sds/linezing/effect_platform/hive/fact/lz_fact_ep_browse_log/dt=${ydate}/logsrc=aplus/*
output=${hadoop_base_dir}/lz_fact_ep_etao_tree/${ydate}
config_path=/group/tbads/sds/linezing/effect_platform/hadoop/conf/
Launcher=com.ali.lz.effect.ownership.etao.EffectETaoTreeRunner
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
#LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar,\
#${EP_HADOOP_DIR}/lib/commons-collections-3.1.jar

$Hadoop dfs -rmr $output

echo $Hadoop jar $MAIN_JAR $Launcher $input $output $config_path 1 $reduce_num $jobid $joblevel 
$Hadoop jar $MAIN_JAR $Launcher $input $output $config_path 1 $reduce_num $jobid $joblevel 

if [ $? -ne 0 ];then
    exit 1
fi

$Hive <<EOF
alter table lz_fact_ep_etao_tree drop partition (dt=${ydate}, nodetype="ipv");
alter table lz_fact_ep_etao_tree add partition (dt=${ydate}, nodetype="ipv") location '$output/ipv';
alter table lz_fact_ep_etao_tree drop partition (dt=${ydate}, nodetype="shop");
alter table lz_fact_ep_etao_tree add partition (dt=${ydate}, nodetype="shop") location '$output/shop';
alter table lz_fact_ep_etao_tree drop partition (dt=${ydate}, nodetype="others");
alter table lz_fact_ep_etao_tree add partition (dt=${ydate}, nodetype="others") location '$output/others';
EOF
         
if [ $? -ne 0 ];then
    exit 1
fi
