#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
period=11
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

input="${hive_fact_dir}/lz_fact_ep_ad_click_log/dt=${ydate}/logsrc=aplus/*"
output=${hadoop_base_dir}/${ydate}/lz_fact_ep_ad_click_info
Launcher=com.ali.lz.effect.ownership.EffectAdClick
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar,${EP_HADOOP_DIR}/lib/commons-collections-3.1.jar

# 处理变量
for config_file in ${config_temp_dir}/$period/*/report_*.xml
do
    file_name=${config_file##*/}
    config_files=${config_files}","${config_file}
    file_names=${file_names}","${file_name}
done
config_files=${config_files#*,}
file_names=${file_names#*,}


#执行MR程序
$Hadoop dfs -mkdir $output
$Hadoop dfs -rmr $output
echo $Hadoop jar -libjars $LIB_JARS -files ${config_files} $MAIN_JAR $Launcher $input $output 1 300 ${file_names} $jobid $joblevel
$Hadoop jar -libjars $LIB_JARS -files ${config_files} $MAIN_JAR $Launcher $input $output 1 300 ${file_names} $jobid $joblevel
if [ $? -ne 0 ];then
    exit 1
fi

$Hive <<EOF
alter table lz_fact_ep_ad_click_info drop partition (dt=${ydate});
alter table lz_fact_ep_ad_click_info add partition (dt=${ydate}) location '${output}';
EOF

if [ $? -ne 0 ];then
    exit 1
fi
