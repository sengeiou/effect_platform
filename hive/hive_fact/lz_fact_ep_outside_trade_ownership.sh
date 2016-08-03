#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300
# 需扫描的report文件id，默认为*，扫描所有文件，测试时可传指定id号，例如传值1，则仅扫描report_1.xml文件
planid='*'

if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    reduce_num=$2
elif [ $# == 3 ];then
    ydate=$1
    reduce_num=$2
    planid=$3
fi

Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# 作者：feiqiong.dpf
# 邮箱：feiqiong.dpf@taobao.com
# 日期：2012-07-10
# ------------------------------------------------------------------------
# 功能：
# 上游：lz_fact_ep_etao_tree, lz_fact_ep_etao_outside_order
# ------------------------------------------------------------------------
#
##########################################################################

if [ "X$SKYNET_PRIORITY" != "X" ];then
    joblevel="mapred.job.level=$SKYNET_PRIORITY"
fi
if [ "X$SKYNET_ID" != "X" ];then
    jobid="mapred.job.skynet_id=$SKYNET_ID"
fi

output=${hadoop_base_dir}/lz_fact_ep_outside_trade_ownership/${ydate}
gmv_path=${hive_fact_dir}/lz_fact_ep_etao_outside_order/dt=$ydate
#/group/taobao/taobao/dw/stb/$ydate/order/
Launcher=com.ali.lz.effect.ownership.EffectOutsideTradeOwnership
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
#LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar

config_list=${config_temp_dir}/*/*/report_$planid.xml

$Hadoop dfs -mkdir $output

counter=0
for config_file in $config_list
do
counter=`expr $counter + 1`
{
file_name=`echo ${config_file} | awk -F/ '{print $NF}'`

plan_id=`echo $file_name | awk -F_ '{print $NF}' | awk -F. '{print $1}'`
input=${hive_fact_dir}/lz_fact_ep_etao_tree/dt=${ydate}/planid=$plan_id/
$Hadoop dfs -rmr $output/$plan_id

echo $Hadoop jar -files ${config_file} $MAIN_JAR $Launcher $input $output/$plan_id ${file_name} 1 $reduce_num gmv=$gmv_path $jobid $joblevel 

$Hadoop jar -files ${config_file} $MAIN_JAR $Launcher $input $output/$plan_id ${file_name} 1 $reduce_num gmv=$gmv_path $jobid $joblevel 

if [ $? -ne 0 ];then
    exit 1
fi
$Hive <<EOF

alter table lz_fact_ep_outside_trade_ownership drop partition (dt=${ydate}, planid=${plan_id});
alter table lz_fact_ep_outside_trade_ownership add partition (dt=${ydate}, planid=${plan_id}) location '${output}/${plan_id}';
EOF
 
if [ $? -ne 0 ];then
    exit 1
fi

} &
done 

#等待所有子进程跑完后，父进程exit
ret=0
for((i=1;i<=$counter;i++)) 
do
    wait %$i
    if [ $? -ne 0 ];then
        ret=`expr $ret + 1`
    fi
done
if [ $ret -ne 0 ];then
    exit 1
fi

exit 0
