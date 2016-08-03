#!/bin/bash
##########################################################################
# 作者：shicheng
# 邮箱：shicheng@taobao.com
# 日期：2012-06-11
# 修改：feiqiong.dpf
# 日期：2012-08-30
# ------------------------------------------------------------------------
# 功能：
# 上游：lz_fact_ep_browse_log, lz_fact_ep_trade_info, lz_fact_ep_cart_info, lz_fact_ep_collect_info
# ------------------------------------------------------------------------
#
##########################################################################

ydate=`date -d -1days +%Y%m%d`

# 选择MR任务执行阶段，0表示依次执行染色建树和效果归属两个阶段任务，1表示仅执行染色建树任务
runner_job=0
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=500
# 需扫描的report文件id，默认为*，扫描所有文件，测试时可传指定id号，例如传值1，则仅扫描report_1.xml文件
planid='*'
## 成交数据周期：11（1天gmv，1天alipay），13（1天gmv，3天alipay），17（1天gmv，7天alipay），33（3天gmv，3天alipay）
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
    planid=$4
elif [ $# == 5 ];then
    ydate=$1
    runner_job=$2
    reduce_num=$3
    planid=$4
    period=$5    
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
output=${hadoop_base_dir}/lz_fact_ep_ownership/${ydate}
mid_path=${hadoop_base_dir}/effect_platform_mid/${ydate}
gmv_path=${hive_fact_dir}/lz_fact_ep_trade_info/dt=${ydate}/round=${period}/logsrc=taobao/*
#gmv_path=/group/tbads/sds/linezing/effect_platform/hive/fact/lz_fact_ep_trade_info/dt=${ydate}/round=1/logsrc=taobao/*
#cart_path=${hive_fact_dir}/lz_fact_ep_cart_info/dt=${ydate}/round=1/logsrc=taobao/*
outside_path=${hive_fact_dir}/lz_fact_ep_etao_outside_order/dt=${ydate}
Launcher=com.ali.lz.effect.ownership.EffectMRRunner
MAIN_JAR=${EP_HADOOP_DIR}/EffectPlatform.jar
#LIB_JARS=${EP_HADOOP_DIR}/lib/protobuf-java-2.4.1.jar,${EP_HADOOP_DIR}/lib/commons-collections-3.1.jar

if [ ! -d "${config_temp_dir}/$period" -o "`ls -A ${config_temp_dir}/$period/*/report_*.xml`" = "" ];then
    exit 0
fi
i=0
for config_file in ${config_temp_dir}/$period/*/report_$planid.xml
do
    file_name=${config_file##*/}
    plan_id=`echo $file_name | awk -F_ '{print $NF}' | awk -F. '{print $1}'`

    config_files=${config_files}","${config_file}
    file_names=${file_names}","${file_name}
    plan_ids=${plan_ids}' '${plan_id}
    i=`expr $i + 1`
if [ $i -ge 5 ];then
    ###### process 5 report files in each MR job #######
    echo $i
    config_files=${config_files#*,}
    file_names=${file_names#*,}

    echo $Hadoop jar -files ${config_files} $MAIN_JAR $Launcher $input $output ${file_names} 1 $reduce_num $mid_path gmv=$gmv_path outside_order=$outside_path runner_job=$runner_job files=${config_files} $jobid $joblevel 

    $Hadoop jar -files ${config_files} $MAIN_JAR $Launcher $input $output ${file_names} 1 $reduce_num $mid_path gmv=$gmv_path outside_order=$outside_path runner_job=$runner_job files=${config_files} $jobid $joblevel 
    if [ $? -ne 0 ];then
        exit 1
    fi
for plan_id in $plan_ids
do
$Hive <<EOF
alter table lz_fact_ep_mid_tree drop partition (dt=${ydate}, planId=${plan_id});
alter table lz_fact_ep_mid_tree add partition (dt=${ydate}, planId=${plan_id}) location '$mid_path/${plan_id}';

alter table lz_fact_ep_ownership drop partition (dt=${ydate}, type="inside", planId=${plan_id} );
alter table lz_fact_ep_ownership add partition (dt=${ydate}, type="inside", planId=${plan_id}) location '${output}/inside/${plan_id}';
alter table lz_fact_ep_ownership drop partition (dt=${ydate}, type="outside", planId=${plan_id} );
alter table lz_fact_ep_ownership add partition (dt=${ydate}, type="outside", planId=${plan_id}) location '${output}/outside/${plan_id}';
EOF
         
if [ $? -ne 0 ];then
    exit 1
fi
done

    i=0
    file_name=''
    plan_id=''
    config_files=''
    file_names=''
    plan_ids=''
fi        

done

##### 处理剩余report文件 #######
if [[ $config_files != '' ]] && [[ $i -gt 0 ]];then
config_files=${config_files#*,}
file_names=${file_names#*,}

echo $Hadoop jar -files ${config_files} $MAIN_JAR $Launcher $input $output ${file_names} 1 $reduce_num $mid_path gmv=$gmv_path cart=$cart_path outside_order=$outside_path runner_job=$runner_job files=${config_files} $jobid $joblevel 

$Hadoop jar -files ${config_files} $MAIN_JAR $Launcher $input $output ${file_names} 1 $reduce_num $mid_path gmv=$gmv_path outside_order=$outside_path runner_job=$runner_job files=${config_files} $jobid $joblevel 
if [ $? -ne 0 ];then
    exit 1
fi
for plan_id in $plan_ids
do
$Hive <<EOF
alter table lz_fact_ep_mid_tree drop partition (dt=${ydate}, planId=${plan_id});
alter table lz_fact_ep_mid_tree add partition (dt=${ydate}, planId=${plan_id}) location '$mid_path/${plan_id}';

alter table lz_fact_ep_ownership drop partition (dt=${ydate}, type="inside", planId=${plan_id} );
alter table lz_fact_ep_ownership add partition (dt=${ydate}, type="inside", planId=${plan_id}) location '${output}/inside/${plan_id}';
alter table lz_fact_ep_ownership drop partition (dt=${ydate}, type="outside", planId=${plan_id} );
alter table lz_fact_ep_ownership add partition (dt=${ydate}, type="outside", planId=${plan_id}) location '${output}/outside/${plan_id}';
EOF
     
if [ $? -ne 0 ];then
exit 1
fi
done
fi
