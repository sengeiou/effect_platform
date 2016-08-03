#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
# 选择MR任务执行阶段，0表示依次执行染色建树和效果归属两个阶段任务，1表示仅执行染色建树任务
runner_job=0
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300
period='11'
if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    runner_job=$2    
elif [ $# == 3 ];then
    ydate=$1
    runner_job=$2
    reduce_num=$3
fi

source $HOME/linezing/effect_platform/config/set_env.conf

BASE="$HOME/linezing/effect_platform";
hivewrapper='python /home/taobao/hivewrapper/hivewrapper.py'

##### hive ddl #################################
#sh $BASE/hive/hive_ddl/lz_fact_ep_wireless_tree.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_dim_ep_wireless_yyz.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_wireless_ownership.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_rpt_ep_wireless_detail.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_rpt_ep_wireless_summary.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#
hivesql synctab s_ods_wireless_total $ydate $ydate
hivesql synctab s_wireless_loginsid $ydate $ydate
hivesql synctab wireless_dim_yyz_page $ydate $ydate
hivesql synctab ds_fdietao_wap_gmv_alipay $ydate $ydate
hivesql synctab lz_dim_sellers $ydate $ydate
###### hive fact #################################
$hivewrapper $BASE/hive/hive_dim/lz_dim_ep_wireless_yyz.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_wireless_log.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_wireless_trade_info.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_wireless_ownership.sh $ydate $runner_job $reduce_num 
if [ $? -ne 0 ]; then
        exit 1
fi

##### hive rpt ###################################
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_wireless_detail.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_wireless_summary.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi

##### load table ###################################
sh $BASE/load/lz_dim_ep_wireless_yyz.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/load/lz_rpt_ep_wireless_detail.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/load/lz_rpt_ep_wireless_summary.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi



