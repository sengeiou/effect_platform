#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
# 选择MR任务执行阶段，0表示依次执行染色建树和效果归属两个阶段任务，1表示仅执行染色建树任务
runner_job=0
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300
# 需扫描的report文件id，默认为*，扫描所有文件，测试时可传指定id号，例如传值1，则仅扫描report_1.xml文件
planid='26'

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
fi

source $HOME/linezing/effect_platform/config/set_env.conf

BASE="$HOME/linezing/effect_platform";

###### hive ddl #################################
#sh $BASE/hive/hive_ddl/lz_fact_ep_ad_click_info.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_ad_click_log.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_ad_config.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_browse_log.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_common_index.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_detail_index.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_summary_common_index.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_summary_detail_index.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_ownership.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_fact_ep_trade_info.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_rpt_ep_ad_click_info.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_rpt_ep_detail.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_ddl/lz_rpt_ep_summary.sh
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#
#hivesql synctab s_dw_etao_cps_order $ydate $ydate
hivesql synctab s_dw_sh_etao_pay_detail $ydate $ydate
hivesql synctab r_atpanel_log $ydate $ydate
hivesql synctab r_gmv_alipay $ydate $ydate
hivesql synctab lz_dim_sellers $ydate $ydate
#
###### hive fact #################################
sh $BASE/hive/hive_fact/lz_fact_ep_browse_log.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_trade_info.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_etao_outside_order.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_ownership.sh $ydate $runner_job $reduce_num $planid
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_ownership_ext.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
#sh $BASE/hive/hive_fact/lz_fact_ep_etao_tree.sh $ydate
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_outside_trade_ownership.sh $ydate $reduce_num $planid
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_common_index.sh $ydate
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_detail_index.sh $ydate
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_summary_common_index.sh $ydate
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_summary_detail_index.sh $ydate
#if [ $? -ne 0 ]; then
#        exit 1
#fi
#sh $BASE/hive/hive_fact/lz_fact_ep_etao_cps_ownership.sh $ydate 
#if [ $? -ne 0 ]; then
#        exit 1
#fi


##### hive rpt ###################################
sh $BASE/hive/hive_rpt/lz_rpt_ep_detail.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_rpt/lz_rpt_ep_summary.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_rpt/lz_rpt_ep_summary_bysrc.sh $ydate
if [ $? -ne 0 ]; then
        exit 1
fi


