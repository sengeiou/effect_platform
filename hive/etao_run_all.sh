#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
# reduce数，默认300，测试时可传值1，加快任务执行速度
reduce_num=300
if [ $# == 1 ];then
    ydate=$1
elif [ $# == 2 ];then
    ydate=$1
    reduce_num=$2
fi

source $HOME/linezing/effect_platform/config/set_env.conf

BASE="$HOME/linezing/effect_platform";
hivewrapper='python /home/taobao/hivewrapper/hivewrapper.py'
##### hive ddl #################################
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_trade_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_cps_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_inside_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_outside_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_channel_visit_effect.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_cps_order.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_cps_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_inside_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_outside_order.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_outside_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_trade_ownership.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_tree.sh
#sh $BASE/hive/hive_ddl/lz_fact_ep_etao_visit_effect.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_channel_bysrc.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_channel.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_summary_bysrc.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_summary.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_tblm.sh
#sh $BASE/hive/hive_ddl/lz_rpt_ep_etao_tbmarket.sh

hivesql synctab s_dw_etao_cps_order $ydate $ydate
hivesql synctab lz_fact_ep_etao_outside_order $ydate $ydate
hivesql synctab lz_fact_ep_browse_log $ydate $ydate
hivesql synctab lz_fact_ep_trade_info $ydate $ydate

##### hive fact #################################
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_cps_order.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
sh $BASE/hive/hive_fact/lz_fact_ep_etao_tree.sh $ydate $reduce_num
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_inside_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_outside_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_cps_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_inside_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_outside_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_cps_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_visit_effect.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_visit_effect.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_trade_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_trade_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_fact/lz_fact_ep_etao_channel_ownership.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi

### hive rpt ###################################
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_summary.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_summary_bysrc.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_tbmarket.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_tblm.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_channel.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi
$hivewrapper $BASE/hive/hive_rpt/lz_rpt_ep_etao_channel_bysrc.sql -d $ydate
if [ $? -ne 0 ]; then
        exit 1
fi


