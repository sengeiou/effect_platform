#!/bin/bash
##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012/10/21
# ------------------------------------------------------------------------
# Description:无线活动效果汇总表load
# Input: lz_rpt_ep_wireless_summary
# Output:
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
if [ $# -eq 1 ];then
    ydate=$1
fi
db_num=10000
month=`date -d "$ydate" +%Y%m`
day=`date -d "$ydate" +%d`

source $HOME/linezing/effect_platform/config/set_env.conf

python ${BranchData} lz_rpt_ep_wireless_summary ${hive_rpt_dir} ${rank_temp_dir} \
-bsp --noflag --field_pos=2 --range=${db_num} -d ${ydate} -o agg_collect_wireless_activity

if [ $? -eq 0 ]
then    
    flag_time=`ls ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/0001/agg_collect_wireless_activity.???? |cut -d. -f2`
    rm -rf ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/0000    
    mv ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/0001 ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/0000    
    if [ $? -eq 0 ]
    then
        touch ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/agg_collect_wireless_activity.finish
        touch ${rank_temp_dir}/$month/$day/agg_collect_wireless_activity/agg_collect_wireless_activity.${flag_time}.flag
        if [ $? -eq 0 ]
        then
            exit 0;
        else
            exit -1;
        fi
    else
        exit -1;
    fi
else    
    exit -1;
fi
