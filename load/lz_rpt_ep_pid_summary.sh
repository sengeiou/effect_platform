#!/bin/bash
##########################################################################
# Owner:nanjia.lj
# Email:nanjia.lj@taobao.com
# Date:2012/12/19
# ------------------------------------------------------------------------
# Description:月光宝盒联盟活动效果汇总表load
# Input: lz_rpt_ep_pid_summary
# Output:
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

ydate=`date -d -1days +%Y%m%d`
if [ $# -eq 1 ];then
    ydate=$1
fi
db_num=30000000
month=`date -d "$ydate" +%Y%m`
day=`date -d "$ydate" +%d`

source $HOME/linezing/effect_platform/config/set_env.conf

python ${BranchData} lz_rpt_ep_pid_summary ${hive_rpt_dir} ${rank_temp_dir} \
-bsp --noflag --field_pos=2 --range=${db_num} -d ${ydate} -o lz_rpt_ep_pid_summary

if [ $? -eq 0 ]
then    
    flag_time=`ls ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/0001/lz_rpt_ep_pid_summary.???? |cut -d. -f2`
    rm -rf ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/0000    
    mv ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/0001 ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/0000    
    if [ $? -eq 0 ]
    then
        touch ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/lz_rpt_ep_pid_summary.finish
        touch ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_summary/lz_rpt_ep_pid_summary.${flag_time}.flag
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
