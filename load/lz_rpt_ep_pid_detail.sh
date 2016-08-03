#!/bin/bash
##########################################################################
# Owner:nanjia.lj
# Email:nanjia.lj@taobao.com
# Date:2012/12/19
# ------------------------------------------------------------------------
# Description:月光宝盒联盟效果明细表load
# Input: lz_rpt_ep_pid_detail
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

python ${BranchData} lz_rpt_ep_pid_detail ${hive_rpt_dir} ${rank_temp_dir} \
-bsp --noflag --field_pos=2 --range=${db_num} -d ${ydate} -o lz_rpt_ep_pid_detail

if [ $? -eq 0 ]
then    
    flag_time=`ls ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/0001/lz_rpt_ep_pid_detail.???? |cut -d. -f2`
    rm -rf ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/0000    
    mv ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/0001 ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/0000    
    if [ $? -eq 0 ]
    then
        touch ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/lz_rpt_ep_pid_detail.finish
        touch ${rank_temp_dir}/$month/$day/lz_rpt_ep_pid_detail/lz_rpt_ep_pid_detail.${flag_time}.flag
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
