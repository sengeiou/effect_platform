#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# != 0 ];then
    ydate=$1
fi
source $HOME/linezing/effect_platform/config/set_env.conf

##########################################################################
# Owner:feiqiong.dpf
# Email:feiqiong.dpf@taobao.com
# Date:2012-11-22
# ------------------------------------------------------------------------
# Description: etao频道规则表
# Input: datax 
# Onput: lz_dim_ep_etao_config
# ------------------------------------------------------------------------
# ChangeLog:
##########################################################################

$Hive <<EOF
alter table lz_dim_ep_etao_config drop partition (dt='${ydate}');
alter table lz_dim_ep_etao_config add partition (dt='${ydate}') 
location '${hadoop_base_dir}/conf';
EOF
if [ $? -ne 0 ];then
    exit 2
fi

