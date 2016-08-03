#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
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
# ------------------------------------------------------------------------
#
##########################################################################

$Hive <<EOF
set hive.exec.compress.output=false;
insert overwrite table lz_rpt_ep_spm_info partition (dt='${ydate}')
select
    spm_id,
    position,
    title
from (
    select
        site_id as spm_id,
        cast(1 as bigint) as position,
        site_name as title
    from s_spm_site
    where pt='${ydate}000000'

    union all

    select
        concat_ws(".", site_id, page_id) as spm_id,
        cast(2 as bigint) as position,
        page_name as title
    from s_spm_page
    where pt='${ydate}000000'

    union all

    select
        module_id as spm_id,
        cast(3 as bigint) as position,
        module_name as title
    from s_spm_module
    where pt='${ydate}000000'
) spm;
EOF
