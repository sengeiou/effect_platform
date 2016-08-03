#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# == 1 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：feiqiong.dpf
# 邮箱：feiqiong.dpf@taobao.com
# 日期：20120604
# ------------------------------------------------------------------------
# 功能：生成aplus对应的月光宝盒页面点击(黄金令箭)日志。其中usefule_extra=adidxxx , 
#       extra顺序为：ip, agent, amid, cmid, pmid, channelid
#       考虑到计算压力，只ETL出在黄金令箭埋点维表中的日志
# 上游：ds_fdi_atplog_base 
# ------------------------------------------------------------------------

$Hive <<EOF
add jars ${HOME}/linezing/tools/hive_udf/p4plz_udf.jar;
create temporary function paramsFilter as 'com.taobao.lzdata.hive.udf.ParamsFilter';

insert overwrite table lz_fact_ep_browse_log partition (dt=${ydate}, logsrc='tmall_hjlj')
select /*+ mapjoin(b) */
    unix_timestamp(visit_datetime, 'yyyyMMddHHmmss') as time_stamp,
    coalesce(url, '') as url,
    '' as refer_url,
    case when shop_nid = '-' or shop_nid is null
        then '' else shop_nid 
        end as shop_id,
    case
        when auction_nid = '-' or auction_nid is null
        then '' else auction_nid
        end as auction_id,
    case
        when uid = '0' or uid = '-' or uid is null 
        then '' else uid 
        end as user_id,
    coalesce(mid, '') as cookie,
    split(session_id,"_")[0] as session,
    coalesce(visitor_id, '') as visit_id,
    concat_ws('\002', 
        concat_ws('\003', 
                'logkey', 
                concat('logkey=', coalesce(b.logkey, ''), '&', 
                    paramsFilter(refer, split(gokeys, '\002'), true )     ))) as useful_extra,
    '' as extra
from
    ds_fdi_atplog_base a
    join lz_dim_ep_hjlj_info b
    on 
        a.pt = '${ydate}000000'
        and a.log_type = '2'
        and a.company = 'b'
        and b.dt='${ydate}'
        and substring(a.logkey, 2)=b.logkey
where
    unix_timestamp(a.visit_datetime, 'yyyyMMddHHmmss') is not null
    and length(paramsFilter(refer, split(gokeys, '\002'), true))>0
;
EOF
if [ $? -ne 0 ];then
    exit 2
fi
