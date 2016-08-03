#!/bin/bash
ydate=`date -d -1days +%Y%m%d`
if [ $# == 1 ];then
    ydate=$1
fi
Dir=`dirname $0`
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：nanjia
# 邮箱：nanjia.lj@taobao.com
# 日期：20120604
# ------------------------------------------------------------------------
# 功能：生成aplus对应的月光宝盒浏览日志。其中usefule_extra=adidxxx , 
#       extra顺序为：ip, agent, amid, cmid, pmid, channelid
# 上游：ds_fdi_atplog_base 
# ------------------------------------------------------------------------

$Hive <<EOF
CREATE TEMPORARY FUNCTION date_format AS 'com.taobao.hive.udf.UDFDateFormat';
CREATE TEMPORARY FUNCTION getValueFromUrl  AS 'com.taobao.hive.udf.UDFGetValueFromUrl';
insert overwrite table lz_fact_ep_browse_log partition (dt=${ydate}, logsrc='aplus')
select
    unix_timestamp(visit_datetime, 'yyyyMMddHHmmss') as time_stamp,
    coalesce(url, '') as url,
    coalesce(pre_url, '') as refer_url,
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
        concat_ws('\003', 'adid', coalesce(split(adid,"_")[0], '')),
        concat_ws('\003', 'ali_refid', coalesce(ali_refid, '')),
        concat_ws('\003', 'ali_trackid', coalesce(ali_trackid, '')),
        concat_ws('\003', 'pmid', coalesce(pmid, ''))) as useful_extra,
    '' as extra
from
    ds_fdi_atplog_base 
where
    pt = '${ydate}000000'
    and log_type = '1'
    and unix_timestamp(visit_datetime, 'yyyyMMddHHmmss') is not null
;
EOF
if [ $? -ne 0 ];then
    exit 2
fi
