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
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
CREATE TEMPORARY FUNCTION date_format AS 'com.taobao.hive.udf.UDFDateFormat';
CREATE TEMPORARY FUNCTION getDomainFromUrl  AS 'com.taobao.hive.udf.UDFGetDomainFromUrl';
CREATE TEMPORARY FUNCTION getValueFromUrl  AS 'com.taobao.hive.udf.UDFGetValueFromUrl';

insert overwrite table lz_fact_ep_ad_click_log partition (dt=${ydate}, logsrc='aplus')
select
    '1.0' as log_version,
    date_format(logtime, 'yyyyMMddHHmmss', 'yyyyMMdd') as thedate,
    unix_timestamp(logtime, 'yyyyMMddHHmmss') as time_stamp,
    url as url,
    url_pre as refer_url,
    case 
        when uid = '-' or uid is null 
        then mid 
        else uid 
    end as uid_mid,
    case
        when getValueFromUrl(refer, "at_autype") != ""
        then coalesce(split(getValueFromUrl(refer, "at_autype"),"_")[1] , split(getValueFromUrl(refer, "at_autype"),"%5f")[1], split(getValueFromUrl(refer, "at_autype"),"%5F")[1] )
        when getValueFromUrl(refer, "at_shoptype") != ""
        then coalesce(split(getValueFromUrl(refer, "at_shoptype"),"%5f")[1] , split(getValueFromUrl(refer, "at_shoptype"),"%5F")[1], split(getValueFromUrl(refer, "at_shoptype"),"_")[1] ) 
        else "" 
    end as shop_id,
    auctionid as auction_id,
    ip as ip,
    mid as mid,
    case
        when uid = '-' or uid is null 
        then '' 
        else uid 
    end as uid,
    split(sid,"_")[0] as sid,
    '' as aid,
    agent as agent,
    case 
        when split(adid,"_")[0] = '-' or split(adid,"_")[0] is null           
        then '' else split(adid,"_")[0]                                       
    end as adid,
    amid as amid,
    cmid as cmid,
    pmid as pmid,
    '' as nmid,
    '' as nuid,
    channelid as channelid
from
    s_web_log_search
where
    ds='${ydate}' and refertype='other_type' and urltype='other_urltype'
    and getDomainFromUrl(url, 2) =  'ju.atpanel.com'
    and url like '%tb_market_id%'
    and length(adid)>1
;
EOF
if [ $? -ne 0 ];then
    exit 2
fi
