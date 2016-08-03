-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/10/21
-- # ------------------------------------------------------------------------
-- # Description:无线WAP日志ETL
-- # Input:s_wireless_loginsid, s_ods_wireless_aplus
-- # Output:lz_fact_ep_browse_log
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import udf:url_decode;
set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.dynamic.partition=true;
insert overwrite table lz_fact_ep_wireless_log partition (dt=${date}, logtype)
select     
    unix_timestamp(parse_time, 'yyyyMMddHHmmss') as time_stamp,
    coalesce(url, '') as url,
    coalesce(pre, '') as refer_url,
    coalesce(shop_id, '') as shop_id,
    coalesce(item_id, '') as auction_id,
    case when uid rlike '^[0-9]{2,}$' then uid else '' end as user_id,
    cna as cookie,
    '' as session,
    uid_mid as visit_id,
    concat_ws('\003', 'platform_id', case when url_decode(gokey) like '%wp=aXBob25l%' then '2' else '1' end) as useful_extra,
    '' as extra,
    case 
        when url_type = 'ipv'
        then 'ipv' 
        else 'others' 
    end as logtype
from
    s_wap_aplus_wireless
where
    ds = '${date}'
    and log_type = '1'
    and (typelog='wap-high' or typelog='wap-standard' or typelog='client')
;
