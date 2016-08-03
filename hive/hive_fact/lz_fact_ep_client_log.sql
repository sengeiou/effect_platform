-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2013/01/23
-- # ------------------------------------------------------------------------
-- # Description:无线client日志ETL
-- # Input:r_wap_client_log_with_uid
-- # Output:lz_fact_ep_client_log
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

set hive.exec.dynamic.partition.mode=nostrick;
set hive.exec.dynamic.partition=true;
insert overwrite table lz_fact_ep_client_log partition (dt=${date}, logtype)
select     
    a.time_stamp
    ,a.url
    ,a.refer_url
    ,a.seller_id
    ,a.auction_id
    ,a.user_id
    ,a.cookie
    ,'' as session
    ,'' as visit_id
    ,'' as useful_extra
    ,a.extra as extra
    ,case 
        when length(a.auction_id) > 1 
        then 'ipv' 
        when length(a.seller_id) > 1 
        then 'shop' 
        else 'others' 
     end as logtype
from
(
    select
        server_time as time_stamp
        ,coalesce(page, '') as url
        ,coalesce(arg1, '') as refer_url
        ,coalesce(regexp_extract(args,'(sellerid|seller_id)=([\\d]+)',2),'') as seller_id
        ,coalesce(regexp_extract(args,'(itemid|item_id)=([\\d]+)',2),'') as auction_id
        ,coalesce(b.user_id, '') as user_id
        ,concat(imei,imsi) as cookie
        ,concat_ws('\002'
                  ,split(app_id,'@')[0]
                  ,app_version
                  ,event_id
                  ,client_ip
                  ,carrier
                  ,resolution
                  ,brand
                  ,arg2
                  ,args
                  ,coalesce(regexp_extract(args,'(ad_word_show)=(.*?),',2),'')
        ) as extra
    from
        wdm_v3_user_track a
    left outer join
        r_bmw_users_mv b
    on
        a.pt = '${date}'
        and b.pt = '${date}000000'
        and a.server_time > 0
        and a.user_nick = b.nick
        and length(b.nick) >= 2
        and b.suspended = 0
        and length(a.user_nick) >= 2

    union all

    select 
        server_time as time_stamp
        ,coalesce(page, '') as url
        ,coalesce(arg1, '') as refer_url
        ,coalesce(regexp_extract(args,'(sellerid|seller_id)=([\\d]+)',2),'') as seller_id
        ,coalesce(regexp_extract(args,'(itemid|item_id)=([\\d]+)',2),'') as auction_id
        ,'' as user_id
        ,concat(imei,imsi) as cookie
        ,concat_ws('\002'
                  ,split(app_id,'@')[0]
                  ,app_version
                  ,event_id
                  ,client_ip
                  ,carrier
                  ,resolution
                  ,brand
                  ,arg2
                  ,args
                  ,coalesce(regexp_extract(args,'(ad_word_show)=(.*?),',2),'')
        ) as extra
    from
        wdm_v3_user_track
    where 
        pt = '${date}'
        and server_time > 0
        and (length(user_nick) < 2 or user_nick is null)
) a 
;
