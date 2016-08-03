-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/10/21
-- # ------------------------------------------------------------------------
-- # Description:无线WAP日志ETL
-- # Input:s_wireless_loginsid, s_ods_wireless_total
-- # Output:lz_fact_ep_browse_log
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################
insert overwrite table lz_fact_ep_browse_log partition (dt=${date}, logsrc='wireless')
select     
    time_stamp,
    url,
    refer_url,
    shop_id,
    auction_id,
    user_id,
    cookie,
    session,
    visit_id,
    useful_extra,
    extra
from 
(
    select 
        time_stamp,
        url,
        refer_url,
        shop_id,
        auction_id,
        user_id,
        cookie,
        session,
        visit_id,
        useful_extra,
        extra
    from lz_fact_ep_wireless_log
    where dt=${date} and logtype='others'
        
    union all
    
    select
        d.time_stamp,
        d.url,
        d.refer_url,
        case when length(d.shop_id) > 1 then d.shop_id else cast(c.shop_id as string) end as shop_id,
        d.auction_id,
        d.user_id,
        d.cookie,
        d.session,
        d.visit_id,
        d.useful_extra,
        d.extra
    from
    (
    select
        a.shop_id,
        b.auction_id
    from lz_dim_sellers a
    right outer join lz_fact_auction_info b
    on a.dt=${date} and b.dt=${date} and a.seller_id=b.seller_id
    )c 
    right outer join lz_fact_ep_wireless_log d 
    on d.dt='${date}' and d.logtype='ipv' and c.auction_id=d.auction_id
)e
;

