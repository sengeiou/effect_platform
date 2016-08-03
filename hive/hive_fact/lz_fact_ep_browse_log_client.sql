-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2013/01/24
-- # ------------------------------------------------------------------------
-- # Description:无线客户端日志ETL
-- # Input:lz_fact_ep_client_log,lz_fact_auction_info,lz_dim_sellers 
-- # Output:lz_fact_ep_browse_log
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################
insert overwrite table lz_fact_ep_browse_log partition (dt=${date}, logsrc='client')
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
        '' as shop_id,
        auction_id,
        user_id,
        cookie,
        session,
        visit_id,
        useful_extra,
        extra
    from 
        lz_fact_ep_client_log
    where 
        dt=${date} 
        and logtype='others'
        
    union all
    
    select
        d.time_stamp,
        d.url,
        d.refer_url,
        coalesce(cast(c.shop_id as string),'') as shop_id,
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
        from 
            lz_dim_sellers a
        right outer join 
            lz_fact_auction_info b
        on 
            a.dt=${date} 
            and b.dt=${date} 
            and a.seller_id=b.seller_id
    ) c 
    right outer join 
        lz_fact_ep_client_log d 
    on 
        d.dt = '${date}' 
        and d.logtype = 'ipv' 
        and c.auction_id = d.auction_id

    union all

    select
        a.time_stamp,
        a.url,
        a.refer_url,
        coalesce(cast(b.shop_id as string),'') as shop_id,
        a.auction_id,
        a.user_id,
        a.cookie,
        a.session,
        a.visit_id,
        a.useful_extra,
        a.extra
    from
        lz_fact_ep_client_log a
    left outer join
        lz_dim_sellers b
    on 
        a.dt = '${date}'
        and b.dt = '${date}'
        and a.logtype = 'shop'
        and a.seller_id = b.seller_id
)e
;

