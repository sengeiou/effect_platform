-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/10/21
-- # ------------------------------------------------------------------------
-- # Description:无线WAP成交ETL
-- # Input:ds_fdietao_wap_gmv_alipay
-- # Output:lz_fact_ep_trade_info
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import udf:getkeyvalue;
insert overwrite table lz_fact_ep_trade_info partition (dt='${date}', round='11', logsrc='wireless')
select
    coalesce(unix_timestamp(a.closingdate), cast(-99 as bigint)) as gmv_trade_timestamp,
    coalesce(cast(b.shop_id as bigint), cast(-99 as bigint)) as shop_id,
    a.auction_id as auction_id,
    a.user_id as user_id,
    case b.shop_type 
        when cast(1 as bigint) then cast(2 as bigint) 
        when cast(0 as bigint) then cast(1 as bigint) 
        else cast(0 as bigint) end as ali_corp,
    a.gmv_trade_num as gmv_trade_num,
    a.gmv_trade_amt as gmv_trade_amt,
    a.gmv_auction_num as gmv_auction_num,
    a.alipay_trade_num as alipay_trade_num,
    a.alipay_trade_amt as alipay_trade_amt,
    a.alipay_auction_num as alipay_auction_num,
    concat_ws('\003', 'platform_id', 
        case when a.wireless_type='wap' then '1' 
             when a.wireless_type='html5' then '2' 
              else '0' end) as useful_extra,
    concat_ws('\002', order_id, parent_id, wireless_type, closingdate, gmt_receive_pay) as extra
from 
(    
    select
        ${date} as thedate,
        id as order_id,
        parent_id as parent_id,
        buyer_id as user_id,
        seller_id as seller_id,
        auction_id as auction_id,
        sum(case when gmv='gmv' then 1 else 0 end) as gmv_trade_num,
        sum(case when gmv='gmv' then gmv_fee else 0.0 end) as gmv_trade_amt,
        sum(case when gmv='gmv' then cast (buy_amount as bigint) else cast(0 as bigint) end) as gmv_auction_num,
        sum(case when alipay='alipay' then 1 else 0 end) as alipay_trade_num,
        sum(case when alipay='alipay' then ali_fee else 0.0 end) as alipay_trade_amt,
        sum(case when alipay='alipay' then cast (buy_amount as bigint) else cast(0 as bigint) end) as alipay_auction_num,
        max(case when gmv='gmv' then gmv_date else '' end) as closingdate,
        max(case when alipay='alipay' then ali_date else '' end) as gmt_receive_pay,
        case  
            when getkeyvalue(attributes,'ttid') like '400000_%' then 'isv'
            when getkeyvalue(attributes,'ttid') like '%@taobao_android%' then 'android'
            when getkeyvalue(attributes,'ttid') like '%@taobao_iphone%' then 'iphone'
            when getkeyvalue(attributes,'ttid') like '%@taobao_ipad%' then 'ipad'
            when getkeyvalue(attributes,'ttid') like '%@taobao_wp7%' then 'wp7'
            when getkeyvalue(attributes,'ttid') like '%@taobao_wp8%' then 'wp8'
            when getkeyvalue(attributes,'ttid') like '%@taobao_win8%' then 'win8'
            when getkeyvalue(attributes,'ttid') like '%@%' then 'other_client'
            when (getkeyvalue(attributes,'ttid') like '%#t#ad%'
            or getkeyvalue(attributes,'ttid') like '%#t#ip%'
            or getkeyvalue(attributes,'ttid') like '%#t#ao%'
            or getkeyvalue(attributes,'ttid') like '%#t#mt%') then 'h5'
            else 'wap' end as wireless_type
    from ds_fdietao_wap_gmv_alipay
    where pt='${date}000000'
    group by 
        id,
        parent_id,
        buyer_id,
        seller_id,
        auction_id,
        attributes
)a
left outer join lz_dim_sellers b
on  b.dt = '${date}'
    and a.seller_id = b.seller_id
--where a.wireless_type='wap' or a.wireless_type='html5' or a.wireless_type='android' or a.wireless_type='iphone'
;
