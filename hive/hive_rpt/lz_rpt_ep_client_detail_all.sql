-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2013/01/29
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒无线客户端产出详情表
-- # Input:lz_fact_ep_client_ownership
-- # Onput:lz_rpt_ep_wireless_detail
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_rpt_ep_client_detail partition (dt = '${date}',type='all')
select
    a.day as day
    ,a.act_name as act_name
    ,a.act_type as act_type
    ,a.pit_id as pit_id
    ,a.pit_detail as pit_detail
    ,a.direct_ipv as direct_ipv
    ,a.direct_iuv as direct_iuv
    ,a.direct_gmv_uv as direct_gmv_uv
    ,a.direct_gmv_trade_num as direct_gmv_trade_num
    ,a.direct_gmv_amt as direct_gmv_amt
    ,a.direct_alipay_uv as direct_alipay_uv
    ,a.direct_alipay_trade_num as direct_alipay_trade_num
    ,a.direct_alipay_amt as direct_alipay_amt
    ,a.guide_ipv as guide_ipv
    ,a.guide_iuv as guide_iuv
    ,a.guide_gmv_uv as guide_gmv_uv
    ,a.guide_gmv_trade_num as guide_gmv_trade_num
    ,a.guide_gmv_amt as guide_gmv_amt
    ,a.guide_alipay_uv as guide_alipay_uv
    ,a.guide_alipay_trade_num as guide_alipay_trade_num
    ,a.guide_alipay_amt as guide_alipay_amt
from
(
    select
        '${date}' as day,
        act_name as act_name,
        act_type as act_type,
        pit_id as pit_id,
        pit_detail as pit_detail,
        sum(direct_ipv) as direct_ipv,
        count(distinct case when direct_ipv > 0 then device_id end) as direct_iuv,
        count(distinct case when direct_gmv_trade_num > 0 then device_id end) as direct_gmv_uv,
        sum(direct_gmv_trade_num) as direct_gmv_trade_num,
        sum(direct_gmv_amt) as direct_gmv_amt,
        count(distinct case when direct_alipay_trade_num > 0 then device_id end) as direct_alipay_uv,
        sum(direct_alipay_trade_num) as direct_alipay_trade_num,
        sum(direct_alipay_amt) as direct_alipay_amt,
        sum(guide_ipv) as guide_ipv,
        count(distinct case when guide_ipv > 0 then device_id end) as guide_iuv,
        count(distinct case when guide_gmv_trade_num > 0 then device_id end) as guide_gmv_uv,
        sum(guide_gmv_trade_num) as guide_gmv_trade_num,
        sum(guide_gmv_amt) as guide_gmv_amt,
        count(distinct case when guide_alipay_trade_num > 0 then device_id end) as guide_alipay_uv,
        sum(guide_alipay_trade_num) as guide_alipay_trade_num,
        sum(guide_alipay_amt) as guide_alipay_amt
    from
        lz_fact_ep_client_ownership
    where
        dt = '${date}' 
        and app_key = 12278902
        and pit_id > 0
        and act_name <> '' 
        and act_name <> 'null' 
        and act_name is not null
    group by
        act_name,
        act_type,
        pit_id,
        pit_detail
)a
join
(
    select
        act_name
        ,act_type
    from 
    (   
        select
            act_name
            ,act_type
            ,sum(effect_pv) as effect_pv
        from
            lz_rpt_ep_client_summary 
        where
            dt = '${date}'
            and type = 'all'
        group by
            act_name
            ,act_type
    )a  
    where
        effect_pv > 1000
)b
on 
    a.act_name = b.act_name
    and a.act_type = b.act_type
;

