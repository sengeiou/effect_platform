-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/10/25
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒无线产出详情表
-- # Input:lz_fact_ep_wireless_ownership
-- # Onput:lz_rpt_ep_wireless_detail
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_rpt_ep_wireless_detail partition (dt = '${date}')
select
    '${date}' as day,
    b.platform_id as platform_id,
    b.plan_id as plan_id,
    b.pit_id as pit_id,
    b.pit_detail as pit_detail,
    a.effect_pv as effect_pv,
    a.effect_uv as effect_uv,
    b.direct_ipv as direct_ipv,
    b.direct_iuv as direct_iuv,
    b.direct_gmv_uv as direct_gmv_uv,
    b.direct_gmv_trade_num as direct_gmv_trade_num,
    b.direct_gmv_amt as direct_gmv_amt,
    b.direct_alipay_uv as direct_alipay_uv,
    b.direct_alipay_trade_num as direct_alipay_trade_num,
    b.direct_alipay_amt as direct_alipay_amt,
    b.guide_ipv as guide_ipv,
    b.guide_iuv as guide_iuv,
    b.guide_gmv_uv as guide_gmv_uv,
    b.guide_gmv_trade_num as guide_gmv_trade_num,
    b.guide_gmv_amt as guide_gmv_amt,
    b.guide_alipay_uv as guide_alipay_uv,
    b.guide_alipay_trade_num as guide_alipay_trade_num,
    b.guide_alipay_amt as guide_alipay_amt
from
(
    select
        platform_id as platform_id,
        plan_id as plan_id,
        sum(effect_pv) as effect_pv,
        count(distinct case when effect_pv > 0 then cookie end) as effect_uv
    from
        lz_fact_ep_wireless_ownership
    where
        dt = '${date}'
    group by
        platform_id,
        plan_id
)a
right outer join
(
    select
        platform_id as platform_id,
        plan_id as plan_id,
        pit_id as pit_id,
        pit_detail as pit_detail,
        sum(direct_ipv) as direct_ipv,
        count(distinct case when direct_ipv > 0 then cookie end) as direct_iuv,
        count(distinct case when direct_gmv_trade_num > 0 then user_id end) as direct_gmv_uv,
        sum(direct_gmv_trade_num) as direct_gmv_trade_num,
        sum(direct_gmv_amt) as direct_gmv_amt,
        count(distinct case when direct_alipay_trade_num > 0 then user_id end) as direct_alipay_uv,
        sum(direct_alipay_trade_num) as direct_alipay_trade_num,
        sum(direct_alipay_amt) as direct_alipay_amt,
        sum(guide_ipv) as guide_ipv,
        count(distinct case when guide_ipv > 0 then cookie end) as guide_iuv,
        count(distinct case when guide_gmv_trade_num > 0 then user_id end) as guide_gmv_uv,
        sum(guide_gmv_trade_num) as guide_gmv_trade_num,
        sum(guide_gmv_amt) as guide_gmv_amt,
        count(distinct case when guide_alipay_trade_num > 0 then user_id end) as guide_alipay_uv,
        sum(guide_alipay_trade_num) as guide_alipay_trade_num,
        sum(guide_alipay_amt) as guide_alipay_amt
    from
        lz_fact_ep_wireless_ownership
    where
        dt = '${date}' and pit_id > 0
    group by
        platform_id,
        plan_id,
        pit_id,
        pit_detail
)b
on a.platform_id = b.platform_id
    and a.plan_id = b.plan_id
;
