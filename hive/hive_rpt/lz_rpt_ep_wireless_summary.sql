-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/10/25
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒无线产出汇总表
-- # Input:lz_fact_ep_wireless_ownership
-- # Onput:lz_rpt_ep_wireless_summary
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_rpt_ep_wireless_summary partition (dt = '${date}')
select
    '${date}' as day,
    platform_id as platform_id,
    plan_id as plan_id,
    sum(count) as count,
    max(effect_pv) as effect_pv,
    max(effect_uv) as effect_uv,
    sum(direct_ipv) as direct_ipv,
    sum(direct_iuv) as direct_iuv,
    sum(direct_gmv_uv) as direct_gmv_uv,
    sum(direct_gmv_trade_num) as direct_gmv_trade_num,
    sum(direct_gmv_amt) as direct_gmv_amt,
    sum(direct_alipay_uv) as direct_alipay_uv,
    sum(direct_alipay_trade_num) as direct_alipay_trade_num,
    sum(direct_alipay_amt) as direct_alipay_amt,
    sum(guide_ipv) as guide_ipv,
    sum(guide_iuv) as guide_iuv,
    sum(guide_gmv_uv) as guide_gmv_uv,
    sum(guide_gmv_trade_num) as guide_gmv_trade_num,
    sum(guide_gmv_amt) as guide_gmv_amt,
    sum(guide_alipay_uv) as guide_alipay_uv,
    sum(guide_alipay_trade_num) as guide_alipay_trade_num,
    sum(guide_alipay_amt) as guide_alipay_amt
from
(
    select
        platform_id as platform_id,
        plan_id as plan_id,
        cast(0 as bigint) as count,
        sum(effect_pv) as effect_pv,
        count(distinct case when effect_pv > 0 then cookie end) as effect_uv,
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
    from(
        select
            platform_id as platform_id,
            plan_id as plan_id,
            user_id,
            cookie,
            sum(effect_pv) as effect_pv,
            sum(direct_ipv) as direct_ipv,
            sum(direct_gmv_trade_num) as direct_gmv_trade_num,
            sum(direct_gmv_amt) as direct_gmv_amt,
            sum(direct_alipay_trade_num) as direct_alipay_trade_num,
            sum(direct_alipay_amt) as direct_alipay_amt,
            sum(guide_ipv) as guide_ipv,
            sum(guide_gmv_trade_num) as guide_gmv_trade_num,
            sum(guide_gmv_amt) as guide_gmv_amt,
            sum(guide_alipay_trade_num) as guide_alipay_trade_num,
            sum(guide_alipay_amt) as guide_alipay_amt
        from
            lz_fact_ep_wireless_ownership
        where
            dt = '${date}'
        group by
            platform_id,
            plan_id,
            user_id,
            cookie
        )a
    group by
        platform_id,
        plan_id

    union all

    select
        platform_id as platform_id,
        plan_id as plan_id,
        cast(1 as bigint) as count,
        effect_pv as effect_pv,
        effect_uv as effect_uv,
        cast(0 as bigint) as direct_ipv,
        cast(0 as bigint) as direct_iuv,
        cast(0 as bigint) as direct_gmv_uv,
        cast(0 as bigint) as direct_gmv_trade_num,
        cast(0.0 as double) as direct_gmv_amt,
        cast(0 as bigint) as direct_alipay_uv,
        cast(0 as bigint) as direct_alipay_trade_num,
        cast(0.0 as double) as direct_alipay_amt,
        cast(0 as bigint) as guide_ipv,
        cast(0 as bigint) as guide_iuv,
        cast(0 as bigint) as guide_gmv_uv,
        cast(0 as bigint) as guide_gmv_trade_num,
        cast(0.0 as double) as guide_gmv_amt,
        cast(0 as bigint) as guide_alipay_uv,
        cast(0 as bigint) as guide_alipay_trade_num,
        cast(0.0 as double) as guide_alipay_amt
    from
        lz_rpt_ep_wireless_detail
    where
        dt = '${date}'
)a
group by
    platform_id,
    plan_id
;
