-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/12/18
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒联盟效果汇总表
-- # Input:lz_fact_ep_pid_ownership,lz_rpt_ep_pid_detail
-- # Onput:lz_rpt_ep_pid_summary
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_rpt_ep_pid_summary partition (dt = '${date}')
select
    '${date}' as day
    ,channel_id as channel_id
    ,sum(count) as count
    ,sum(effect_pv) as effect_pv
    ,count(distinct case when effect_pv > 0 then visit_id end) as effect_uv
    ,sum(effect_click_pv) as effect_click_pv
    ,count(distinct case when effect_click_pv > 0 then visit_id end) as effect_click_uv
    ,sum(channel_pv) as channel_pv
    ,count(distinct case when channel_pv > 0 then visit_id end) as channel_uv 
    ,sum(guide_ipv) as guide_ipv
    ,count(distinct case when guide_ipv > 0 then visit_id end) as guide_iuv
    ,count(distinct case when direct_gmv_trade_num > 0 then visit_id end) as direct_gmv_uv
    ,sum(direct_gmv_trade_num) as direct_gmv_trade_num
    ,sum(direct_gmv_amt) as direct_gmv_amt
    ,count(distinct case when direct_alipay_trade_num > 0 then visit_id end) as direct_alipay_uv
    ,sum(direct_alipay_trade_num) as direct_alipay_trade_num
    ,sum(direct_alipay_amt) as direct_alipay_amt
    ,count(distinct case when guide_gmv_trade_num > 0 then visit_id end) as guide_gmv_uv
    ,sum(guide_gmv_trade_num) as guide_gmv_trade_num
    ,sum(guide_gmv_amt) as guide_gmv_amt
    ,count(distinct case when guide_alipay_trade_num > 0 then visit_id end) as guide_alipay_uv
    ,sum(guide_alipay_trade_num) as guide_alipay_trade_num
    ,sum(guide_alipay_amt) as guide_alipay_amt
    ,sum(p4p_click_num) as p4p_click_num
    ,sum(p4p_pay_amt) as p4p_pay_amt
    ,sum(tbk_click_num) as tbk_click_num
    ,sum(tbk_pay_amt_c) as tbk_pay_amt_c
    ,sum(tbk_pay_amt_b) as tbk_pay_amt_b
from
(
    select
        channel_id as channel_id
        ,cast(1 as bigint) as count
        ,cast(0 as bigint) as effect_pv
        ,cast(0 as bigint) as effect_uv
        ,cast(0 as bigint) as effect_click_pv
        ,cast(0 as bigint) as effect_click_uv
        ,cast(0 as bigint) as channel_pv
        ,cast(0 as bigint) as channel_uv
        ,cast(0 as bigint) as guide_ipv 
        ,cast(0 as bigint) as guide_iuv 
        ,cast(0 as bigint) as direct_gmv_uv
        ,cast(0 as bigint) as direct_gmv_trade_num 
        ,cast(0 as double) as direct_gmv_amt
        ,cast(0 as bigint) as direct_alipay_uv
        ,cast(0 as bigint) as direct_alipay_trade_num 
        ,cast(0 as double) as direct_alipay_amt
        ,cast(0 as bigint) as guide_gmv_uv
        ,cast(0 as bigint) as guide_gmv_trade_num 
        ,cast(0 as double) as guide_gmv_amt
        ,cast(0 as bigint) as guide_alipay_uv
        ,cast(0 as bigint) as guide_alipay_trade_num 
        ,cast(0 as double) as guide_alipay_amt
        ,p4p_click_num as p4p_click_num
        ,p4p_pay_amt as p4p_pay_amt
        ,tbk_click_num as tbk_click_num
        ,tbk_pay_amt_c as tbk_pay_amt_c
        ,tbk_pay_amt_b as tbk_pay_amt_b
        ,cast(null as string) as visit_id
    from
        lz_rpt_ep_pid_detail
    where 
        dt = '${date}' 

    union all

    select
        channel_id as channel_id
        ,cast(0 as bigint) as count
        ,effect_pv as effect_pv
        ,cast(0 as bigint) as effect_uv
        ,effect_click_pv as effect_click_pv
        ,cast(0 as bigint) as effect_click_uv
        ,channel_pv as channel_pv
        ,cast(0 as bigint) as channel_uv 
        ,guide_ipv as guide_ipv
        ,cast(0 as bigint) as guide_iuv
        ,cast(0 as bigint) as direct_gmv_uv
        ,direct_gmv_trade_num as direct_gmv_trade_num
        ,direct_gmv_amt as direct_gmv_amt
        ,cast(0 as bigint) as direct_alipay_uv
        ,direct_alipay_trade_num as direct_alipay_trade_num
        ,direct_alipay_amt as direct_alipay_amt
        ,cast(0 as bigint) as guide_gmv_uv
        ,guide_gmv_trade_num as guide_gmv_trade_num
        ,guide_gmv_amt as guide_gmv_amt
        ,cast(0 as bigint) as guide_alipay_uv
        ,guide_alipay_trade_num as guide_alipay_trade_num
        ,guide_alipay_amt as guide_alipay_amt
        ,cast(0 as bigint) as p4p_click_num
        ,cast(0 as double) as p4p_pay_amt
        ,cast(0 as bigint) as tbk_click_num
        ,cast(0 as double) as tbk_pay_amt_c
        ,cast(0 as double) as tbk_pay_amt_b
        ,visit_id as visit_id
    from
        lz_fact_ep_pid_ownership_extend
    where
        dt = '${date}' 
) a 
group by
    channel_id
;
