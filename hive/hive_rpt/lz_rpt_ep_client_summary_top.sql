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
insert overwrite table lz_rpt_ep_client_summary partition (dt='${date}',type='top')
select
    c.day as day,
    c.province as province,
    c.carrier as carrier,
    c.resolution as resolution,
    c.device_model as device_model,
    c.act_name as act_name,
    c.act_type as act_type,
    c.effect_pv as effect_pv,
    c.effect_uv as effect_uv,
    c.direct_ipv as direct_ipv,
    c.direct_iuv as direct_iuv,
    c.direct_gmv_uv as direct_gmv_uv,
    c.direct_gmv_trade_num as direct_gmv_trade_num,
    c.direct_gmv_amt as direct_gmv_amt,
    c.direct_alipay_uv as direct_alipay_uv,
    c.direct_alipay_trade_num as direct_alipay_trade_num,
    c.direct_alipay_amt as direct_alipay_amt,
    c.guide_ipv as guide_ipv,
    c.guide_iuv as guide_iuv,
    c.guide_gmv_uv as guide_gmv_uv,
    c.guide_gmv_trade_num as guide_gmv_trade_num,
    c.guide_gmv_amt as guide_gmv_amt,
    c.guide_alipay_uv as guide_alipay_uv,
    c.guide_alipay_trade_num as guide_alipay_trade_num,
    c.guide_alipay_amt as guide_alipay_amt
from
    lz_rpt_ep_client_summary c
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
    c.dt = '${date}'
    and c.type = 'all'
    and c.act_name = b.act_name
    and c.act_type = b.act_type
;
