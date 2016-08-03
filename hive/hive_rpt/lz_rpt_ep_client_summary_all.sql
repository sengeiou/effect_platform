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
import udf:getcity;
insert overwrite table lz_rpt_ep_client_summary partition (dt = '${date}',type='all')
select
    '${date}' as day,
    province,
    carrier,
    resolution,
    device_model,
    act_name,
    act_type,
    sum(effect_pv) as effect_pv,
    count(distinct case when effect_pv > 0 then device_id end) as effect_uv,
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
(
    select
        *
        ,case
            when getcity(ip,"prov") rlike '(广东|江苏|浙江|山东|四川|河南|河北|福建|湖北|北京|山西|湖南|安徽|上海|陕西|辽宁|广西|江西|重庆|黑龙江|天津|云南|贵州|内蒙古|吉林|甘肃|新疆|海南|宁夏|青海|西藏|香港|澳门|台湾)'
            then getcity(ip,"prov")
            else '其他'
        end as province
    from
        lz_fact_ep_client_ownership
    where
        dt = '${date}'
        and app_key = 12278902
        and act_name <> ''
        and act_name <> 'null'
        and act_name is not null
)a
group by
    province
    ,carrier
    ,resolution
    ,device_model
    ,act_name
    ,act_type
;
