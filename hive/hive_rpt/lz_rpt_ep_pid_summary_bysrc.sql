-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/12/17
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒联盟效果分来源效果表
-- # Input:lz_fact_ep_pid_ownership_extend
-- # Onput:lz_rpt_ep_pid_summary_bysrc
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_rpt_ep_pid_summary_bysrc partition (dt = '${date}')
select
    '${date}' as day
    ,channel_id as channel_id
    ,pid as pid
    ,pub_id as pub_id
    ,site_id as site_id
    ,site_name as site_name
    ,adzone_id as adzone_id
    ,adzone_name as adzone_name
    ,src_refer_type as src_refer_type
    ,sum(effect_pv) as effect_pv
    ,count(distinct case when effect_pv >0 then visit_id end) as effect_uv
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
    ,sum(case when p4p_clickid <> '' then 1 else 0 end) as p4p_click_num
    ,sum(p4p_pay_amt) as p4p_pay_amt
    ,sum(case when tbk_clickid <> '' then 1 else 0 end) as tbk_click_num
    ,sum(tbk_pay_amt_c) as tbk_pay_amt_c
    ,sum(tbk_pay_amt_b) as tbk_pay_amt_b
from
    lz_fact_ep_pid_ownership_extend
where
    dt = '${date}'
group by
    channel_id
    ,pid
    ,pub_id
    ,site_id
    ,site_name
    ,adzone_id
    ,adzone_name
    ,src_refer_type
;
