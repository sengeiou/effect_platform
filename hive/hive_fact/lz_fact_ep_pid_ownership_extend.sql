-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/12/17
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒联盟效果中间表
-- # Input:lz_fact_ep_pid_ownership
-- # Onput:lz_fact_ep_pid_ownership_extend
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_fact_ep_pid_ownership_extend partition (dt = '${date}')
select
    d.channel_id as channel_id
    ,d.pid as pid
    ,d.pub_id as pub_id
    ,case 
        when d.site_id like 'site_id:%'
        then ''
        else d.site_id
     end as site_id
    ,coalesce(e.sitename, '-') as site_name
    ,d.adzone_id as adzone_id 
    ,coalesce(e.name, '-') as adzone_name
    ,d.src_refer as src_refer
    ,d.src_refer_type as src_refer_type 
    ,d.url as url
    ,d.refer as refer
    ,d.shop_id as shop_id
    ,d.auction_id as auction_id
    ,d.user_id as user_id
    ,d.cookie as cookie
    ,d.session as session
    ,d.visit_id as visit_id
    ,d.is_effect_page as is_effect_page
    ,d.refer_is_effect_page as refer_is_effect_page
    ,d.pit_id as pit_id
    ,d.pit_detail as pit_detail
    ,d.ali_refid as ali_refid
    ,d.effect_pv as effect_pv
    ,d.effect_click_pv as effect_click_pv
    ,d.channel_pv as channel_pv
    ,d.guide_ipv as guide_ipv 
    ,d.direct_gmv_trade_num as direct_gmv_trade_num 
    ,d.direct_gmv_amt as direct_gmv_amt
    ,d.direct_alipay_trade_num as direct_alipay_trade_num 
    ,d.direct_alipay_amt as direct_alipay_amt
    ,d.guide_gmv_trade_num as guide_gmv_trade_num 
    ,d.guide_gmv_amt as guide_gmv_amt
    ,d.guide_alipay_trade_num as guide_alipay_trade_num 
    ,d.guide_alipay_amt as guide_alipay_amt
    ,d.p4p_clickid as p4p_clickid
    ,d.p4p_pay_amt as p4p_pay_amt
    ,d.tbk_clickid as tbk_clickid
    ,d.tbk_flag as tbk_flag
    ,d.tbk_pay_amt_c as tbk_pay_amt_c
    ,d.tbk_pay_amt_b as tbk_pay_amt_b
from
(
    select
        a.channel_id as channel_id
        ,a.pid as pid
        ,a.pub_id as pub_id
        ,case
            when (a.site_id = '' or a.site_id = 0)
            then concat('site_id:',round(rand()*100000000000)) 
            else a.site_id
         end as site_id
        ,a.adzone_id as adzone_id 
        ,a.src_refer as src_refer
        ,a.src_refer_type as src_refer_type
        ,a.url as url
        ,a.refer as refer
        ,a.shop_id as shop_id
        ,a.auction_id as auction_id
        ,a.user_id as user_id
        ,a.cookie as cookie
        ,a.session as session
        ,a.visit_id as visit_id
        ,a.is_effect_page as is_effect_page
        ,a.refer_is_effect_page as refer_is_effect_page
        ,a.pit_id as pit_id
        ,a.pit_detail as pit_detail
        ,a.ali_refid as ali_refid
        ,a.effect_pv as effect_pv
        ,a.effect_click_pv as effect_click_pv
        ,a.channel_pv as channel_pv
        ,a.guide_ipv as guide_ipv 
        ,a.direct_gmv_trade_num as direct_gmv_trade_num 
        ,a.direct_gmv_amt as direct_gmv_amt
        ,a.direct_alipay_trade_num as direct_alipay_trade_num 
        ,a.direct_alipay_amt as direct_alipay_amt
        ,a.guide_gmv_trade_num as guide_gmv_trade_num 
        ,a.guide_gmv_amt as guide_gmv_amt
        ,a.guide_alipay_trade_num as guide_alipay_trade_num 
        ,a.guide_alipay_amt as guide_alipay_amt
        ,a.p4p_clickid as p4p_clickid
        ,coalesce(b.kwamt/100, cast(0 as double)) as p4p_pay_amt
        ,a.tbk_clickid as tbk_clickid
        ,a.tbk_flag as tbk_flag
        ,cast(0 as double) as tbk_pay_amt_c
        ,cast(0 as double) as tbk_pay_amt_b
    from
        lz_fact_ep_pid_ownership a
    left outer join 
        fact_cost_click_effect1_d b 
    on 
        a.dt = '${date}' 
        and a.pit_id > 0
        and a.p4p_clickid <> ''
        and b.dt = '${date}'
        and a.p4p_clickid = b.clickid

    union all

    select
        a.channel_id as channel_id
        ,a.pid as pid
        ,a.pub_id as pub_id
        ,case
            when (a.site_id = '' or a.site_id = 0)
            then concat('site_id:',round(rand()*100000000000)) 
            else a.site_id
         end as site_id
        ,a.adzone_id as adzone_id 
        ,a.src_refer as src_refer
        ,a.src_refer_type as src_refer_type
        ,a.url as url
        ,a.refer as refer
        ,a.shop_id as shop_id
        ,a.auction_id as auction_id
        ,a.user_id as user_id
        ,a.cookie as cookie
        ,a.session as session
        ,a.visit_id as visit_id
        ,a.is_effect_page as is_effect_page
        ,a.refer_is_effect_page as refer_is_effect_page
        ,a.pit_id as pit_id
        ,a.pit_detail as pit_detail
        ,a.ali_refid as ali_refid
        ,a.effect_pv
        ,a.effect_click_pv as effect_click_pv
        ,a.channel_pv
        ,a.guide_ipv as guide_ipv 
        ,a.direct_gmv_trade_num as direct_gmv_trade_num 
        ,a.direct_gmv_amt as direct_gmv_amt
        ,a.direct_alipay_trade_num as direct_alipay_trade_num 
        ,a.direct_alipay_amt as direct_alipay_amt
        ,a.guide_gmv_trade_num as guide_gmv_trade_num 
        ,a.guide_gmv_amt as guide_gmv_amt
        ,a.guide_alipay_trade_num as guide_alipay_trade_num 
        ,a.guide_alipay_amt as guide_alipay_amt
        ,a.p4p_clickid as p4p_clickid
        ,cast(0 as double) as p4p_pay_amt
        ,a.tbk_clickid as tbk_clickid
        ,a.tbk_flag as tbk_flag
        ,case
            when a.tbk_flag = 1
            then coalesce(c.pay_price * c.discount, cast(0 as double))
            else cast(0 as double)
        end as tbk_pay_amt_c
        ,case
            when a.tbk_flag = 2
            then coalesce(c.pay_price * c.discount, cast(0 as double))
            else cast(0 as double)
        end as tbk_pay_amt_b
    from
        lz_fact_ep_pid_ownership a
    left outer join 
        s_dw_cps_payment c 
    on 
        a.dt = '${date}' 
        and a.pit_id > 0
        and a.tbk_flag > 0
        and c.ds = '${date}'
        and a.order_id = c.taobao_trade_id

    union all 

    select
        channel_id as channel_id
        ,pid as pid
        ,pub_id as pub_id
        ,case
            when (site_id = '' or site_id = 0)
            then concat('site_id:',round(rand()*100000000000)) 
            else site_id
         end as site_id
        ,adzone_id as adzone_id 
        ,src_refer as src_refer
        ,src_refer_type as src_refer_type
        ,url as url
        ,refer as refer
        ,shop_id as shop_id
        ,auction_id as auction_id
        ,user_id as user_id
        ,cookie as cookie
        ,session as session
        ,visit_id as visit_id
        ,is_effect_page as is_effect_page
        ,refer_is_effect_page as refer_is_effect_page
        ,pit_id as pit_id
        ,pit_detail as pit_detail
        ,ali_refid as ali_refid
        ,effect_pv as effect_pv
        ,effect_click_pv as effect_click_pv
        ,channel_pv as channel_pv 
        ,guide_ipv as guide_ipv 
        ,direct_gmv_trade_num as direct_gmv_trade_num 
        ,direct_gmv_amt as direct_gmv_amt
        ,direct_alipay_trade_num as direct_alipay_trade_num 
        ,direct_alipay_amt as direct_alipay_amt
        ,guide_gmv_trade_num as guide_gmv_trade_num 
        ,guide_gmv_amt as guide_gmv_amt
        ,guide_alipay_trade_num as guide_alipay_trade_num 
        ,guide_alipay_amt as guide_alipay_amt
        ,a.p4p_clickid as p4p_clickid
        ,cast(0 as double) as p4p_pay_amt
        ,a.tbk_clickid as tbk_clickid
        ,cast(0 as bigint) as tbk_flag
        ,cast(0 as double) as tbk_pay_amt_c
        ,cast(0 as double) as tbk_pay_amt_b
    from
        lz_fact_ep_pid_ownership a
    where
        dt = '${date}' 
        and p4p_clickid = ''
        and tbk_flag = 0
) d 
left outer join
    s_ods_mm_adzones e
on 
    e.ds = '${date}'
    and d.site_id = e.siteid 
    and d.adzone_id = e.adzoneid
;
