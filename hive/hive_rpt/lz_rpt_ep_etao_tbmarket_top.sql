-- ##########################################################################
-- # Owner:luqian
-- # Email:luqian@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------ 
-- # Description:产出etao总览表
-- # Input:lz_fact_ep_etao_ownership                                          
-- # Onput:lz_rpt_ep_etao_tbmarket                                            
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ########################################################################## 

insert overwrite table lz_rpt_ep_etao_tbmarket_top partition (dt='${date}')
select
   day                  ,
   tb_market_id,
   refer_site,
   adid,
   lp_domain_name       ,
   effect_pv            ,
   effect_uv            ,
   effect_click_pv      ,
   effect_click_uv      ,
   click_rate           ,
   bounce_rate          ,
   lp_avg_pv            ,
   lp_direct_gmv_uv     ,
   lp_direct_gmv_trade_num ,
   lp_direct_gmv_amt    ,
   lp_direct_alipay_uv  ,
   lp_direct_alipay_trade_num ,
   lp_direct_alipay_amt ,
   lp_direct_shop_gmv_uv ,
   lp_direct_shop_gmv_trade_num ,
   lp_direct_shop_gmv_amt ,
   lp_direct_shop_alipay_uv ,
   lp_direct_shop_alipay_trade_num ,
   lp_direct_shop_alipay_amt ,
   lp_outside_gmv_uv    ,
   lp_outside_gmv_trade_num ,
   lp_outside_gmv_amt   ,
   lp_outside_alipay_uv ,
   lp_outside_alipay_trade_num ,
   lp_outside_alipay_amt ,
   lp_cps_new_uv        ,
   lp_cps_new_trade_num ,
   lp_cps_new_amt       ,
   lp_cps_old_uv        ,
   lp_cps_old_trade_num ,
   lp_cps_old_amt       ,
   etao_pv              ,
   etao_uv              ,
   etao_avg_pv          ,
   direct_gmv_uv        ,
   direct_gmv_trade_num ,
   direct_gmv_amt       ,
   direct_alipay_uv     ,
   direct_alipay_trade_num ,
   direct_alipay_amt    ,
   direct_shop_gmv_uv   ,
   direct_shop_gmv_trade_num ,
   direct_shop_gmv_amt  ,
   direct_shop_alipay_uv ,
   direct_shop_alipay_trade_num ,
   direct_shop_alipay_amt ,
   outside_gmv_uv       ,
   outside_gmv_trade_num ,
   outside_gmv_amt      ,
   outside_alipay_uv    ,
   outside_alipay_trade_num ,
   outside_alipay_amt   ,
   cps_new_uv           ,
   cps_new_trade_num    ,
   cps_new_amt          ,
   cps_old_uv           ,
   cps_old_trade_num    ,
   cps_old_amt          
from lz_rpt_ep_etao_tbmarket 
where dt='${date}'
order by
    effect_pv desc
limit 100
;
