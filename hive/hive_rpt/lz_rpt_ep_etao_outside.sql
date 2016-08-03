-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------ 
-- # Description:产出站外自然流量来源明细表
-- # Input:lz_fact_ep_etao_ownership                                          
-- # Onput:lz_rpt_ep_etao_outside                                            
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ########################################################################## 

insert overwrite table lz_rpt_ep_etao_outside partition (dt='${date}')
select
    '${date}' as day
    ,src_domain_name_level1
    ,src_domain_name_level2
    ,case when lp_domain_name='' or lp_domain_name is null then '-' else lp_domain_name end as lp_domain_name
    ,sum(case when is_lp=1 then pv else 0.0 end) as effect_pv
    ,count(distinct case when is_lp=1 then cookie end) as effect_uv
    ,sum(case when refer_is_lp=1 then pv else 0.0 end) as effect_click_pv
    ,count(distinct case when  refer_is_lp=1 then cookie end) as effect_click_uv
    ,case when sum(case when is_lp=1 then pv end)>0 
          then sum(case when refer_is_lp=1 then pv else 0.0 end)/sum(case when is_lp=1 then pv end) 
          else 0.0 end as click_rate
    ,case when count(distinct case when  is_lp=1  then cookie end)>0 
          then (count(distinct case when  is_lp=1  then cookie end) 
                - count(distinct case when  refer_is_lp=1  then cookie end) 
                )/count(distinct case when  is_lp=1  then cookie end) 
          else 0.0 end bounce_rate
    ,case when count(distinct case when refer_is_lp=1 then cookie end)>0
          then sum(case when refer_is_lp=1 then pv else 0.0 end)
               /count(distinct case when refer_is_lp=1 then cookie end)
          else 0.0 end as lp_avg_pv
    ,count(distinct case when refer_is_lp=1 and direct_item_gmv_trade_num>0 then user_id end) as lp_direct_item_gmv_uv
    ,sum(case when refer_is_lp=1 then direct_item_gmv_trade_num else 0.0 end) as lp_direct_item_gmv_trade_num
    ,sum(case when refer_is_lp=1 then direct_item_gmv_amt else 0.0 end) as lp_direct_item_gmv_amt
    ,count(distinct case when refer_is_lp=1 and direct_item_alipay_trade_num>0 
        then user_id end) as lp_direct_item_alipay_uv
    ,sum(case when refer_is_lp=1 then direct_item_alipay_trade_num else 0.0 end) as lp_direct_item_alipay_trade_num
    ,sum(case when refer_is_lp=1 then direct_item_alipay_amt else 0.0 end) as lp_direct_item_alipay_amt
    ,count(distinct case when refer_is_lp=1 and direct_itemshop_gmv_trade_num>0 
        then user_id end) as lp_direct_itemshop_gmv_uv
    ,sum(case when refer_is_lp=1 then direct_itemshop_gmv_trade_num else 0.0 end) as lp_direct_itemshop_gmv_trade_num
    ,sum(case when refer_is_lp=1 then direct_itemshop_gmv_amt else 0.0 end) as lp_direct_itemshop_gmv_amt
    ,count(distinct case when refer_is_lp=1 and direct_itemshop_alipay_trade_num>0 
        then user_id end) as lp_direct_itemshop_alipay_uv
    ,sum(case when refer_is_lp=1 then direct_itemshop_alipay_trade_num 
        else 0.0 end) as lp_direct_itemshop_alipay_trade_num
    ,sum(case when refer_is_lp=1 then direct_itemshop_alipay_amt else 0.0 end) as lp_direct_itemshop_alipay_amt
    ,count(distinct case when refer_is_lp=1 and outside_gmv_trade_num>0 then user_id end) as lp_outside_gmv_uv
    ,sum(case when refer_is_lp=1 then outside_gmv_trade_num else 0.0 end) as lp_outside_gmv_trade_num
    ,sum(case when refer_is_lp=1 then outside_gmv_amt else 0.0 end) as lp_outside_gmv_amt
    ,count(distinct case when refer_is_lp=1 and outside_alipay_trade_num>0 
        then user_id end) as lp_outside_alipay_uv
    ,sum(case when refer_is_lp=1 then outside_alipay_trade_num else 0.0 end) as lp_outside_alipay_trade_num
    ,sum(case when refer_is_lp=1 then outside_alipay_amt else 0.0 end) as lp_outside_alipay_amt
    ,count(distinct case when refer_is_lp=1 and cps_new_trade_num>0 then user_id end) as lp_cps_new_uv
    ,sum(case when refer_is_lp=1 then cps_new_trade_num else 0.0 end) as lp_cps_new_trade_num
    ,sum(case when refer_is_lp=1 then cps_new_amt else 0.0 end) as lp_cps_new_amt
    ,count(distinct case when refer_is_lp=1 and cps_old_trade_num>0 then user_id end) as lp_cps_old_uv
    ,sum(case when refer_is_lp=1 then cps_old_trade_num else 0.0 end) as lp_cps_old_trade_num
    ,sum(case when refer_is_lp=1 then cps_old_amt else 0.0 end) as lp_cps_old_amt
    ,sum(case when is_etao=1 then pv else 0.0 end) as etao_pv
    ,count(distinct case when is_etao=1 then cookie end) as etao_uv
    ,case when count(distinct case when is_etao=1 then cookie end)>0 
        then sum(case when is_etao=1 then pv else 0.0 end)/count(distinct case when is_etao=1 then cookie end) 
        else 0.0 end as avg_pv
    ,count(distinct case when direct_item_gmv_trade_num>0 then user_id end) as direct_item_gmv_uv
    ,sum(direct_item_gmv_trade_num           ) as direct_item_gmv_trade_num
    ,sum(direct_item_gmv_amt                 ) as direct_item_gmv_amt
    ,count(distinct case when direct_item_alipay_trade_num>0 then user_id end) as direct_item_alipay_uv
    ,sum(direct_item_alipay_trade_num        ) as direct_item_alipay_trade_num
    ,sum(direct_item_alipay_amt              ) as direct_item_alipay_amt
    ,count(distinct case when direct_itemshop_gmv_trade_num>0 then user_id end) as direct_itemshop_gmv_uv
    ,sum(direct_itemshop_gmv_trade_num      ) as direct_itemshop_gmv_trade_num
    ,sum(direct_itemshop_gmv_amt            ) as direct_itemshop_gmv_amt
    ,count(distinct case when direct_itemshop_alipay_trade_num>0 then user_id end) as direct_itemshop_alipay_uv
    ,sum(direct_itemshop_alipay_trade_num   ) as direct_itemshop_alipay_trade_num
    ,sum(direct_itemshop_alipay_amt         ) as direct_itemshop_alipay_amt
    ,count(distinct case when outside_gmv_trade_num>0 then user_id end) as outside_gmv_uv
    ,sum(outside_gmv_trade_num          ) as outside_gmv_trade_num
    ,sum(outside_gmv_amt                ) as outside_gmv_amt
    ,count(distinct case when outside_alipay_trade_num>0 then user_id end) as outside_alipay_uv
    ,sum(outside_alipay_trade_num       ) as outside_alipay_trade_num
    ,sum(outside_alipay_amt             ) as outside_alipay_amt
    ,count(distinct case when cps_new_trade_num>0 then user_id end) as cps_new_uv
    ,sum(cps_new_trade_num              ) as cps_new_trade_num
    ,sum(cps_new_amt                    ) as cps_new_amt
    ,count(distinct case when cps_old_trade_num>0 then user_id end) as cps_old_uv
    ,sum(cps_old_trade_num              ) as cps_old_trade_num
    ,sum(cps_old_amt                    ) as cps_old_amt
    ,count(distinct case when cps_old_trade_num>0 or cps_new_trade_num>0 then user_id end) total_cps_uv
    ,sum(cps_old_trade_num+cps_new_trade_num) total_cps_trade_num
    ,sum(cps_old_amt+cps_new_amt) total_cps_amt
    ,count(distinct case when direct_item_gmv_trade_num>0 
        or direct_itemshop_gmv_trade_num>0 
        or outside_gmv_trade_num>0 then user_id end) total_gmv_uv
    ,sum(direct_item_gmv_trade_num+direct_itemshop_gmv_trade_num+outside_gmv_trade_num) total_gmv_trade_num
    ,sum(direct_item_gmv_amt+direct_itemshop_gmv_amt+outside_gmv_amt) total_gmv_amt
    ,count(distinct case when direct_item_alipay_trade_num>0 
        or direct_itemshop_alipay_trade_num>0 
        or outside_alipay_trade_num>0 then user_id end) total_alipay_uv
    ,sum(direct_item_alipay_trade_num+direct_itemshop_alipay_trade_num+outside_alipay_trade_num) total_alipay_trade_num
    ,sum(direct_item_alipay_amt+direct_itemshop_alipay_amt+outside_alipay_amt) total_alipay_amt
from lz_fact_ep_etao_ownership 
where dt='${date}' and lp_src=204
group by 
    src_domain_name_level1
    ,src_domain_name_level2
    ,case when lp_domain_name='' or lp_domain_name is null then '-' else lp_domain_name end 
;
