-- ##########################################################################
-- # Owner:luqian
-- # Email:luqian@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:产出etao总览表
-- # Input:lz_fact_ep_etao_ownership
-- # Onput:lz_rpt_ep_etao_summary
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################
insert overwrite table lz_rpt_ep_etao_summary partition (dt='${date}')
select
    day
    ,sum(effect_pv)
    ,max(effect_uv)
    ,sum(effect_click_pv)
    ,max(effect_click_uv)
    ,sum(click_rate)
    ,sum(bounce_rate)
    ,sum(etao_pv)
    ,max(etao_uv)
    ,max(avg_pv)
    ,max(direct_item_gmv_uv                  ) direct_item_gmv_uv
    ,sum(direct_item_gmv_trade_num           ) direct_item_gmv_trade_num
    ,sum(direct_item_gmv_amt                 ) direct_item_gmv_amt
    ,max(direct_item_alipay_uv               ) direct_item_alipay_uv
    ,sum(direct_item_alipay_trade_num        ) direct_item_alipay_trade_num
    ,sum(direct_item_alipay_amt              ) direct_item_alipay_amt
    ,max(direct_itemshop_gmv_uv             ) direct_itemshop_gmv_uv
    ,sum(direct_itemshop_gmv_trade_num      ) direct_itemshop_gmv_trade_num
    ,sum(direct_itemshop_gmv_amt            ) direct_itemshop_gmv_amt
    ,max(direct_itemshop_alipay_uv          ) direct_itemshop_alipay_uv
    ,sum(direct_itemshop_alipay_trade_num   ) direct_itemshop_alipay_trade_num
    ,sum(direct_itemshop_alipay_amt         ) direct_itemshop_alipay_amt
    ,max(outside_gmv_uv                 )
    ,sum(outside_gmv_trade_num          ) outside_gmv_trade_num
    ,sum(outside_gmv_amt                ) outside_gmv_amt
    ,max(outside_alipay_uv              )
    ,sum(outside_alipay_trade_num       ) outside_alipay_trade_num
    ,sum(outside_alipay_amt             ) outside_alipay_amt
    ,max(cps_new_uv                     )
    ,sum(cps_new_trade_num              ) cps_new_trade_num
    ,sum(cps_new_amt                    ) cps_new_amt
    ,max(cps_old_uv                     )
    ,sum(cps_old_trade_num              ) cps_old_trade_num
    ,sum(cps_old_amt                    ) cps_old_amt
    ,max(total_cps_uv                   )
    ,sum(total_cps_trade_num            ) 
    ,sum(total_cps_amt                  ) 
    ,max(total_gmv_uv                   )
    ,sum(total_gmv_trade_num            ) 
    ,sum(total_gmv_amt                  ) 
    ,max(total_alipay_uv                )
    ,sum(total_alipay_trade_num         ) 
    ,sum(total_alipay_amt               ) 
    ,sum(count)
from(
select
    '${date}' as day
    ,sum(effect_pv) as effect_pv
    ,sum(case when effect_pv > 0 then 1 else 0 end) as effect_uv
    ,sum(effect_click_pv) as effect_click_pv
    ,sum(case when effect_click_pv > 0 then 1 else 0 end) as effect_click_uv
    ,case when sum(effect_pv)>0 
          then sum(effect_click_pv)/sum(effect_pv) 
          else 0.0 end as click_rate
    ,case when sum(case when effect_pv > 0 then 1 else 0 end)>0 
          then (sum(case when effect_pv > 0 then 1 else 0 end)                
                - sum(case when effect_click_pv>0 then 1 else 0 end) 
                )/sum(case when effect_pv>0 then 1 else 0 end)
          else 0.0 end bounce_rate
    ,sum(etao_pv) etao_pv
    ,sum(case when etao_pv>0 then 1 else 0 end) etao_uv
    ,case when sum(case when etao_pv>0 then 1 else 0 end)>0 
        then sum(etao_pv)/sum(case when etao_pv>0 then 1 else 0 end) 
        else 0.0 end avg_pv
    ,sum(case when direct_item_gmv_trade_num>0 then 1 else 0 end) direct_item_gmv_uv
    ,sum(direct_item_gmv_trade_num           ) direct_item_gmv_trade_num
    ,sum(direct_item_gmv_amt                 ) direct_item_gmv_amt
    ,sum(case when direct_item_alipay_trade_num>0 then 1 else 0 end) direct_item_alipay_uv
    ,sum(direct_item_alipay_trade_num        ) direct_item_alipay_trade_num
    ,sum(direct_item_alipay_amt              ) direct_item_alipay_amt
    ,sum(case when direct_itemshop_gmv_trade_num>0 then 1 else 0 end) direct_itemshop_gmv_uv
    ,sum(direct_itemshop_gmv_trade_num      ) direct_itemshop_gmv_trade_num
    ,sum(direct_itemshop_gmv_amt            ) direct_itemshop_gmv_amt
    ,sum(case when direct_itemshop_alipay_trade_num>0 then 1 else 0 end) direct_itemshop_alipay_uv
    ,sum(direct_itemshop_alipay_trade_num   ) direct_itemshop_alipay_trade_num
    ,sum(direct_itemshop_alipay_amt         ) direct_itemshop_alipay_amt
    ,sum(case when outside_gmv_trade_num>0 then 1 else 0 end) outside_gmv_uv
    ,sum(outside_gmv_trade_num          ) outside_gmv_trade_num
    ,sum(outside_gmv_amt                ) outside_gmv_amt
    ,sum(case when outside_alipay_trade_num>0 then 1 else 0 end) outside_alipay_uv
    ,sum(outside_alipay_trade_num       ) outside_alipay_trade_num
    ,sum(outside_alipay_amt             ) outside_alipay_amt
    ,sum(case when cps_new_trade_num>0 then 1 else 0 end) cps_new_uv
    ,sum(cps_new_trade_num              ) cps_new_trade_num
    ,sum(cps_new_amt                    ) cps_new_amt
    ,sum(case when cps_old_trade_num>0 then 1 else 0 end) cps_old_uv
    ,sum(cps_old_trade_num              ) cps_old_trade_num
    ,sum(cps_old_amt                    ) cps_old_amt
    ,sum(case when cps_old_trade_num>0 or cps_new_trade_num>0 then 1 else 0 end) total_cps_uv
    ,sum(cps_old_trade_num+cps_new_trade_num) total_cps_trade_num
    ,sum(cps_old_amt+cps_new_amt) total_cps_amt
    ,sum(case when direct_item_gmv_trade_num>0 
        or direct_itemshop_gmv_trade_num>0 
        or outside_gmv_trade_num>0 then 1 else 0 end) total_gmv_uv
    ,sum(direct_item_gmv_trade_num+direct_itemshop_gmv_trade_num+outside_gmv_trade_num) total_gmv_trade_num
    ,sum(direct_item_gmv_amt+direct_itemshop_gmv_amt+outside_gmv_amt) total_gmv_amt
    ,sum(case when direct_item_alipay_trade_num>0 
        or direct_itemshop_alipay_trade_num>0 
        or outside_alipay_trade_num>0 then 1 else 0 end) total_alipay_uv
    ,sum(direct_item_alipay_trade_num+direct_itemshop_alipay_trade_num+outside_alipay_trade_num) total_alipay_trade_num
    ,sum(direct_item_alipay_amt+direct_itemshop_alipay_amt+outside_alipay_amt) total_alipay_amt
    ,cast(0 as bigint) as count
from(
    select
        cookie
        ,sum(case when is_lp=1 then pv else 0.0 end) as effect_pv
        ,sum(case when refer_is_lp=1 then pv else 0.0 end) as effect_click_pv
        ,sum(case when is_etao=1 then pv else 0.0 end) etao_pv
        ,sum(direct_item_gmv_trade_num          ) direct_item_gmv_trade_num
        ,sum(direct_item_gmv_amt                ) direct_item_gmv_amt
        ,sum(direct_item_alipay_trade_num       ) direct_item_alipay_trade_num
        ,sum(direct_item_alipay_amt             ) direct_item_alipay_amt
        ,sum(direct_itemshop_gmv_trade_num      ) direct_itemshop_gmv_trade_num
        ,sum(direct_itemshop_gmv_amt            ) direct_itemshop_gmv_amt
        ,sum(direct_itemshop_alipay_trade_num   ) direct_itemshop_alipay_trade_num
        ,sum(direct_itemshop_alipay_amt         ) direct_itemshop_alipay_amt
        ,sum(outside_gmv_trade_num              ) outside_gmv_trade_num
        ,sum(outside_gmv_amt                    ) outside_gmv_amt
        ,sum(outside_alipay_trade_num           ) outside_alipay_trade_num
        ,sum(outside_alipay_amt                 ) outside_alipay_amt
        ,sum(cps_new_trade_num                  ) cps_new_trade_num
        ,sum(cps_new_amt                        ) cps_new_amt
        ,sum(cps_old_trade_num                  ) cps_old_trade_num
        ,sum(cps_old_amt                        ) cps_old_amt
    from lz_fact_ep_etao_ownership 
    where dt='${date}' 
    group by
        cookie
) a

union all

select
    day
    ,cast(0 as double) as effect_pv
    ,cast(0 as bigint) as effect_uv
    ,cast(0 as double) as effect_click_pv
    ,cast(0 as bigint) as effect_click_uv
    ,cast(0 as double) as click_rate
    ,cast(0 as double) as bounce_rate
    ,cast(0 as double) as etao_pv
    ,cast(0 as bigint) as etao_uv
    ,cast(0 as double) as avg_pv
    ,cast(0 as bigint) as direct_item_gmv_uv
    ,cast(0 as double) as direct_item_gmv_trade_num          
    ,cast(0 as double) as direct_item_gmv_amt                
    ,cast(0 as bigint) as direct_item_alipay_uv
    ,cast(0 as double) as direct_item_alipay_trade_num       
    ,cast(0 as double) as direct_item_alipay_amt             
    ,cast(0 as bigint) as direct_itemshop_gmv_uv
    ,cast(0 as double) as direct_itemshop_gmv_trade_num     
    ,cast(0 as double) as direct_itemshop_gmv_amt           
    ,cast(0 as bigint) as direct_itemshop_alipay_uv
    ,cast(0 as double) as direct_itemshop_alipay_trade_num  
    ,cast(0 as double) as direct_itemshop_alipay_amt        
    ,cast(0 as bigint) as outside_gmv_uv
    ,cast(0 as double) as outside_gmv_trade_num         
    ,cast(0 as double) as outside_gmv_amt               
    ,cast(0 as bigint) as outside_alipay_uv
    ,cast(0 as double) as outside_alipay_trade_num      
    ,cast(0 as double) as outside_alipay_amt            
    ,cast(0 as bigint) as cps_new_uv
    ,cast(0 as double) as cps_new_trade_num             
    ,cast(0 as double) as cps_new_amt                   
    ,cast(0 as bigint) as cps_old_uv
    ,cast(0 as double) as cps_old_trade_num             
    ,cast(0 as double) as cps_old_amt                   
    ,cast(0 as bigint) as total_cps_uv
    ,cast(0 as double) as total_cps_trade_num             
    ,cast(0 as double) as total_cps_amt                   
    ,cast(0 as bigint) as total_gmv_uv
    ,cast(0 as double) as total_gmv_trade_num             
    ,cast(0 as double) as total_gmv_amt                   
    ,cast(0 as bigint) as total_alipay_uv
    ,cast(0 as double) as total_alipay_trade_num             
    ,cast(0 as double) as total_alipay_amt                   
    ,sum(1) as count
from lz_rpt_ep_etao_summary_bysrc
where dt='${date}'
group by
    day
) b
group by
    day
;
