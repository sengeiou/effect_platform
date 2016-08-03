-- ##########################################################################
-- # Owner: feiqiong.dpf        
-- # Email: feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------ 
-- # Description:产出etao频道市场外投来源表
-- # Input:lz_fact_ep_etao_channel_ownership
-- # Onput:lz_rpt_ep_etao_channel_tbmarket                                             
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ########################################################################## 


insert overwrite table lz_rpt_ep_etao_channel_tbmarket partition (dt='${date}')
select /*+ MAPJOIN(c) */
   day                                  
   ,channel_id
   ,tb_market_id                         
   ,refer_site                           
   ,coalesce(c.ad_site_name    , '-')                        
   ,coalesce(c.ad_page_name    , '-')                        
   ,coalesce(c.ad_position_name, '-')
   ,coalesce(c.ad_creative_name, '-')
   ,coalesce(c.ad_activity_name, '-')
   ,b.adid                                 
   ,effect_pv                            
   ,effect_uv                            
   ,effect_click_pv                      
   ,effect_click_uv                      
   ,click_rate                           
   ,bounce_rate                          
   ,channel_pv                              
   ,channel_uv                              
   ,avg_pv                          
   ,direct_item_gmv_uv                   
   ,direct_item_gmv_trade_num            
   ,direct_item_gmv_amt                  
   ,direct_item_alipay_uv                
   ,direct_item_alipay_trade_num         
   ,direct_item_alipay_amt               
   ,direct_itemshop_gmv_uv               
   ,direct_itemshop_gmv_trade_num        
   ,direct_itemshop_gmv_amt              
   ,direct_itemshop_alipay_uv            
   ,direct_itemshop_alipay_trade_num     
   ,direct_itemshop_alipay_amt           
   ,direct_shop_gmv_uv               
   ,direct_shop_gmv_trade_num        
   ,direct_shop_gmv_amt              
   ,direct_shop_alipay_uv            
   ,direct_shop_alipay_trade_num     
   ,direct_shop_alipay_amt           
   ,outside_gmv_uv                       
   ,outside_gmv_trade_num                
   ,outside_gmv_amt                      
   ,outside_alipay_uv                    
   ,outside_alipay_trade_num             
   ,outside_alipay_amt                   
   ,cps_new_uv                           
   ,cps_new_trade_num                    
   ,cps_new_amt                          
   ,cps_old_uv                           
   ,cps_old_trade_num                    
   ,cps_old_amt                          
   ,total_cps_uv                           
   ,total_cps_trade_num                    
   ,total_cps_amt                          
   ,total_gmv_uv                           
   ,total_gmv_trade_num                    
   ,total_gmv_amt                          
   ,total_alipay_uv                           
   ,total_alipay_trade_num                    
   ,total_alipay_amt                          
from(
select
    '${date}' as day
    ,channel_id
    ,tb_market_id
    ,refer_site
    ,adid
    ,sum(effect_pv) as effect_pv
    ,count(distinct effect_uv) as effect_uv
    ,sum(effect_click_pv) as effect_click_pv
    ,count(distinct effect_click_uv) as effect_click_uv
    ,case when sum(channel_pv)>0 
          then sum(effect_click_pv)/sum(channel_pv) 
          else 0.0 end as click_rate
    ,case when count(distinct channel_uv)>0 
          then (count(distinct channel_uv)-count(distinct effect_click_uv))
                /count(distinct channel_uv) 
          else 0.0 end bounce_rate
    ,sum(channel_pv) as channel_pv
    ,count(distinct channel_uv) as channel_uv
    ,case 
        when count(distinct channel_uv)>0 
        then sum(channel_pv)/count(distinct channel_uv) 
        else 0.0 end as avg_pv
    ,count(distinct direct_item_gmv_uv          ) as direct_item_gmv_uv
    ,sum(direct_item_gmv_trade_num              ) as direct_item_gmv_trade_num
    ,sum(direct_item_gmv_amt                    ) as direct_item_gmv_amt
    ,count(distinct direct_item_alipay_uv       ) as direct_item_alipay_uv
    ,sum(direct_item_alipay_trade_num           ) as direct_item_alipay_trade_num
    ,sum(direct_item_alipay_amt                 ) as direct_item_alipay_amt
    ,count(distinct direct_itemshop_gmv_uv      ) as direct_itemshop_gmv_uv
    ,sum(direct_itemshop_gmv_trade_num          ) as direct_itemshop_gmv_trade_num
    ,sum(direct_itemshop_gmv_amt                ) as direct_itemshop_gmv_amt
    ,count(distinct direct_itemshop_alipay_uv   ) as direct_itemshop_alipay_uv
    ,sum(direct_itemshop_alipay_trade_num       ) as direct_itemshop_alipay_trade_num
    ,sum(direct_itemshop_alipay_amt             ) as direct_itemshop_alipay_amt
    ,count(distinct direct_shop_gmv_uv          ) as direct_shop_gmv_uv
    ,sum(direct_shop_gmv_trade_num              ) as direct_shop_gmv_trade_num
    ,sum(direct_shop_gmv_amt                    ) as direct_shop_gmv_amt
    ,count(distinct direct_shop_alipay_uv       ) as direct_shop_alipay_uv
    ,sum(direct_shop_alipay_trade_num           ) as direct_shop_alipay_trade_num
    ,sum(direct_shop_alipay_amt                 ) as direct_shop_alipay_amt
    ,count(distinct outside_gmv_uv              ) as outside_gmv_uv
    ,sum(outside_gmv_trade_num                  ) as outside_gmv_trade_num
    ,sum(outside_gmv_amt                        ) as outside_gmv_amt
    ,count(distinct outside_alipay_uv           ) as outside_alipay_uv
    ,sum(outside_alipay_trade_num               ) as outside_alipay_trade_num
    ,sum(outside_alipay_amt                     ) as outside_alipay_amt
    ,count(distinct cps_new_uv                  ) as cps_new_uv
    ,sum(cps_new_trade_num                      ) as cps_new_trade_num
    ,sum(cps_new_amt                            ) as cps_new_amt
    ,count(distinct cps_old_uv                  ) as cps_old_uv
    ,sum(cps_old_trade_num                      ) as cps_old_trade_num
    ,sum(cps_old_amt                            ) as cps_old_amt
    ,count(distinct total_cps_uv                ) as total_cps_uv
    ,sum(total_cps_trade_num                    ) as total_cps_trade_num
    ,sum(total_cps_amt                          ) as total_cps_amt
    ,count(distinct total_gmv_uv                ) as total_gmv_uv
    ,sum(total_gmv_trade_num                    ) as total_gmv_trade_num
    ,sum(total_gmv_amt                          ) as total_gmv_amt
    ,count(distinct total_alipay_uv             ) as total_alipay_uv
    ,sum(total_alipay_trade_num                 ) as total_alipay_trade_num
    ,sum(total_alipay_amt                       ) as total_alipay_amt
from
(
    select
        '${date}' as day
        ,refer_channel_id as channel_id
        ,ref_channel_tb_market_id as tb_market_id
        ,case 
            when ref_channel_refer_site='' or ref_channel_refer_site is null then '-' 
            else ref_channel_refer_site end as refer_site
        ,case 
            when ref_channel_adid='' or ref_channel_adid is null then '-' 
            else ref_channel_adid end as adid
        ,cast(0 as double) as effect_pv
        ,cast(null as string) as effect_uv
        ,pv as effect_click_pv
        ,cookie as effect_click_uv
        ,cast(0 as double) as channel_pv
        ,cast(null as string) as channel_uv
        ,cast(null as string) as direct_item_gmv_uv
        ,cast(0 as double) as direct_item_gmv_trade_num
        ,cast(0 as double) as direct_item_gmv_amt
        ,cast(null as string) as direct_item_alipay_uv
        ,cast(0 as double) as direct_item_alipay_trade_num
        ,cast(0 as double) as direct_item_alipay_amt
        ,cast(null as string) as direct_itemshop_gmv_uv
        ,cast(0 as double) as direct_itemshop_gmv_trade_num
        ,cast(0 as double) as direct_itemshop_gmv_amt
        ,cast(null as string) as direct_itemshop_alipay_uv
        ,cast(0 as double) as direct_itemshop_alipay_trade_num
        ,cast(0 as double) as direct_itemshop_alipay_amt
        ,cast(null as string) as direct_shop_gmv_uv
        ,cast(0 as double) as direct_shop_gmv_trade_num
        ,cast(0 as double) as direct_shop_gmv_amt
        ,cast(null as string) as direct_shop_alipay_uv
        ,cast(0 as double) as direct_shop_alipay_trade_num
        ,cast(0 as double) as direct_shop_alipay_amt
        ,cast(null as string) as outside_gmv_uv
        ,cast(0 as double) as outside_gmv_trade_num
        ,cast(0 as double) as outside_gmv_amt
        ,cast(null as string) as outside_alipay_uv
        ,cast(0 as double) as outside_alipay_trade_num
        ,cast(0 as double) as outside_alipay_amt
        ,cast(null as string) as cps_new_uv
        ,cast(0 as double) as cps_new_trade_num
        ,cast(0 as double) as cps_new_amt
        ,cast(null as string) as cps_old_uv
        ,cast(0 as double) as cps_old_trade_num
        ,cast(0 as double) as cps_old_amt
        ,cast(null as string) as total_cps_uv
        ,cast(0 as double) as total_cps_trade_num
        ,cast(0 as double) as total_cps_amt
        ,cast(null as string) as total_gmv_uv
        ,cast(0 as double) as total_gmv_trade_num
        ,cast(0 as double) as total_gmv_amt
        ,cast(null as string) as total_alipay_uv
        ,cast(0 as double) as total_alipay_trade_num
        ,cast(0 as double) as total_alipay_amt
    from lz_fact_ep_etao_channel_ownership 
    where   
        dt='${date}' 
        and refer_is_channel_lp=1 
        and refer_channel_src=100
        and length(ref_channel_tb_market_id)<50

    union all

    select
        '${date}' as day
        ,channel_id
        ,channel_tb_market_id as tb_market_id
        ,case 
            when channel_refer_site='' or channel_refer_site is null then '-' 
            else channel_refer_site end as refer_site
        ,case 
            when channel_adid='' or channel_adid is null then '-' 
            else channel_adid end as adid
        ,case when is_channel_lp=1 then pv else 0.0 end as effect_pv
        ,case when is_channel_lp=1 then cookie end as effect_uv
        ,cast(0 as double) as effect_click_pv
        ,cast(null as string) as effect_click_uv
        ,pv as channel_pv
        ,cookie as channel_uv
        ,case when direct_item_gmv_trade_num>0 then user_id end as direct_item_gmv_uv
        ,direct_item_gmv_trade_num
        ,direct_item_gmv_amt
        ,case when direct_item_alipay_trade_num>0 then user_id end as direct_item_alipay_uv
        ,direct_item_alipay_trade_num
        ,direct_item_alipay_amt
        ,case when direct_itemshop_gmv_trade_num>0 then user_id end as direct_itemshop_gmv_uv
        ,direct_itemshop_gmv_trade_num
        ,direct_itemshop_gmv_amt
        ,case when direct_itemshop_alipay_trade_num>0 then user_id end as direct_itemshop_alipay_uv
        ,direct_itemshop_alipay_trade_num
        ,direct_itemshop_alipay_amt
        ,case when direct_shop_gmv_trade_num>0 then user_id end as direct_shop_gmv_uv
        ,direct_shop_gmv_trade_num
        ,direct_shop_gmv_amt
        ,case when direct_shop_alipay_trade_num>0 then user_id end as direct_shop_alipay_uv
        ,direct_shop_alipay_trade_num
        ,direct_shop_alipay_amt
        ,case when outside_gmv_trade_num>0 then user_id end as outside_gmv_uv
        ,outside_gmv_trade_num
        ,outside_gmv_amt
        ,case when outside_alipay_trade_num>0 then user_id end as outside_alipay_uv
        ,outside_alipay_trade_num
        ,outside_alipay_amt
        ,case when cps_new_trade_num>0 then user_id end as cps_new_uv
        ,cps_new_trade_num
        ,cps_new_amt
        ,case when cps_old_trade_num>0 then user_id end as cps_old_uv
        ,cps_old_trade_num
        ,cps_old_amt
        ,case when cps_old_trade_num>0 or cps_new_trade_num>0 then user_id end total_cps_uv
        ,(cps_old_trade_num+cps_new_trade_num) total_cps_trade_num
        ,(cps_old_amt+cps_new_amt) total_cps_amt
        ,case when direct_item_gmv_trade_num>0 
            or direct_itemshop_gmv_trade_num>0 
            or direct_shop_gmv_trade_num>0
            or outside_gmv_trade_num>0 then user_id end total_gmv_uv
        ,(direct_item_gmv_trade_num
            + direct_itemshop_gmv_trade_num
            + direct_shop_gmv_trade_num
            + outside_gmv_trade_num) total_gmv_trade_num
        ,(direct_item_gmv_amt
            + direct_itemshop_gmv_amt
            + direct_shop_gmv_amt
            + outside_gmv_amt) total_gmv_amt
        ,case when direct_item_alipay_trade_num>0 
            or direct_itemshop_alipay_trade_num>0 
            or direct_shop_alipay_trade_num>0
            or outside_alipay_trade_num>0 then user_id end total_alipay_uv
        ,(direct_item_alipay_trade_num
            + direct_itemshop_alipay_trade_num
            + direct_shop_alipay_trade_num
            + outside_alipay_trade_num) total_alipay_trade_num
        ,(direct_item_alipay_amt
            + direct_itemshop_alipay_amt
            + direct_shop_alipay_amt
            + outside_alipay_amt) total_alipay_amt
    from lz_fact_ep_etao_channel_ownership 
    where 
        dt='${date}' 
        and channel_id > 0 
        and channel_src=100
        and length(channel_tb_market_id)<50
) a
group by 
    channel_id
    ,tb_market_id
    ,refer_site
    ,adid
) b left outer join lz_fact_ep_ad_config c
on c.dt='${date}' and b.adid=c.ad_id
;
