-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------ 
-- # Description:产出seo明细表
-- # Input:lz_fact_ep_etao_ownership                                          
-- # Onput:lz_rpt_ep_etao_seo_limit                                            
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ########################################################################## 

insert overwrite table lz_rpt_ep_etao_seo_limit partition (dt='${date}')
select
   day                                 
   ,src_domain_name_level1              
   ,src_domain_name_level2              
   ,keyword                             
   ,lp_domain_name                      
   ,effect_pv                            
   ,effect_uv                            
   ,effect_click_pv                      
   ,effect_click_uv                      
   ,click_rate                           
   ,bounce_rate                          
   ,lp_avg_pv                            
   ,lp_direct_item_gmv_uv              
   ,lp_direct_item_gmv_trade_num       
   ,lp_direct_item_gmv_amt             
   ,lp_direct_item_alipay_uv           
   ,lp_direct_item_alipay_trade_num    
   ,lp_direct_item_alipay_amt          
   ,lp_direct_itemshop_gmv_uv          
   ,lp_direct_itemshop_gmv_trade_num   
   ,lp_direct_itemshop_gmv_amt         
   ,lp_direct_itemshop_alipay_uv       
   ,lp_direct_itemshop_alipay_trade_num
   ,lp_direct_itemshop_alipay_amt      
   ,lp_outside_gmv_uv                  
   ,lp_outside_gmv_trade_num           
   ,lp_outside_gmv_amt                 
   ,lp_outside_alipay_uv               
   ,lp_outside_alipay_trade_num        
   ,lp_outside_alipay_amt              
   ,lp_cps_new_uv                      
   ,lp_cps_new_trade_num              
   ,lp_cps_new_amt                    
   ,lp_cps_old_uv                     
   ,lp_cps_old_trade_num             
   ,lp_cps_old_amt                   
   ,etao_pv                           
   ,etao_uv                           
   ,etao_avg_pv                       
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
from lz_rpt_ep_etao_seo
where dt='${date}' 
    and etao_pv>5
    and length(keyword)>0
;
