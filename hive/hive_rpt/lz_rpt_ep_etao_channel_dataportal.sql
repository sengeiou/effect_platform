-- ##########################################################################
-- # Owner: feiqiong.dpf        
-- # Email: feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------ 
-- # Description:产出etao频道总览表,仅供etao数据门户使用
-- # Input:lz_rpt_ep_etao_channel
-- # Onput:lz_rpt_ep_etao_channel_dataportal                                             
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ########################################################################## 


insert overwrite table lz_rpt_ep_etao_channel_dataportal partition (dt='${date}')
select
   day                                  
   ,channel_id                          
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
from lz_rpt_ep_etao_channel
where dt='${date}';
