-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2013/01/11
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒无线产出详情top表,
-- # 用于解决每个活动详情数据量过大导致入库和多维展现无法支撑的问题
-- # Input:lz_rpt_ep_wireless_detail
-- # Onput:lz_rpt_ep_wireless_detail_top
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################
import udf:row_number;
insert overwrite table lz_rpt_ep_wireless_detail_top partition (dt=${date})
select
   day          
   ,platform_id  
   ,plan_id      
   ,pit_id       
   ,pit_detail   
   ,effect_pv            
   ,effect_uv            
   ,direct_ipv           
   ,direct_iuv           
   ,direct_gmv_uv        
   ,direct_gmv_trade_num 
   ,direct_gmv_amt       
   ,direct_alipay_uv     
   ,direct_alipay_trade_num    
   ,direct_alipay_amt    
   ,guide_ipv            
   ,guide_iuv            
   ,guide_gmv_uv         
   ,guide_gmv_trade_num  
   ,guide_gmv_amt        
   ,guide_alipay_uv      
   ,guide_alipay_trade_num    
   ,guide_alipay_amt
from(
select
   day          
   ,platform_id  
   ,plan_id      
   ,pit_id       
   ,pit_detail   
   ,effect_pv            
   ,effect_uv            
   ,direct_ipv           
   ,direct_iuv           
   ,direct_gmv_uv        
   ,direct_gmv_trade_num 
   ,direct_gmv_amt       
   ,direct_alipay_uv     
   ,direct_alipay_trade_num    
   ,direct_alipay_amt    
   ,guide_ipv            
   ,guide_iuv            
   ,guide_gmv_uv         
   ,guide_gmv_trade_num  
   ,guide_gmv_amt        
   ,guide_alipay_uv      
   ,guide_alipay_trade_num    
   ,guide_alipay_amt
   ,row_number(platform_id, plan_id, pit_id) as rank
from(
    select
       day          
       ,platform_id  
       ,plan_id      
       ,pit_id       
       ,pit_detail   
       ,effect_pv            
       ,effect_uv            
       ,direct_ipv           
       ,direct_iuv           
       ,direct_gmv_uv        
       ,direct_gmv_trade_num 
       ,direct_gmv_amt       
       ,direct_alipay_uv     
       ,direct_alipay_trade_num    
       ,direct_alipay_amt    
       ,guide_ipv            
       ,guide_iuv            
       ,guide_gmv_uv         
       ,guide_gmv_trade_num  
       ,guide_gmv_amt        
       ,guide_alipay_uv      
       ,guide_alipay_trade_num    
       ,guide_alipay_amt
       ,(direct_ipv+guide_ipv) as ipv
    from lz_rpt_ep_wireless_detail
    where dt=${date}
    distribute by platform_id, plan_id, pit_id
    sort by platform_id, plan_id, pit_id, ipv desc
    )a 
)b
where rank<=500;

