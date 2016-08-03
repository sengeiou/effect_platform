-- ##########################################################################
-- # Owner:nanjia.lj
-- # Email:nanjia.lj@taobao.com
-- # Date:2013/01/29
-- # ------------------------------------------------------------------------ 
-- # Description:月光宝盒无线产出详情top表,
-- # Input:lz_rpt_ep_client_detail
-- # Onput:lz_rpt_ep_client_detail
-- # ------------------------------------------------------------------------ 
-- # ChangeLog:
-- ##########################################################################
import udf:row_number;
insert overwrite table lz_rpt_ep_client_detail partition (dt=${date}, type='top50')
select
   day          
   ,act_name
   ,act_type
   ,pit_id       
   ,pit_detail   
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
   ,act_name
   ,act_type
   ,pit_id       
   ,pit_detail   
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
   ,row_number(act_name, act_type, pit_id) as rank
from(
    select
       day          
       ,act_name
       ,act_type
       ,pit_id       
       ,pit_detail   
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
    from 
        lz_rpt_ep_client_detail
    where 
        dt = ${date} 
        and type = 'all'
    distribute by act_name, act_type, pit_id
    sort by act_name, act_type, pit_id, ipv desc
    )a 
)b
where rank<=50;

