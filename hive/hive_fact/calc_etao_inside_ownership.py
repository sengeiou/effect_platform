#!/usr/bin/env python
# -*- encoding:utf-8 -*-

import sys

SPLIT = '\001'

INPUT_FIELD_NUM = 29
OUTPUT_FIELD_NUM = 29

class i:
    (   
        log_type,
        ts              , 
        shop_id         ,     
        auction_id      ,     
        user_id         ,     
        lp_src          ,     
        cookie          ,     
        refer_is_lp     ,     
        lp_domain_name  , 
        adid            ,     
        tb_market_id    ,     
        refer_site      ,     
        site_id         ,     
        ad_id           ,     
        apply,
        t_id,
        linkname,
        pub_id,
        pid_site_id,
        adzone_id,
        keyword,
        src_domain_name_level1,
        src_domain_name_level2,
        gmv_trade_num         ,   
        gmv_trade_amt         ,
        gmv_auction_num       ,
        alipay_trade_num      ,
        alipay_trade_amt      ,
        alipay_auction_num  
    ) = range(INPUT_FIELD_NUM)

class o:
    (
        shop_id         ,     
        auction_id      ,     
        user_id         ,     
        lp_src          ,     
        cookie          ,     
        refer_is_lp     ,     
        lp_domain_name  , 
        adid            ,     
        tb_market_id    ,     
        refer_site      ,     
        site_id         ,     
        ad_id           ,     
        apply,
        t_id,
        linkname,
        pub_id,
        pid_site_id,
        adzone_id,
        keyword,
        src_domain_name_level1,
        src_domain_name_level2,
        direct_item_gmv_trade_num, 
        direct_item_gmv_amt       ,
        direct_item_alipay_trade_num ,
        direct_item_alipay_amt    ,
        direct_itemshop_gmv_trade_num ,
        direct_itemshop_gmv_amt  ,
        direct_itemshop_alipay_trade_num ,
        direct_itemshop_alipay_amt 
    ) = range(OUTPUT_FIELD_NUM)

class OutputRecord:
    def __init__(self, input_record):
        self.v = ['' for j in range(OUTPUT_FIELD_NUM)]
        self.v[o.shop_id] = input_record[i.shop_id]
        self.v[o.auction_id] = input_record[i.auction_id]
        self.v[o.user_id] = input_record[i.user_id]
        self.v[o.lp_src] = input_record[i.lp_src]
        self.v[o.cookie] = input_record[i.cookie]
        self.v[o.refer_is_lp] = input_record[i.refer_is_lp]
        self.v[o.lp_domain_name] = input_record[i.lp_domain_name]
        self.v[o.adid] = input_record[i.adid]
        self.v[o.tb_market_id] = input_record[i.tb_market_id]
        self.v[o.refer_site] = input_record[i.refer_site]
        self.v[o.site_id] = input_record[i.site_id]
        self.v[o.ad_id] = input_record[i.ad_id]
        self.v[o.apply] = input_record[i.apply]
        self.v[o.t_id] = input_record[i.t_id]
        self.v[o.linkname] = input_record[i.linkname]
        self.v[o.pub_id] = input_record[i.pub_id]
        self.v[o.pid_site_id] = input_record[i.pid_site_id]
        self.v[o.adzone_id] = input_record[i.adzone_id]
        self.v[o.keyword] = input_record[i.keyword]
        self.v[o.src_domain_name_level1] = input_record[i.src_domain_name_level1]
        self.v[o.src_domain_name_level2] = input_record[i.src_domain_name_level2]
        self.v[o.direct_item_gmv_trade_num] = '0'
        self.v[o.direct_item_gmv_amt] = '0'
        self.v[o.direct_item_alipay_trade_num] = '0'
        self.v[o.direct_item_alipay_amt] = '0'
        self.v[o.direct_itemshop_gmv_trade_num] = '0'
        self.v[o.direct_itemshop_gmv_amt] = '0'
        self.v[o.direct_itemshop_alipay_trade_num] = '0'
        self.v[o.direct_itemshop_alipay_amt] = '0'
    
    def direct(self, record):
        self.v[o.direct_item_gmv_trade_num] = record[i.gmv_trade_num]
        self.v[o.direct_item_gmv_amt] = record[i.gmv_trade_amt]
        self.v[o.direct_item_alipay_trade_num] = record[i.alipay_trade_num]
        self.v[o.direct_item_alipay_amt] = record[i.alipay_trade_amt]
       
    def indirect(self, record):
        self.v[o.direct_itemshop_gmv_trade_num] = record[i.gmv_trade_num]
        self.v[o.direct_itemshop_gmv_amt] = record[i.gmv_trade_amt]
        self.v[o.direct_itemshop_alipay_trade_num] = record[i.alipay_trade_num]
        self.v[o.direct_itemshop_alipay_amt] = record[i.alipay_trade_amt]
   
    def output(self):
        print SPLIT.join(self.v) 

def main(argv):
    a_session = a_cookie = a_index_root_path = ''
    a_shop_id = a_user_id = ''
    m_auctions = {}
    last = []

    for line in sys.stdin:
        line = line.strip('\n')
        curr = line.split(SPLIT)
        if len(curr) != INPUT_FIELD_NUM:
            continue

        if curr[i.shop_id]==a_shop_id and curr[i.user_id]==a_user_id:
            if(int(curr[i.log_type])>0):
                if( len(last) == 0 ):
                    continue

                if( m_auctions.get(curr[i.auction_id]) ):
                    # 直接成交
                    out = OutputRecord(m_auctions.get(curr[i.auction_id]))
                    out.direct(curr)
                    out.output() 
                else :
                    # 间接成交
                    out = OutputRecord(last)
                    out.indirect(curr)
                    out.output() 
            else:
                # 保存auction_id列表，计算直接成交
                m_auctions[curr[i.auction_id]] = curr
                # 保留最近的curr
                last = curr 
        else:
            a_shop_id = curr[i.shop_id]
            a_user_id = curr[i.user_id]
            if( int(curr[i.log_type])<=0 ):
                # 保存auction_id列表，计算直接成交
                m_auctions[curr[i.auction_id]] = curr
                # 保留最近的curr
                last = curr 
            else:
                last = []


if __name__ == "__main__":
    main(sys.argv)

