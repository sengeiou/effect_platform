#!/usr/bin/env python
# -*- encoding:utf-8 -*-

import sys

SPLIT = '\001'

INPUT_FIELD_NUM = 24;
OUTPUT_FIELD_NUM = 22;

class i:
    (   
            log_type,
            ts              , 
            refer_channel_id,     
            trade_track_info,
            user_id         ,     
            channel_src     ,    
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
            outside_gmv_trade_num         ,   
            outside_gmv_amt         ,
            outside_alipay_trade_num       ,
            outside_alipay_amt      
    ) = range(INPUT_FIELD_NUM)

class o:
    (
            refer_channel_id,     
            trade_track_info,
            user_id         ,     
            channel_src     ,    
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
            outside_gmv_trade_num         ,   
            outside_gmv_amt         ,
            outside_alipay_trade_num       ,
            outside_alipay_amt      
    ) = range(OUTPUT_FIELD_NUM)

class OutputRecord:
    def __init__(self, input_record):
        self.v = ['' for j in range(OUTPUT_FIELD_NUM)]
        self.v[o.refer_channel_id] = input_record[i.refer_channel_id]
        self.v[o.trade_track_info] = input_record[i.trade_track_info]
        self.v[o.user_id] = input_record[i.user_id]
        self.v[o.channel_src] = input_record[i.channel_src]
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
        self.v[o.outside_gmv_trade_num] = '0'
        self.v[o.outside_gmv_amt] = '0'
        self.v[o.outside_alipay_trade_num] = '0'
        self.v[o.outside_alipay_amt] = '0'
    
    def owner(self, record):
        self.v[o.outside_gmv_trade_num] = record[i.outside_gmv_trade_num]
        self.v[o.outside_gmv_amt] = record[i.outside_gmv_amt]
        self.v[o.outside_alipay_trade_num] = record[i.outside_alipay_trade_num]
        self.v[o.outside_alipay_amt] = record[i.outside_alipay_amt]
       
    def output(self):
        print SPLIT.join(self.v) 

def main(argv):
    a_trade_track_info = ''
    last = {}

    for line in sys.stdin:
        line = line.strip('\n')
        curr = line.split(SPLIT)
        if len(curr) != INPUT_FIELD_NUM:
            continue

        if curr[i.trade_track_info]==a_trade_track_info :
            if(int(curr[i.log_type])>0):
                for info in last.values():
                    if( len(info) == 0 ):
                        continue
                    out = OutputRecord(info)
                    out.owner(curr)
                    out.output() 
            else:
                last[curr[i.refer_channel_id]] = curr 
        else:
            a_trade_track_info = curr[i.trade_track_info]
            last = {}
            if( not int(curr[i.log_type])>0 ):
                last[curr[i.refer_channel_id]] = curr
            else:
                last[curr[i.refer_channel_id]] = [] 


if __name__ == "__main__":
    main(sys.argv)

