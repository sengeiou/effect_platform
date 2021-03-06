#!/usr/bin/env python
# -*- encoding:utf-8 -*-

import sys

SPLIT = '\001'

INPUT_FIELD_NUM = 27
OUTPUT_FIELD_NUM = 24

class i:
    (  
            log_type        , 
            ts              , 
            trade_track_info, 
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
            is_new,
            source,
            cps_trade_num         ,   
            cps_amt        ,
            joinkey 
    ) = range(INPUT_FIELD_NUM)

class o:
    (
            trade_track_info, 
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
            is_new,
            source,
            cps_trade_num         ,   
            cps_amt       
    ) = range(OUTPUT_FIELD_NUM)

class OutputRecord:
    def __init__(self, input_record):
        self.v = ['' for j in range(OUTPUT_FIELD_NUM)]
        self.v[o.trade_track_info] = input_record[i.trade_track_info]
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
        self.v[o.is_new] = '0'
        self.v[o.source] = '0'
        self.v[o.cps_trade_num] = '0'
        self.v[o.cps_amt] = '0'
    
    def owner(self, record):
        self.v[o.is_new] = record[i.is_new]
        self.v[o.source] = record[i.source]
        self.v[o.cps_trade_num] = record[i.cps_trade_num]
        self.v[o.cps_amt] = record[i.cps_amt]
       
    def output(self):
        print SPLIT.join(self.v) 

def main(argv):
    a_session = a_cookie = a_index_root_path = ''
    a_joinkey = ''
    last = []

    for line in sys.stdin:
        line = line.strip('\n')
        curr = line.split(SPLIT)
        if len(curr) != INPUT_FIELD_NUM:
            continue
        if curr[i.joinkey] == a_joinkey :
            if( int(curr[i.log_type])>0 ):
                if( len(last)==0 ):
                    continue
                out = OutputRecord(last)
                out.owner(curr)
                out.output() 
            else:
                last = curr 
        else:
            a_joinkey = curr[i.joinkey]
            if( not int(curr[i.log_type])>0 ):
                last = curr
            else:
                last = [] 


if __name__ == "__main__":
    main(sys.argv)

