#!/usr/bin/env python
# -*- encoding:utf-8 -*-
'''
从admin_host获取配置文件列表，保存到本地
'''
import os
import sys
from os import path
import optparse
from datetime import datetime
from datetime import timedelta

from ep_config import *

def get_options():
    '''解析程序执行传入的参数
    '''
    p = optparse.OptionParser(usage="usage: %prog [-d]", version="1.0")
    p.add_option('-v', '--get-version', dest='getVersion', default=False, action='store_true', help='Get version number of plugin')
    p.add_option('-d', action='store', type='string', dest='ydate', default=(datetime.today()-timedelta(days=1)).strftime('%Y%m%d'), help='thedate, %Y%m%d')                                                                              
    options, arguments = p.parse_args()

    try:
        ydate = datetime.strptime(options.ydate, '%Y%m%d')
    except:
        print "ERROR: 参数日期错误"
    
    return ydate


def parse_file(file_name):
    '''
    分析本地临时文件目录中的计划配置文件，根据 计算归属有效周期(period)， 路径树差分规则(tree_split) 属性放到不同的目录中
    '''
    return '11', 'none'

def del_file_from_hadoop(period, tree_split, file_path, file_name):
    '''
    把本地文件put到云梯上
    '''
    conf = {
    'PERIOD':period, 
    'TREE_SPLIT':tree_split, 
    }
    h_conf = {'local_path':file_path, 'hadoop_path':hadoop_path % conf, 'hadoop_cmd':hadoop_cmd, 'file_name':file_name}
    cmd = '''
%(hadoop_cmd)s dfs -rm %(hadoop_path)s/%(file_name)s
''' % h_conf
    print cmd
    os_rt = os.system(cmd)
    if os_rt != 0:
        print "ERROR: %s" % cmd
    return os_rt

def main():
    ydate = get_options()
    conf = {
    'YYYY' : ydate.strftime('%Y') ,
    'MM' : ydate.strftime('%m') ,
    'DD' : ydate.strftime('%d') ,
    }
    if(path.isdir(remote_path % conf)):
        for file_name in os.listdir(remote_path % conf):
            period, tree_split = parse_file(file_name)
            if ( del_file_from_hadoop(period, tree_split, remote_del_path%conf, file_name) != 0 ):
                return -1
    pass

if __name__ == '__main__':
    sys.exit(main())

