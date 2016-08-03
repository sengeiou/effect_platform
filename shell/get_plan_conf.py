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

# 全局变量

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


def get_remote_files(ydate):
    '''
    从admin_host机器获取用户当天新增加的计划配置文件到本地的临时目录
    '''
    if not path.exists(local_temp_path) :
        os.makedirs(local_temp_path)

    conf = {
    'YYYY' : ydate.strftime('%Y') ,
    'MM' : ydate.strftime('%m') ,
    'DD' : ydate.strftime('%d') ,
    }
    cmd = "%s %s:%s/* %s" % (ssh_cmd, admin_host, remote_path % conf, local_temp_path)
    print cmd
    os_rt = os.system(cmd)
    if os_rt != 0:
        print "ERROR: %s" % cmd
    return os_rt

def parse_file(file_name):
    '''
    分析本地临时文件目录中的计划配置文件，根据 计算归属有效周期(period)， 路径树差分规则(tree_split) 属性放到不同的目录中
    '''
    return '11', 'none'

def move_local_file(period, tree_split, file_name):
    '''
    从temp目录转移本地文件到其他目录
    '''
    conf = {'PERIOD':period, 'TREE_SPLIT':tree_split, }
    local_file_path = local_path % conf;
    if not path.exists(local_file_path) :
        os.makedirs(local_file_path)
    os.rename(path.join(local_temp_path, file_name), path.join(local_file_path, file_name))
    return 0

def put_file_to_hadoop(period, tree_split, file_name):
    '''
    把本地文件put到云梯上
    '''
    conf = {'PERIOD':period, 'TREE_SPLIT':tree_split}
    h_conf = {'local_path':local_path % conf, 'hadoop_path':hadoop_path % conf, 'hadoop_cmd':hadoop_cmd, 'file_name':file_name}
    cmd = '''
%(hadoop_cmd)s dfs -rm %(hadoop_path)s/%(file_name)s
%(hadoop_cmd)s dfs -put %(local_path)s/%(file_name)s %(hadoop_path)s/%(file_name)s''' % h_conf
    print cmd
    os_rt = os.system(cmd)
    if os_rt != 0:
        print "ERROR: %s" % cmd
    return os_rt

def main():
    ydate = get_options()
    if ( get_remote_files(ydate) != 0 ):
        return -1
    for file_name in os.listdir(local_temp_path):
        period, tree_split = parse_file(file_name)
        if ( move_local_file(period, tree_split, file_name) != 0 ):
            return -1
        if ( put_file_to_hadoop(period, tree_split, file_name) != 0 ):
            return -1
    pass

if __name__ == '__main__':
    sys.exit(main())

