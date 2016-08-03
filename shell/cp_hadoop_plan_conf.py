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

def get_hadoop_files():
    h_conf = {'local_path':local_root_path, 'hadoop_path':hadoop_root_path, 'hadoop_cmd':hadoop_cmd}
    cmd = '''
rm -rf %(local_path)s
%(hadoop_cmd)s dfs -copyToLocal %(hadoop_path)s %(local_path)s''' % h_conf
    print cmd
    os_rt = os.system(cmd)
    if os_rt != 0:
        print "ERROR: %s" % cmd
    return os_rt

def main():
    return get_hadoop_files()

if __name__ == '__main__':
    sys.exit(main())

