#!/bin/python
# -*- coding:utf-8 -*-
import datetime
import time
import os
import commands


def string_datetime(time):
    """将string类型转换成datetime类型"""
    return datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")


def datetime_string(datetime):
    """将datetime类型转换成string类型"""
    return datetime.strftime("%Y-%m-%d %H:%M:%S")


def timestamp_datetime(value):
    """将unix时间戳转换成datetime"""
    value = datetime.datetime.fromtimestamp(value)
    return value


def datetime_unix(dtime):
    """将date类型转换成unix时间戳"""
    return time.mktime(dtime.timetuple())


def execute_command(cmd):
    """执行cmd,包含异常处理"""
    try:
        ret = commands.getoutput(cmd)
        if ret.__contains__('command not found'):
            print '\033[1;31m\t  执行命令时出错\t\033[0m'
            exit(1)
        else:
            return ret
    except Exception as e:
        print '\033[1;31m\t  执行命令时出错\t\033[0m'
        print e
        exit(1)

# 巡检脚本目录
current_path = os.path.abspath(__file__)
father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".")
# 系统版本
cmd = "cat /usr/local/deepflow/conf/deepflow.conf | " \
      "grep -e ^version | cut -d ' ' -f 3"
system_version = commands.getoutput(cmd)