#!/usr/bin/env python
# -*- coding:UTF-8

# Copyright (c) 2023 CSUDATA.COM and/or its affiliates.  All rights reserved.
# CLup is licensed under AGPLv3.
# See the GNU AFFERO GENERAL PUBLIC LICENSE v3 for more details.
# You can use this software according to the terms and conditions of the AGPLv3.
#
# THIS SOFTWARE IS PROVIDED BY CSUDATA.COM "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT, ARE
# DISCLAIMED.  IN NO EVENT SHALL CSUDATA.COM BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
@Author: tangcheng
@description: 配置管理模块
"""

import copy
import logging
import os
import sys
import threading
import traceback

import ip_lib


def get_root_path():
    if __file__.endswith(".py"):
        root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".." + os.sep))
    else:
        pid = os.getpid()
        proc_exe_file = f"/proc/{pid}/exe"
        exe_file = os.readlink(proc_exe_file)
        root_path = os.path.realpath(os.path.join(os.path.dirname(exe_file), '..' + os.sep))
    return root_path


__root_path = get_root_path()
__module_path = os.path.join(__root_path, 'lib')
__cfg_path = os.path.join(__root_path, 'conf')
__config_file = os.path.join(__cfg_path, '.' + os.sep + 'clup-agent.conf')
__data_path = os.path.join(__root_path, 'data')
__bin_path = os.path.join(__root_path, 'bin')
__log_path = os.path.join(__root_path, 'logs')
__tmp_path = os.path.join(__root_path, 'tmp')
__web_root = os.path.join(__root_path, 'ui')


if os.path.isdir('/run'):
    __run_path = '/run'
else:
    __run_path = '/var/run'

__data = {}

# 一个全局锁
__lock = threading.Lock()


def load():
    global __data
    global __lock

    __lock.acquire()
    try:
        with open(__config_file, encoding='utf-8') as f:
            for line in f:
                line = line.strip()

                if len(line) < 1:
                    continue
                if line[0] == "#":
                    continue
                elif line[0] == ";":
                    continue
                try:
                    pos = line.index('=')
                except ValueError:
                    continue
                key = line[:pos].strip()
                value = line[pos + 1:].strip()
                __data[key] = value
    except Exception:
        logging.error(f"Load configuration failed: {traceback.format_exc()}.")
        sys.exit(1)

    # 获得自己的IP地址，需要处理本机可能存在多个IP地址的情况
    try:
        nic_dict = ip_lib.get_nic_ip_dict()
        # 先获得一个IP地址的列表
        ip_list_dict = {}
        for nic in nic_dict:
            if 'ipv4' not in nic_dict[nic]:
                continue
            ipv4_dict = nic_dict[nic]['ipv4']
            for ip in ipv4_dict:
                if ip == '127.0.0.1':
                    continue
                # 掩码长度为32，通常是vip，忽略
                if ipv4_dict[ip] == 32:
                    continue
                ip_list_dict[ip] = ipv4_dict[ip]
        if len(ip_list_dict) > 1:
            network_addr = __data.get('mgr_network', '')
            if not network_addr:
                str_ip_list = str(list(ip_list_dict.keys()))
                err_msg = f"This machine have more than one ip({str_ip_list}), must config 'mgr_network' in clup-agent.conf!!!"
                logging.fatal(err_msg)
                sys.exit(1)
            network_num = ip_lib.ipv4_to_num(network_addr)

            my_ip = ''
            for ip in ip_list_dict:
                netmask_len = ip_list_dict[ip]
                netmask_num = int('1' * netmask_len + '0' * (32 - netmask_len), 2)
                ip_num = ip_lib.ipv4_to_num(ip)
                if ip_num & netmask_num == network_num:
                    my_ip = ip
                    break
            if not my_ip:
                str_ip_list = str(list(ip_list_dict.keys()))
                err_msg = f"Config mgr_network is error, this machine ip({str_ip_list}) not in this network({network_addr})!"
                logging.error(err_msg)
                sys.exit(1)
        else:
            my_ip = list(ip_list_dict.keys())[0]
        __data['my_ip'] = my_ip
    except Exception:
        err_msg = f"An unknown error has occurred: {traceback.format_exc()}."
        logging.fatal(err_msg)
        sys.exit(1)
    finally:
        __lock.release()



def get_run_path():
    """
    :return: 返回run目录
    """
    return __run_path


def get_cfg_path():
    """获得conf目录路径"""
    return __cfg_path


def get_module_path():
    return __module_path


def get_data_path():
    return __data_path


def get_bin_path():
    return __bin_path


def get_log_path():
    return __log_path


def get_tmp_path():
    return __tmp_path


def get_web_root():
    return __web_root


def get_pid_file():
    global __run_path
    return "%s/clup-agent.pid" % __run_path


def get(key, def_val=''):
    global __data
    global __lock
    __lock.acquire()
    try:
        if key not in __data:
            return def_val
        return __data[key]
    finally:
        __lock.release()


def set_key(key, value):
    global __data
    global __lock
    __lock.acquire()
    try:
        __data[key] = value
    finally:
        __lock.release()


def getint(key):
    global __data
    global __lock
    __lock.acquire()
    try:
        return int(__data[key])
    finally:
        __lock.release()


def has_key(key):
    global __data
    global __lock
    __lock.acquire()
    try:
        return key in __data
    finally:
        __lock.release()


def get_all():
    global __data
    global __lock
    __lock.acquire()
    try:
        ret_data = copy.copy(__data)
        return ret_data
    finally:
        __lock.release()
