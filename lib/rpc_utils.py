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
@description: rpc工具的库
"""

import logging
import os
import time
# import traceback

import csurpc

import config

__server_address = None
__get_server_address_time = 0


def get_server_address():
    return __server_address


def get_server_connect():
    global __server_address
    global __get_server_address_time
    try:
        str_server_address = config.get('server_address')
        if not str_server_address:
            err_msg = 'can not find server_address config in clup-agent.conf!'
            logging.fatal(err_msg)
            os._exit(1)

        str_server_address = str_server_address.strip()
        hostport_list = str_server_address.split(',')
        host_list = []
        for hostport in hostport_list:
            cells = hostport.split(':')
            host_list.append((cells[0], int(cells[1])))
        if len(host_list) == 0:
            err_msg = 'can not server_address config in clup-agent.conf!'
            logging.fatal(err_msg)
            os._exit(1)

        if len(host_list) > 1:
            curr_time = time.time()
            # 减少频繁调用etcd获得server_address
            if __server_address is None or curr_time - __get_server_address_time > 60:
                # 做一个字典，key为主库ip, value为返回是这个主库的次数，先把次数初始化为0
                primary_host_dict = {}
                primary_port = '4242'
                my_config_clup_list = []
                for host, port in host_list:
                    primary_host_dict[host] = 0
                    primary_port = port
                    my_config_clup_list.append(host)
                rpc_conn_dict = {}
                for host, port in host_list:
                    server_address = "%s:%d" % (host, port)
                    try:
                        c1 = csurpc.Client()
                        c1.connect("tcp://%s" % server_address, password=config.get('internal_rpc_pass'))
                        rpc_conn_dict[host] = c1
                        primary_host, clup_host_list = c1.get_clup_node_info()
                        logging.debug(f"{host} return primary is {primary_host}, clup_host_list is {repr(clup_host_list)}.")
                        if len(clup_host_list) == 0:
                            logging.fatal(f"clup({host}) is not multiple clup mode, clup-agent exit!")
                            os._exit(1)
                        if list(set(my_config_clup_list) ^ (set(clup_host_list))):
                            logging.fatal(f"my config clup list({my_config_clup_list}) not equal return clup list({clup_host_list})!")
                            os._exit(1)
                        if not primary_host:
                            continue
                        if primary_host not in primary_host_dict:
                            logging.fatal(f"{host} return primary {primary_host} is not in my config({','.join(host_list)}), clup-agent exit!")
                            os._exit(1)
                        primary_host_dict[primary_host] += 1
                    except Exception as e:
                        logging.info(f"Can not connect to {server_address}: {str(e)}.")
                        continue
                actual_primary_host = ''
                for host in primary_host_dict:
                    if primary_host_dict[host] >= 2:
                        actual_primary_host = host
                        break
                if not actual_primary_host:
                    return -1, "Can not find primary clup!"
                actual_primary_address = f"{actual_primary_host}:{primary_port}"
                if __server_address is not None and actual_primary_address != __server_address:
                    logging.info(f"switch clup server from {__server_address} to {actual_primary_host}.")

                __server_address = actual_primary_address
                __get_server_address_time = curr_time
                for host in rpc_conn_dict:
                    if host != actual_primary_host:
                        rpc_conn_dict[host].close()
                c1 = rpc_conn_dict[actual_primary_host]
                return 0, c1
        else:
            __server_address = str_server_address
        c1 = csurpc.Client()
        c1.connect("tcp://%s" % __server_address, password=config.get('internal_rpc_pass'))
        return 0, c1
    except Exception as e:
        return -1, "Can not connect clup: " + str(e)


def get_rpc_connect(ip, rpc_port=0):
    try:
        if not rpc_port:
            rpc_port = config.get('agent_rpc_port')
        rpc_address = "tcp://%s:%s" % (ip, rpc_port)
        c1 = csurpc.Client()
        c1.connect(rpc_address, password=config.get('internal_rpc_pass'))
        return 0, c1
    except Exception as e:
        return -1, "Can not connect %s: %s" % (ip, str(e))


def os_read_file(host, file_path, offset, data_len):
    err_code, rpc = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect {host}: maybe host is down.")
        return err_code, rpc

    err_code, err_msg = rpc.os_read_file(file_path, offset, data_len)
    rpc.close()
    return err_code, err_msg


def pg_get_valid_wal_list_le_pt(host, pgdata, pt):
    err_code, rpc = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, rpc

    err_code, err_msg = rpc.pg_get_valid_wal_list_le_pt(pgdata, pt)
    if err_code != 0:
        rpc.close()
        logging.error(f"Call rpc pg_get_valid_wal_list_le_pt({pgdata}, {pt}) failed: {err_msg}.")
        return err_code, err_msg
    rpc.close()
    return err_code, err_msg


def task_insert_log(task_id, task_state, msg, task_type):
    # 在server端记录日志
    err_code, rpc = get_server_connect()
    if err_code != 0:
        logging.error(f"connect clup-server failed: {rpc}.")
        return err_code, rpc

    ret = rpc.task_insert_log(task_id, task_state, msg, task_type)
    rpc.close()
    return err_code, ret

