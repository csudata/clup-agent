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
@description: 注册节点
"""

import logging
import os
import socket

import config
import rpc_utils
import utils


def register_node():
    """
    :return: err_code, err_msg, can_retry
    """

    err_code, rpc = rpc_utils.get_server_connect()
    if err_code != 0:
        return err_code, rpc, True

    try:
        str_server_address = config.get('server_address')
        if not str_server_address:
            err_msg = 'can not find server_address config in clup-agent.conf!'
            logging.fatal(err_msg)
            os._exit(1)
        str_server_address = str_server_address.strip()
        logging.info(f"clup server host is {str_server_address}.")

        my_ip = config.get('my_ip')
        mem_size = utils.get_mem_size()
        cpu_info = utils.get_cpu_info()

        # sn = utils.get_machine_sn()
        os_type = utils.get_os_type()
        err, data = rpc.register_node(socket.gethostname(), my_ip, mem_size, cpu_info, os_type)
        if err != 0:
            if err > 0:
                can_retry = False
            else:
                can_retry = True
            logging.error(f"register failed: {data}.")
            return err, data, can_retry
        return 0, data, True
    except socket.error as e:
        logging.error(f"In register unknown error: {str(e)}.")
        return -1, str(e), True
    except UserWarning as e:
        logging.error(f"In register internal error: {str(e)}.")
        return -1, str(e), False
    finally:
        rpc.close()
