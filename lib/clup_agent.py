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
@description: clup-agent主模块
"""

import logging
import os
import socket
import sys
import time
from optparse import OptionParser


import config
import csuapp
import grace_exit
import logger
import register_node
import rpc_utils
import service_hander
import version

# import agent_collect_db_stats


socket.setdefaulttimeout(20)
exit_flag = 0


def start(foreground):

    csuapp.prepare_run('clup-agent', foreground)

    logging.info("========== clup-agent starting ==========")
    while not csuapp.is_exit():
        err_code, data, can_retry = register_node.register_node()
        server_address = rpc_utils.get_server_address()

        if err_code == 0:
            for item in data:
                config.set_key(item, str(data[item]))

            logging.info(f"Successfully registered this node to the server({server_address}).")
            break
        if err_code > 0:
            logging.error(f"The server({server_address}) has rejected the registration of this node: {data}")
        else:
            logging.error(f"Failed to register to the server({server_address}): {data}")

        if not can_retry:
            logging.info("========== clup-agent stoppped ==========")
            sys.exit(1)

        logging.error(f"Failed to register to the server({server_address}): {data}")
        logging.info("sleep 30 ...")
        time.sleep(30)


    # 启动对外的管理服务
    service_hander.run_service()

    if grace_exit.wait_all_thread_exit():
        logging.info("========== clup-agent stoppped ==========")
        sys.exit(0)
    else:
        logging.info("========== clup-agent force stoppped ==========")
        sys.exit(1)


def stop():
    csuapp.stop('clup-agent', retry_cnt=1, retry_sleep_seconds=1)


def status():
    err_code, status_msg = csuapp.status('clup-agent')
    print(status_msg)
    sys.exit(err_code)


def reg_service():
    script = os.path.join('/opt/clup-agent/bin', "clup-agent")
    err_code, err_msg = csuapp.reg_service('clup-agent', after_service_list=[
        'network.target'], service_script=script)
    if err_code != 0:
        print(err_msg)


def main():
    usage = "usage: %prog <command> [options]\n" \
            "    command can be one of the following:\n" \
            "      start  : start agent\n" \
            "      stop   : stop aggent \n" \
            "      status : display agent status\n" \
            "      reg_service : register to a system service\n" \
            "      version : display version information.\n" \
            ""
    parser = OptionParser(usage=usage)
    parser.add_option("-l", "--loglevel", action="store", dest="loglevel", default="info",
                      help="Specifies log level:  debug, info, warn, error, critical, default is info")
    parser.add_option("-f", "--foreground", action="store_true", dest="foreground",
                      help="Run in foreground, not daemon, only for start command.")

    if len(sys.argv) == 1 or sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print(version.copyright_message())
        parser.print_help()
        sys.exit(0)
    if sys.argv[1] == 'version':
        print(version.copyright_message())
        sys.exit(0)
    orig_args = sys.argv[2:]
    new_args = []
    for arg in orig_args:
        if len(arg) == 1:
            arg = '-' + arg
        new_args.append(arg)
    (options, _args) = parser.parse_args(new_args)

    log_level_dict = {"debug": logging.DEBUG,
                      "info": logging.INFO,
                      "warn": logging.WARN,
                      "error": logging.ERROR,
                      "critical": logging.CRITICAL,
                      }

    str_log_level = options.loglevel.lower()
    if str_log_level not in log_level_dict:
        sys.stderr.write("Unknown loglevel: " + options.loglevel)
        sys.exit(-1)

    # 初使用化日志
    log_level = log_level_dict[options.loglevel.lower()]
    logger.init(log_level)
    if sys.argv[1] in ['start', 'status', 'reg_service']:
        logging.info(version.copyright_message())
    # logging.info("Start loading configuration ...")
    config.load()
    # logging.info("Complete configuration loading.")

    if sys.argv[1] == 'start':
        start(options.foreground)
    elif sys.argv[1] == 'status':
        status()
    elif sys.argv[1] == 'stop':
        stop()
    elif sys.argv[1] == 'reg_service':
        reg_service()
    else:
        sys.stderr.write('Invalid command: %s\n' % sys.argv[1])
        sys.exit(1)


if __name__ == "__main__":
    main()
