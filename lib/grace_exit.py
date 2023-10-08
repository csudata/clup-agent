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
@description: 让程序能友好退出
"""

import logging
import os
import signal
import threading
import time

import config

_exit_flag = False

_exit_handles = []


def is_exit():
    """是否置了退出标志
    :return:bool
    """
    return _exit_flag


def set_exit():
    """
    设置退出状态为真
    :return:无
    """
    global _exit_flag
    _exit_flag = True


def wait_all_thread_exit():
    """
    先前已把全局变量g_exit_flag设置为True，然后等待各个线程退出
    :return:
    """
    global _exit_handles

    # 调用退出处理函数
    for handle in _exit_handles:
        handle()

    # 试图让线程自己中止，如果各个线程检测到g_exit_flag为1了，则会退出
    # 如果线程在9秒后没有停止，则最后会调用exit()强制停止进程

    all_threads = sorted(threading.enumerate(), key=lambda d: d.name)
    logging.debug("all_threads: %s" % repr(all_threads))

    thread_cnt = len(all_threads)
    stopped_thread_cnt = 0
    i = 0
    retry_cnt = 30
    for t in all_threads:
        if t.name == 'MainThread':  # 不需要等主线程退出，因为此函数就是在主线程中
            continue
        while True:
            is_alive = False
            try:
                is_alive = t.isAlive()
            except Exception:
                pass
            if is_alive:
                if i > retry_cnt:
                    logging.info("Not waiting for the thread(%s) to stop!" % t.name)
                    break
                time.sleep(0.3)
                i += 1
                continue
            else:
                stopped_thread_cnt += 1
                logging.info("Thread(%s) is stopped." % t.name)
                break

    os.unlink(config.get_pid_file())

    if stopped_thread_cnt != thread_cnt - 1:
        return False
    else:
        return True


def sig_handle(signum, frame):
    """
    :param signum:
    :param frame:
    :return:
    """
    global _exit_flag

    logging.info("========== Recv signal %d, Program will stop... ==========" % signum)
    _exit_flag = 1


def set_signal():
    """
    设置合适的signal，以便让程序友好退出
    """
    signal.signal(signal.SIGINT, sig_handle)
    signal.signal(signal.SIGTERM, sig_handle)
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    # 注意：设置了这条之后，会导致无法获得子进程的退出码
    # signal.signal(signal.SIGCHLD, signal.SIG_IGN)


def register_exit_handle(handle):
    global _exit_handles
    _exit_handles.append(handle)
