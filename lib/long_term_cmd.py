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
@description: 后台长时间运行的命令模块
"""

import os
import queue
import select
import signal
import subprocess
import threading
import time
import traceback

__lock = threading.Lock()
__ltc_dict = dict()


def __run_cmd_real_time_out(cmd_dict):
    global __lock
    global __ltc_dict

    ret_code = 0
    err_code = 0
    err_msg = ''
    try:
        cmd_q = cmd_dict['cmd_q']
        cmd = cmd_dict['cmd']
        stdout_q = cmd_dict['stdout']
        stderr_q = cmd_dict['stderr']
        output_timeout = cmd_dict['output_timeout']

        p = subprocess.Popen(cmd, shell=True, close_fds=True,
                    stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE)
        p_stdout_fd = p.stdout.fileno()
        p_stderr_fd = p.stderr.fileno()
        os.set_blocking(p.stdout.fileno(), False)
        os.set_blocking(p.stderr.fileno(), False)
        rlist = [p_stdout_fd, p_stderr_fd]
        elist = [p_stdout_fd, p_stderr_fd]
        out_data = b''
        err_data = b''
        read_empty_err_msg_cnt = 0
        while True:
            rs, _ws, es = select.select(rlist, [], elist)
            val = cmd_q.get() if not cmd_q.empty() else ''
            for r in rs:
                if r == p_stdout_fd:
                    out_data = p.stdout.read()
                    if out_data:
                        stdout_q.put(out_data.decode(), block=True, timeout=output_timeout)
                if r == p_stderr_fd:
                    err_data = p.stderr.read()
                    if err_data == b'':
                        read_empty_err_msg_cnt += 1
                    else:
                        stderr_q.put(err_data.decode(), block=True, timeout=output_timeout)
            if val == 'terminate':
                err_msg = "强制停止"
                # p.terminate()
                os.killpg(p.pid, signal.SIGKILL)
                break
            for r in es:
                err_code = -1
                rlist.remove(r)
            if len(rlist) == 0:
                break
            if (not out_data) and (not err_data):
                break
            if read_empty_err_msg_cnt > 20:
                err_code = -1
                break
        ret_code = p.wait()
    except queue.Full:
        err_code = -1
        err_msg = 'write to output timeout!'
    except Exception:
        err_code = -1
        err_msg = traceback.format_exc()

    __lock.acquire()
    try:
        cmd_dict['ret_code'] = ret_code
        cmd_dict['err_code'] = err_code
        cmd_dict['err_msg'] = err_msg
        if err_code != 0 or ret_code != 0:
            cmd_dict['state'] = -1
        else:
            cmd_dict['state'] = 1
    finally:
        __lock.release()


def run_long_term_cmd(cmd, output_qsize=10, output_timeout=600):
    global __lock
    global __ltc_dict

    cmd_id = int(time.time() * 10000000)
    cmd_dict = {
        "stdout": queue.Queue(output_qsize),
        "stderr": queue.Queue(output_qsize),
        "cmd_q": queue.Queue(1),
        "cmd": cmd,
        "output_timeout": output_timeout,
        "err_code": 0,
        "err_msg": '',
        "state": 0
    }
    __lock.acquire()
    try:
        __ltc_dict[cmd_id] = cmd_dict
    finally:
        __lock.release()

    t = threading.Thread(target=__run_cmd_real_time_out, args=(cmd_dict,))
    t.setDaemon(True)  # 设置线程为后台线程
    t.start()
    return cmd_id


def get_long_term_cmd_state(cmd_id):
    global __lock
    global __ltc_dict

    __lock.acquire()
    try:
        cmd_dict = __ltc_dict[cmd_id]
        stdout_q = cmd_dict['stdout']
        stderr_q = cmd_dict['stderr']
        err_code = cmd_dict['err_code']
        err_msg = cmd_dict['err_msg']
        state = cmd_dict['state']
    finally:
        __lock.release()
    stdout_lines = []
    while not stdout_q.empty():
        line = stdout_q.get()
        stdout_lines.append(line)

    stderr_lines = []
    while not stderr_q.empty():
        line = stderr_q.get()
        stderr_lines.append(line)
    return state, err_code, err_msg, stdout_lines, stderr_lines


def remove_long_term_cmd(cmd_id):
    global __lock
    global __ltc_dict

    __lock.acquire()
    try:
        if cmd_id in __ltc_dict:
            del __ltc_dict[cmd_id]
    finally:
        __lock.release()
    return 0, ''


def terminate_long_term_cmd(cmd_id):
    global __lock
    global __ltc_dict

    __lock.acquire()
    try:
        if cmd_id not in __ltc_dict:
            return -1, f"cmd({cmd_id}) not exists"
        cmd_dict = __ltc_dict[cmd_id]
        cmd_q = cmd_dict['cmd_q']
    finally:
        __lock.release()

    try:
        cmd_q.put('terminate', timeout=10)
    except Exception:
        pass
    return 0, ''


if __name__ == '__main__':
    pass
