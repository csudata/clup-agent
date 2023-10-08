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
@description: OS命令运行模块
"""

import logging
import os
import select
import signal
import subprocess
import sys


def run_cmd(cmd):
    logging.debug(f"Run {cmd}")
    cp = subprocess.run(cmd, shell=True)
    return cp.returncode


def daemon_execv(path, args):

    pid = os.fork()
    if pid > 0:  # 父进程返回
        return
    pid = os.getpid()

    sys.stdout.flush()
    sys.stderr.flush()
    si = open('/dev/null', 'r')
    so = open('/dev/null', 'a+')
    se = open('/dev/null', 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # 把打开的文件句柄关闭，防止影响子进程
    fds = os.listdir('/proc/%s/fd' % pid)
    for fd in fds:
        int_fd = int(fd)
        if int_fd > 2:
            try:
                os.close(int_fd)
            except Exception:
                pass
    os.execv(path, args)


def __exec_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, close_fds=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_msg = b''
    err_msg = b''
    p_stdout_fd = p.stdout.fileno()
    p_stderr_fd = p.stderr.fileno()
    os.set_blocking(p.stdout.fileno(), False)
    os.set_blocking(p.stderr.fileno(), False)
    rlist = [p_stdout_fd, p_stderr_fd]
    out_data = b''
    err_data = b''
    while True:
        rs, _ws, es = select.select(rlist, [], [])
        for r in rs:
            if r == p_stdout_fd:
                out_data = p.stdout.read()
                out_msg += out_data
            if r == p_stderr_fd:
                err_data = p.stderr.read()
                err_msg += err_data
        for r in es:
            rlist.remove(r)
        if len(rlist) == 0:
            break
        if (not out_data) and (not err_data):
            break

    ret_out_msg = out_msg.decode('utf-8')
    ret_err_msg = err_msg.decode('utf-8')
    err_no = p.wait()
    return err_no, ret_err_msg, ret_out_msg


def open_cmd(cmd):

    try:
        err_code, err_msg, out_msg = __exec_cmd(cmd)
        logging.debug(f"Run {cmd}")
    except Exception as e:
        raise e

    if err_code:
        raise OSError(err_code, "Run cmd %s failed: \n %s" % (cmd, err_msg))
    return out_msg


def test_cmd(cmd):

    try:
        err_code, _err_msg, _out_msg = __exec_cmd(cmd)
        logging.debug(f"Run {cmd}")
        return err_code
    except Exception:
        return -1


def run_cmd_result(cmd):
    out_msg = b''
    try:
        err_code, err_msg, out_msg = __exec_cmd(cmd)
        logging.debug(f"Run {cmd}")
    except Exception as e:
        err_code = -1
        err_msg = str(e)

    return err_code, err_msg, out_msg


def run_cmd_real_time_out(cmd, q, log_q):
    p = subprocess.Popen(cmd, shell=True, close_fds=True,
            stdout=subprocess.PIPE, preexec_fn=os.setsid, stderr=subprocess.PIPE)
    out_msg = b''
    err_msg = b''
    p_stdout_fd = p.stdout.fileno()
    p_stderr_fd = p.stderr.fileno()
    os.set_blocking(p.stdout.fileno(), False)
    os.set_blocking(p.stderr.fileno(), False)
    rlist = [p_stdout_fd, p_stderr_fd]
    elist = [p_stdout_fd, p_stderr_fd]
    out_data = b''
    err_data = b''
    need_exists = False
    while True:
        rs, _ws, es = select.select(rlist, [], elist)
        val = q.get() if not q.empty() else ''
        for r in rs:
            if r == p_stdout_fd:
                out_data = p.stdout.read()
                out_msg += out_data
                if out_data and val == 'log':
                    log_q.put(out_data)
            if r == p_stderr_fd:
                err_data = p.stderr.read()
                if err_data == b'':
                    need_exists = True
                else:
                    logging.info(err_data.decode())
                    if val == 'log':
                        log_q.put(err_data.decode())
                err_msg += err_data
        if val == 'terminate':
            err_msg += "\n强制停止".encode('utf-8')
            out_msg += "\n强制停止".encode('utf-8')
            # p.terminate()
            os.killpg(p.pid, signal.SIGTERM)
            break
        for r in es:
            rlist.remove(r)
        if len(rlist) == 0:
            break
        if (not out_data) and (not err_data):
            break
        if need_exists:
            break

    ret_out_msg = out_msg.decode('utf-8')
    ret_err_msg = err_msg.decode('utf-8')
    err_no = p.wait()
    return err_no, ret_err_msg, ret_out_msg


def run_cmd_real_time_out1(cmd):
    p = subprocess.Popen(cmd, shell=True, close_fds=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_msg = b''
    err_msg = b''
    p_stdout_fd = p.stdout.fileno()
    p_stderr_fd = p.stderr.fileno()
    os.set_blocking(p.stdout.fileno(), False)
    os.set_blocking(p.stderr.fileno(), False)
    rlist = [p_stdout_fd, p_stderr_fd]
    elist = [p_stdout_fd, p_stderr_fd]
    out_data = b''
    err_data = b''
    need_exists = False
    while True:
        rs, _ws, es = select.select(rlist, [], elist)
        for r in rs:
            if r == p_stdout_fd:
                out_data = p.stdout.read()
                out_msg += out_data
                if out_data:
                    logging.info(out_data)
            if r == p_stderr_fd:
                err_data = p.stderr.read()
                if err_data == b'':
                    need_exists = True
                else:
                    logging.info(err_data.decode())
                err_msg += err_data
        for r in es:
            rlist.remove(r)
        if len(rlist) == 0:
            break
        if (not out_data) and (not err_data):
            break
        if need_exists:
            break

    ret_out_msg = out_msg.decode('utf-8')
    ret_err_msg = err_msg.decode('utf-8')
    err_no = p.wait()
    return err_no, ret_err_msg, ret_out_msg


def run_cmd_read_lines(cmd, stdout_callback, stderr_callback):
    out_data = b''
    err_data = b''
    try:
        p = subprocess.Popen(cmd, shell=True, close_fds=True, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        logging.debug(f"Run {cmd}")
        # p.stdin.write()
        # p.stdin.flush()
        # p.stdin.close()

        p_stdout_fd = p.stdout.fileno()
        p_stderr_fd = p.stderr.fileno()
        # p_stdin_fd = p.stdin.fileno()

        os.set_blocking(p.stdout.fileno(), False)
        os.set_blocking(p.stderr.fileno(), False)

        rlist = [p_stdout_fd, p_stderr_fd]
        wlist = []
        read_empty_count = 0
        err_empty_count = 0
        while True:
            rs, _, es = select.select(rlist, wlist, [])

            for r in rs:
                if r == p_stdout_fd:
                    out_data = p.stdout.read()
                    if not out_data:
                        read_empty_count += 1
                    else:
                        stdout_callback(out_data.decode())
                if r == p_stderr_fd:
                    err_data = p.stderr.read()
                    if not err_data:
                        err_empty_count += 1
                    else:
                        err_empty_count = 0
                        stderr_callback(err_data.decode())
            for r in es:
                rlist.remove(r)
            if len(rlist) == 0:
                break
            if err_empty_count > 10 or read_empty_count > 10:
                break
        err_no = p.wait()
    except Exception as e:
        err_no = -1
        err_data = str(e).encode()

    return err_no, err_data.decode(), out_data.decode()


def send_to_exec(cmd, data):
    """
    """
    out_msg = ''
    err_code = 0
    try:
        logging.debug(f"run start: {cmd}, stdin data: {data}.")
        p = subprocess.Popen(cmd, shell=True, close_fds=True,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        b_data = (data + "\n").encode()
        p.stdin.write(b_data)
        p.stdin.close()
        out_msg = p.stdout.read().decode('utf-8')
        err_msg = p.stderr.read().decode('utf-8')
        err_code = p.wait()
    except Exception as e:
        if err_code == 0:
            err_code = -1
        err_msg = str(e)
    return err_code, err_msg, out_msg


if __name__ == '__main__':
    pass
