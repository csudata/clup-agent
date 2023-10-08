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
@description: 跨机器的管道
"""


import logging
import os
import queue
import select
import subprocess
import threading
import time
import traceback

import config
import rpc_utils

__lock = threading.Lock()
__chp_cmd_dict = dict()
__chp_pipe_out_cmd_dict = dict()



def run_cmd_readout(cmd, stdout_callback, stderr_callback):
    out_data = b''
    err_data = b''
    total_err_data = b''
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
                    out_data = p.stdout.read(512 * 1024)
                    if not out_data:
                        read_empty_count += 1
                    else:
                        err_code, err_msg = stdout_callback(out_data)
                        if err_code != 0:
                            break
                if r == p_stderr_fd:
                    err_data = p.stderr.read(384 * 1024)
                    if not err_data:
                        err_empty_count += 1
                    else:
                        if len(total_err_data) < 384 * 1024:
                            total_err_data += err_data
                        err_empty_count = 0
                        stderr_callback(err_data)
            for r in es:
                rlist.remove(r)
            if len(rlist) == 0:
                break
            if err_empty_count > 10 or read_empty_count > 10:
                break
        err_no = p.wait()
        if err_code == 0:
            err_code = err_no
    except Exception as e:
        err_code = -1
        err_msg = str(e)
    if err_code != 0:
        if err_msg:
            err_msg += ' *** ' + total_err_data.decode()
        else:
            err_msg = total_err_data.decode()

    return err_code, err_msg


class PipeCmdCallback():
    """该类用于pipecmd的发送数据的回调函数
    """
    def __init__(self, rpc, cmd_id):
        self.rpc = rpc
        self.cmd_id = cmd_id

    def stdout(self, data):
        try:
            req = {}
            req['type'] = 'DATA'
            err_code, err_msg = self.rpc.chp_send_pipe_out_data(self.cmd_id, req, data)
            return err_code, err_msg
        except Exception as e:
            return -1, str(e)

    def stderr(self, data):
        logging.error(f"pipe_cmd({self.cmd_id}): {data.decode()}")


def thread_func_pipe_out_cmd(cmd_dict):
    """
    rpc中启动一个线程，此线程执行这个函数
    """
    global __lock
    global __chp_cmd_dict

    try:
        cmd_id = cmd_dict['cmd_id']
        dst_cmd = cmd_dict['dst_cmd']
        src_host = cmd_dict['src_host']
    except Exception:
        logging.error(f"pipe_out_cmd invalid cmd_dict: {cmd_dict}.")
        return

    pre_msg = f"pipe_out_cmd(cmd_id={cmd_id})"
    logging.info(f"begin run {pre_msg}...")
    try:
        err_code, rpc = rpc_utils.get_rpc_connect(src_host)
        if err_code != 0:
            logging.error(f"{pre_msg} failed: can not connect {src_host}!")
            return err_code, rpc
    except Exception as e:
        logging.error(f"{pre_msg} failed: can not connect {src_host}: {repr(e)}!")
        return -1, repr(e)

    logging.info(f"{pre_msg} begin run {dst_cmd} ...")
    callback = PipeCmdCallback(rpc, cmd_id)
    err_code, err_msg = run_cmd_readout(dst_cmd, callback.stdout, callback.stderr)
    if err_code != 0:
        logging.info(f"{pre_msg} failed: {err_msg}")

    try:
        req = {}
        req['type'] = 'CLOSE'
        req['err_code'] = err_code
        if err_code != 0:
            req['err_msg'] = err_msg

        err_code, err_msg = rpc.chp_send_pipe_out_data(cmd_id, req, b'')
        if err_code != 0:
            err_msg = f"pipe_out_cmd(cmd_id={cmd_id} call rpc.chp_send_pipe_out_data error: {err_msg}"
            logging.error(err_msg)
    except Exception as e:
        err_code = -1
        err_msg = f"pipe_out_cmd(cmd_id={cmd_id} unknown error: {repr(e)}"
        logging.error(err_msg)

    __lock.acquire()
    try:
        if cmd_id in __chp_cmd_dict:
            cmd_dict = __chp_cmd_dict[cmd_id]
            cmd_dict['err_msg'] = err_msg
            if err_code == 0:
                cmd_dict['state'] = 1
            else:
                cmd_dict['state'] = -1
            cmd_dict['end_time'] = int(time.time())
    finally:
        __lock.release()

    return err_code, err_msg


def create_pipe_out_cmd(cmd_dict):
    """
    处理rpc请求，执行一个命令，把命令标准输出送到另一端
    """

    global __lock
    global __chp_pipe_out_cmd_dict

    t = threading.Thread(target=thread_func_pipe_out_cmd, args=(cmd_dict,))
    t.setDaemon(True)  # 设置线程为后台线程
    cmd_dict['thread'] = t
    __lock.acquire()
    try:
        # 先清除过期的pipe_cmd
        need_rm_list = []
        for tmp_id in __chp_pipe_out_cmd_dict:
            tmp_dict = __chp_pipe_out_cmd_dict[tmp_id]
            if 'end_time' in tmp_dict:
                end_time = tmp_dict['end_time']
                if time.time() - end_time > 7 * 24 * 3600:
                    need_rm_list.append(tmp_id)
        for tmp_id in need_rm_list:
            del __chp_pipe_out_cmd_dict[tmp_id]
        cmd_id = cmd_dict['cmd_id']
        __chp_pipe_out_cmd_dict[cmd_id] = cmd_dict
    finally:
        __lock.release()
    t.start()
    return 0, ''


def remove_pipe_out_cmd(cmd_id):
    global __lock
    global __chp_pipe_out_cmd_dict

    __lock.acquire()
    try:
        if cmd_id not in __chp_pipe_out_cmd_dict:
            return -1, f"chp pipe out cmd({cmd_id}) not exists!"
        cmd_dict = __chp_pipe_out_cmd_dict[cmd_id]
        state = cmd_dict['state']
        if state == 0:
            return -1, f"chp pipe out cmd({cmd_id}) is running!"
        else:
            del __chp_pipe_out_cmd_dict[cmd_id]
            return 0, ''
    except Exception as e:
        return -1, str(e)
    finally:
        __lock.release()


def recv_pipe_out_data(cmd_id, req, data):
    global __lock
    global __chp_cmd_dict

    __lock.acquire()
    try:
        if cmd_id not in __chp_cmd_dict:
            return -1, f"recv pipe cmd({cmd_id}) not exists!"
        cmd_dict = __chp_cmd_dict[cmd_id]
        recv_q = cmd_dict['queue']
        state = cmd_dict['state']
    finally:
        __lock.release()
    if state != 0:
        return -1, f"pipe cmd({cmd_id} already finished(code={state})!"
    recv_q.put((req, data))
    return 0, ''


def set_cmd_dict(cmd_dict, state, err_msg, end_time=None):
    __lock.acquire()
    try:
        cmd_dict['state'] = state
        cmd_dict['err_msg'] = err_msg
        if end_time:
            cmd_dict['end_time'] = end_time
    finally:
        __lock.release()


def set_transferred_size(cmd_dict, transferred_size):
    __lock.acquire()
    try:
        cmd_dict['transferred_size'] = transferred_size
    finally:
        __lock.release()


def pipe_cmd(cmd_dict):
    """
    """

    cmd_id = cmd_dict['cmd_id']
    src_cmd = cmd_dict['src_cmd']
    dst_host = cmd_dict['dst_host']
    pre_msg = f"pipe_cmd(cmd_id={cmd_id})"

    logging.info(f"begin run {pre_msg}...")
    logging.info("clean expired pipe_cmd...")
    __lock.acquire()
    try:
        # 先清除过期的pipe_cmd
        need_rm_list = []
        for tmp_id in __chp_cmd_dict:
            tmp_dict = __chp_cmd_dict[tmp_id]
            if 'end_time' in tmp_dict:
                end_time = tmp_dict['end_time']
                if time.time() - end_time > 24 * 3600:
                    need_rm_list.append(tmp_id)
        for tmp_id in need_rm_list:
            logging.info(f"pipe_cmd(cmd_id={__chp_cmd_dict[tmp_id]['cmd_id']}) is cleaned!")
            del __chp_cmd_dict[tmp_id]
        __chp_cmd_dict[cmd_id] = cmd_dict
    finally:
        __lock.release()

    logging.info("expired pipe_cmd cleaned!")

    err_code, rpc = rpc_utils.get_rpc_connect(dst_host)
    if err_code != 0:
        err_msg = f"Can not connect {dst_host}: {rpc}"
        set_cmd_dict(cmd_dict, -1, err_msg)
        prt_err_msg = f"{pre_msg} failed: {err_msg}"
        logging.error(prt_err_msg)
        return err_code, err_msg

    # 先启动远程的命令
    rpc_cmd_dict = {}
    rpc_cmd_dict.update(cmd_dict)
    del rpc_cmd_dict['queue']
    del rpc_cmd_dict['thread']
    try:
        err_code, err_msg = rpc.chp_create_pipe_out_cmd(rpc_cmd_dict)
        if err_code != 0:
            err_msg = f"Can rpc.chp_create_pipe_out_cmd({dst_host}) failed: {err_msg}"
            set_cmd_dict(cmd_dict, -1, err_msg)
            logging.error(f"{pre_msg} failed: {err_msg}")
            return err_code, err_msg
    except Exception as e:
        err_code = -1
        err_msg = repr(e)
        err_msg = f"Can rpc.chp_create_pipe_out_cmd({dst_host}) failed: {err_msg}"
        set_cmd_dict(cmd_dict, -1, err_msg)
        logging.error(f"{pre_msg} failed: {err_msg}")
        return err_code, err_msg
    finally:
        # 先把rpc给关掉，因为后面的过程运行的时间很久
        rpc.close()
        rpc = None

    # 启动本地的命令
    out_data = b''
    err_data = b''
    cmd_id = cmd_dict['cmd_id']
    recv_q = cmd_dict['queue']

    logging.info(f"{pre_msg} begin run cmd: {src_cmd}")
    try:
        p = subprocess.Popen(src_cmd, shell=True, close_fds=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        p_stdout_fd = p.stdout.fileno()
        p_stderr_fd = p.stderr.fileno()
        p_stdin_fd = p.stdin.fileno()

        os.set_blocking(p.stdout.fileno(), False)
        os.set_blocking(p.stderr.fileno(), False)
        # os.set_blocking(p.stdin.fileno(), False)

        rlist = [p_stdout_fd, p_stderr_fd]
        wlist = [p_stdin_fd]
        elist = rlist + wlist
        read_empty_count = 0
        err_empty_count = 0
        log_time = time.time()
        transferred_size = 0
        is_broken_pipe_error = False
        total_err_data = b''
        while True:
            rs, ws, es = select.select(rlist, wlist, elist)
            for fd in es:
                if fd in rlist:
                    rlist.remove(fd)
                if fd in wlist:
                    wlist.remove(fd)
            if len(wlist) == 0:
                break
            if len(rlist) == 0:
                break

            for r in rs:
                if r == p_stdout_fd:
                    out_data = p.stdout.read()
                    if not out_data:
                        read_empty_count += 1
                    else:
                        logging.debug(f"{prt_err_msg}: {out_data.decode()}")
                if r == p_stderr_fd:
                    err_data = p.stderr.read()
                    if not err_data:
                        err_empty_count += 1
                    else:
                        if len(total_err_data) < 512 * 1024:
                            total_err_data += err_data
                        err_empty_count = 0
                        logging.error(f"{pre_msg}: {err_data.decode()}")

            if not is_broken_pipe_error:
                for _w in ws:
                    req, data = recv_q.get()
                    if req['type'] == 'DATA':
                        try:
                            data_len = len(data)
                            pos = 0
                            while pos < data_len:
                                send_len = p.stdin.write(data[pos:])
                                if send_len != data_len:
                                    logging.warning(f" ******* send_len({send_len}) != data_len({data_len})")
                                pos += send_len
                        except BrokenPipeError:
                            is_broken_pipe_error = True
                            break
                        transferred_size += len(data)
                        curr_time = time.time()
                        if curr_time - log_time >= 10:
                            set_transferred_size(cmd_dict, transferred_size)
                            log_time = curr_time
                    elif req['type'] == 'CLOSE':
                        p.stdin.close()
                        read_empty_count = 999999
                        if req['err_code'] != 0:
                            err_code = req['err_code']
                            if err_msg:
                                err_msg += ' *** ' + req['err_msg']
                            else:
                                err_msg = req['err_msg']
                        break

            if err_empty_count > 10 or read_empty_count > 10:
                break
        err_no = p.wait()
        if err_code != 0:
            err_code = err_no
    except BrokenPipeError as e:
        err_code = -1
        err_msg = f"{pre_msg} maybe exit,because write data error: {repr(e)}"
        set_cmd_dict(cmd_dict, -1, err_msg)
        logging.error(err_msg)

    except Exception as e:
        err_code = -1
        exc_msg = traceback.format_exc()
        err_msg = f"{pre_msg}: An unknown error has occurred: {exc_msg}"
        set_cmd_dict(cmd_dict, -1, err_msg)
        logging.error(err_msg)

    if is_broken_pipe_error:
        err_code = -1
    if err_code != 0:
        pre_err_msg = ''
        if err_msg:
            pre_err_msg = f' *** {err_msg}'
        err_msg = total_err_data.decode()
        if pre_err_msg:
            err_msg += pre_err_msg

    rpc_is_ok = False
    try:
        err_code, rpc = rpc_utils.get_rpc_connect(dst_host)
        if err_code != 0:
            err_msg = f"Can not connect {dst_host}: {rpc}"
            set_cmd_dict(cmd_dict, -1, err_msg)
            prt_err_msg = f"{pre_msg} failed: {err_msg}"
            logging.error(prt_err_msg)
        else:
            rpc_is_ok = True
    except Exception as e:
        err_msg = f"Can not connect {dst_host}: {repr(e)}"
        set_cmd_dict(cmd_dict, -1, err_msg)
        prt_err_msg = f"{pre_msg} failed: {err_msg}"
        logging.error(prt_err_msg)

    if rpc_is_ok:
        try:
            rpc.chp_remove_pipe_out_cmd(cmd_id)
        except Exception as e:
            err_msg = f"Can rpc.chp_remove_pipe_out_cmd({cmd_id}) failed: {err_msg}"
            set_cmd_dict(cmd_dict, -1, err_msg)
            logging.error(f"{pre_msg} failed: {err_msg}")
        finally:
            rpc.close()
            rpc = None

    if err_code == 0:
        set_cmd_dict(cmd_dict, 1, "success", int(time.time()))
    else:
        set_cmd_dict(cmd_dict, -1, err_msg, int(time.time()))

    return err_code, err_msg


def get_chp_state(cmd_id):
    global __lock
    global __chp_cmd_dict

    __lock.acquire()
    try:
        if cmd_id not in __chp_cmd_dict:
            return -1, f"recv pipe cmd({cmd_id}) not exists!", -1
        cmd_dict = __chp_cmd_dict[cmd_id]
        state = cmd_dict['state']
        transferred_size = cmd_dict['transferred_size']
        if state == 0:
            return 0, transferred_size, state
        elif state == 1:
            return 0, transferred_size, state
        else:
            return 0, cmd_dict['err_msg'], state
    finally:
        __lock.release()


def remove_chp(cmd_id):
    global __lock
    global __chp_cmd_dict

    __lock.acquire()
    try:
        if cmd_id not in __chp_cmd_dict:
            return -1, f"recv pipe cmd({cmd_id}) not exists!"
        cmd_dict = __chp_cmd_dict[cmd_id]
        state = cmd_dict['state']
        if state == 0:
            return -1, f"recv pipe cmd({cmd_id}) is running!"
        else:
            del __chp_cmd_dict[cmd_id]
            return 0, ''
    except Exception as e:
        return -1, str(e)
    finally:
        __lock.release()


def create_chp(src_cmd, dst_host, dst_cmd):
    """创建一个跨机器的管道

    Args:
        src_cmd ([type]): [description]
        dst_host ([type]): [description]
        dst_cmd ([type]): [description]

    Returns:
        [type]: [description]
    """
    global __lock
    global __chp_cmd_dict

    cmd_id = int(time.time() * 10000000)
    cmd_dict = {}
    cmd_dict['cmd_id'] = cmd_id
    cmd_dict['src_host'] = config.get('my_ip')
    cmd_dict['src_cmd'] = src_cmd
    cmd_dict['dst_cmd'] = dst_cmd
    cmd_dict['dst_host'] = dst_host
    cmd_dict['queue'] = queue.Queue(1)
    cmd_dict['state'] = 0
    cmd_dict['transferred_size'] = 0
    t = threading.Thread(target=pipe_cmd, args=(cmd_dict,))
    t.setDaemon(True)  # 设置线程为后台线程
    cmd_dict['thread'] = t
    t.start()
    __lock.acquire()
    try:
        # 先清除过期的pipe_cmd
        need_rm_list = []
        for tmp_id in __chp_cmd_dict:
            tmp_dict = __chp_cmd_dict[tmp_id]
            if 'end_time' in tmp_dict:
                end_time = tmp_dict['end_time']
                if time.time() - end_time > 24 * 3600:
                    need_rm_list.append(tmp_id)
        for tmp_id in need_rm_list:
            del __chp_cmd_dict[tmp_id]
        __chp_cmd_dict[cmd_id] = cmd_dict
    finally:
        __lock.release()
    return 0, cmd_id


def run_chp(remote_host, remote_cmd, local_cmd):
    """
    执行一个跨机器的管道，机远程把远程命令的输出管道到本地的命令local_cmd上
    """
    err_code, err_msg = create_chp(local_cmd, remote_host, remote_cmd)
    if err_code != 0:
        return err_code, err_msg
    cmd_id = err_msg
    while True:
        err_code, err_msg, state = get_chp_state(cmd_id)
        if err_code != 0:
            break
        if state != 0:
            if state < 0:
                err_code = -1
                if not err_msg:
                    err_msg = 'remote command failed!'
            break
        time.sleep(5)
    remove_chp(cmd_id)
    return err_code, err_msg


def trans_dir(remote_host, remote_dir, local_dir):
    """
    把远程目录下的文件都拷贝到本地的目录中
    """
    local_cmd = f"tar -xf - -C {local_dir}"
    remote_cmd = f"tar -cf - -C {remote_dir} ."
    err_code, err_msg = create_chp(local_cmd, remote_host, remote_cmd)
    if err_code != 0:
        return err_code, err_msg
    cmd_id = err_msg
    while True:
        err_code, err_msg, state = get_chp_state(cmd_id)
        if err_code != 0:
            break
        if state != 0:
            err_code = -1
            if not err_msg:
                err_msg = 'remote command failed!'
            break
        time.sleep(5)
    remove_chp(cmd_id)
    return err_code, err_msg


if __name__ == '__main__':
    pass
