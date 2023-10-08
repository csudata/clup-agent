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
@description: 文件传输模块
"""


import os
import threading
import queue
import time
import logging
import traceback

import rpc_utils
import config

__lock = threading.Lock()
__running_cft_dict = dict()


def set_cft_dict(cft_dict, state, err_msg, end_time=None):
    __lock.acquire()
    try:
        cft_dict['state'] = state
        cft_dict['err_msg'] = err_msg
        if end_time:
            cft_dict['end_time'] = end_time
    finally:
        __lock.release()


def create_cft(src_dir, dst_host, dst_dir, task_id=None, big_file_size=768 * 1024, trans_block_size=512 * 1024):
    global __lock
    global __running_cft_dict

    cft_id = int(time.time() * 10000000)
    cft_dict = {}
    cft_dict['cft_id'] = cft_id
    cft_dict['src_host'] = config.get('my_ip')
    cft_dict['src_dir'] = src_dir
    cft_dict['dst_host'] = dst_host
    cft_dict['dst_dir'] = dst_dir
    cft_dict['queue'] = queue.Queue(1)
    cft_dict['state'] = 0
    cft_dict['task_id'] = task_id
    cft_dict['big_file_size'] = big_file_size
    cft_dict['trans_block_size'] = trans_block_size


    __lock.acquire()
    try:
        # 先清除过期的pipe_cmd
        need_rm_list = []
        for tmp_id in __running_cft_dict:
            tmp_dict = __running_cft_dict[tmp_id]
            if 'end_time' in tmp_dict:
                end_time = tmp_dict['end_time']
                if time.time() - end_time > 24 * 3600:
                    need_rm_list.append(tmp_id)
        for tmp_id in need_rm_list:
            del __running_cft_dict[tmp_id]
        __running_cft_dict[cft_id] = cft_dict
    finally:
        __lock.release()

    t = threading.Thread(target=cft_run, args=(cft_dict,))
    t.setDaemon(True)  # 设置线程为后台线程
    cft_dict['thread'] = t
    t.start()
    return cft_id


def cft_batch_cmd(req_list):
    try:
        for req in req_list:
            attr = req['attr']
            mode = attr['mode'] & 0b111111111111
            if req['type'] == "dir":
                os.mkdir(req['path'], mode)
                os.utime(req['path'], (attr['atime'], attr['mtime']))
            elif req['type'] == 'link':
                os.symlink(req['linkto'], req['path'])
                os.utime(req['path'], (attr['atime'], attr['mtime']), follow_symlinks=False)
                # os.chmod是改指向文件的mode，如果指向文件不存在，改会报错,所以我们不要改执行文件的mode
                # os.chmod(req['path'], mode)
            elif req['type'] == 'file':
                with open(req['path'], 'wb') as fp:
                    fp.write(req['data'])
                os.utime(req['path'], (attr['atime'], attr['mtime']))
                os.chmod(req['path'], mode)
        return 0, ''
    except Exception:
        err_msg = f"cft_batch_cmd with unexpected error,{traceback.format_exc()}."
        return -1, err_msg


def set_file_attr(file_path, attr):
    try:
        mode = attr['mode'] & 0b111111111111
        os.utime(file_path, (attr['atime'], attr['mtime']), follow_symlinks=False)
        os.chmod(file_path, mode)
        return 0, ''
    except Exception:
        err_msg = f"set_file_attr with unexpected error,{traceback.format_exc()}."
        return -1, err_msg


def send_batch_req(dst_host, req_list):
    err_code, err_msg = rpc_utils.get_rpc_connect(dst_host)
    if err_code != 0:
        return err_code, err_msg
    rpc = err_msg
    try:
        err_code, err_msg = rpc.cft_batch_cmd(req_list)
    except Exception as e:
        err_code = -1
        err_msg = f"rpc.cft_batch_cmd failed: {repr(e)}"
    finally:
        rpc.close()
    return err_code, err_msg


def send_big_file(dst_host, local_file, req, trans_block_size, notify_handler=None):
    file_path = req['path']
    file_size = req['size']
    attr = req['attr']

    err_code, rpc = rpc_utils.get_rpc_connect(dst_host)
    if err_code != 0:
        return err_code, rpc

    try:
        fd = os.open(local_file, os.O_RDONLY)
    except Exception as e:
        rpc.close()
        return -1, repr(e)
    try:
        block_size = trans_block_size
        block_cnt = (file_size + block_size - 1) // block_size
        for i in range(block_cnt):
            offset = i * block_size
            try:
                data = os.read(fd, block_size)
            except Exception as e:
                err_msg = f"读文件{local_file}是发生错误：{repr(e)}"
                return -1, err_msg
            err_code, err_msg = rpc.os_write_file(file_path, offset, data)
            if err_code != 0:
                return err_code, err_msg
            read_size = len(data)
            offset += read_size
            if notify_handler:
                notify_handler.need_trans_size += read_size
                notify_handler.transed_size += read_size
                if notify_handler.need_trans_size > 5 * 1024 * 1024:
                    notify_handler.need_trans_size = 0
                    notify_handler.np.notify(notify_handler.transed_file_count, notify_handler.transed_size)
        err_code, err_msg = rpc.set_file_attr(file_path, attr)
    except Exception as e:
        exc_msg = traceback.format_exc()
        err_msg = f"send_big_file函数处发生未知错误: {exc_msg}"
        return -1, err_msg
    finally:
        os.close(fd)
        rpc.close()
    return err_code, err_msg


class NotifyProgress():
    """该类用于通知任务的进度
    """
    def __init__(self, task_id, interval):
        self.task_id = task_id
        self.interval = interval
        self.trans_time = time.time()

    def notify(self, file_count, transed_size):
        try:
            curr_time = time.time()
            if curr_time - self.trans_time >= self.interval:
                self.trans_time = curr_time
                if self.task_id:
                    rpc_utils.task_insert_log(task_id=self.task_id, task_state=0,
                        msg=f"{file_count} files , {transed_size//(1024*1024)} MB has been transmitted.",
                        task_type='general')
        except Exception as e:
            logging.error(f"notice progress failed: {repr(e)}")


class WalkHandler():
    """该类用于提供遍历到某个文件或目录的处理函数
    """
    def __init__(self, task_id, interval, src_dir, dst_host, dst_dir, big_file_size, trans_block_size):
        self.task_id = task_id
        self.interval = interval
        self.src_dir = src_dir
        self.dst_host = dst_host
        self.dst_dir = dst_dir
        self.big_file_size = big_file_size
        self.trans_block_size = trans_block_size

        self.transed_size = 0
        self.transed_file_count = 0
        self.req_list = []
        self.need_trans_size = 0
        self.np = NotifyProgress(task_id, interval)

    def process(self, item):
        stat_result = item.stat()
        attr = {
            "mode": stat_result.st_mode,
            "uid": stat_result.st_uid,
            "gid": stat_result.st_gid,
            "atime": stat_result.st_atime,
            "mtime": stat_result.st_mtime
        }
        local_file = item.path
        remote_file = self.dst_dir + local_file[len(self.src_dir):]
        req = {
            "path": remote_file,
            "attr": attr
        }

        if item.is_symlink():  # 注意需要先处理symlink，因为一个链接，使用item.is_dir()时也会为真
            req['type'] = 'link'
            req['linkto'] = os.readlink(item.path)
            self.req_list.append(req)
        elif item.is_dir():
            req['type'] = 'dir'
            self.req_list.append(req)
            if len(self.req_list) > 100:
                err_code, err_msg = send_batch_req(self.dst_host, self.req_list)
                if err_code != 0:
                    return err_code, err_msg
                self.need_trans_size = 0
                self.req_list = []
                # 通知进度
                self.np.notify(self.transed_file_count, self.transed_size)
        elif item.is_file():
            req['type'] = 'file'
            req['size'] = stat_result.st_size
            self.transed_file_count += 1
            # 如果文件比较大，则单独发生
            if stat_result.st_size >= self.big_file_size:
                # 单独发生之前，把之前积累的都发送出去
                if len(self.req_list) > 0:
                    err_code, err_msg = send_batch_req(self.dst_host, self.req_list)
                    if err_code != 0:
                        return err_code, err_msg
                    self.need_trans_size = 0
                    self.req_list = []
                    # 通知进度
                    self.np.notify(self.transed_file_count, self.transed_size)
                err_code, err_msg = send_big_file(self.dst_host, local_file, req, self.trans_block_size, self)
                if err_code != 0:
                    return err_code, err_msg
                # 通知进度
                self.np.notify(self.transed_file_count, self.transed_size)
                return 0, ''
            # 这是小文件的情况，积累了批量发送
            self.transed_size += stat_result.st_size
            with open(local_file, 'rb') as fp:
                data = fp.read()
            req['data'] = data
            self.req_list.append(req)
            self.need_trans_size += stat_result.st_size
            if self.need_trans_size >= self.big_file_size:
                err_code, err_msg = send_batch_req(self.dst_host, self.req_list)
                if err_code != 0:
                    return err_code, err_msg
                self.need_trans_size = 0
                self.req_list = []
                # 通知进度
                self.np.notify(self.transed_file_count, self.transed_size)
            return 0, ''
        return 0, ''

    def flush(self):
        if len(self.req_list) > 0:
            err_code, err_msg = send_batch_req(self.dst_host, self.req_list)
            if err_code != 0:
                return err_code, err_msg
        return 0, ''


def scandir(path, processFunc):
    err_code = 0
    err_msg = ''
    for item in os.scandir(path):
        # 注意需要先处理symlink，因为一个symlink, is_file()也可能为真
        if item.is_symlink():
            err_code, err_msg = processFunc(item)
        elif item.is_file():
            err_code, err_msg = processFunc(item)
        elif item.is_dir():
            err_code, err_msg = processFunc(item)
            if err_code != 0:
                break
            err_code, err_msg = scandir(item.path, processFunc)
        if err_code != 0:
            break
    return err_code, err_msg


def cft_run(cft_dict):

    try:
        src_dir = cft_dict['src_dir']
        dst_host = cft_dict['dst_host']
        dst_dir = cft_dict['dst_dir']
        log_interval = cft_dict.get('log_interval', 10)
        big_file_size = cft_dict.get('big_file_size', 768 * 1024)
        trans_block_size = cft_dict.get('trans_block_size', 512 * 1024)
        task_id = cft_dict['task_id']

        handler = WalkHandler(task_id, log_interval, src_dir, dst_host, dst_dir, big_file_size, trans_block_size)
        err_code, err_msg = scandir(src_dir, handler.process)
        if err_code != 0:
            set_cft_dict(cft_dict, -1, err_msg, int(time.time()))
            return
        # 把还缓存在hanlder中而文件发送出去
        err_code, err_msg = handler.flush()
        if err_code != 0:
            set_cft_dict(cft_dict, -1, err_msg, int(time.time()))
            return
        set_cft_dict(cft_dict, 1, 'success', int(time.time()))
    except Exception:
        exc_msg = traceback.format_exc()
        err_msg = f"发生未知错误：{exc_msg}"
        set_cft_dict(cft_dict, -1, err_msg, end_time=int(time.time()))


def get_cft_state(cft_id):
    global __lock
    global __running_cft_dict

    __lock.acquire()
    try:
        if cft_id not in __running_cft_dict:
            return -1, f"recv pipe cmd({cft_id}) not exists!"
        cft_dict = __running_cft_dict[cft_id]
        state = cft_dict['state']
        if state == 0:
            return 0, "running", state
        else:
            return 0, cft_dict['err_msg'], state
    finally:
        __lock.release()


def remove_cft(cft_id):
    global __lock
    global __running_cft_dict

    __lock.acquire()
    try:
        if cft_id not in __running_cft_dict:
            return -1, f"recv pipe cmd({cft_id}) not exists!"
        cft_dict = __running_cft_dict[cft_id]
        state = cft_dict['state']
        if state == 0:
            return -1, f"async copy cmd({cft_id}) is running!"
        else:
            del __running_cft_dict[cft_id]
            return 0, ''
    except Exception as e:
        return -1, str(e)
    finally:
        __lock.release()


def trans_dir(rpc, src_dir, dst_ip, dst_dir, task_id):
    """把rpc所在机器src_dir下的文件传输到dst_ip机器的dst_dir目录下
    Args:
        rpc ([type]): [description]
        src_dir ([type]): [description]
        dst_ip ([type]): [description]
        dst_dir ([type]): [description]
        task_id ([type]): [description]

    Returns:
        [int]: [err_code]
        [str]: [err_msg]
    """

    err_code = 0
    err_msg = ''
    cft_id = rpc.create_cft(src_dir, dst_ip, dst_dir, task_id)
    while True:
        err_code, err_msg, state = rpc.get_cft_state(cft_id)
        if err_code != 0:
            rpc.remove_cft(cft_id)
            return err_code, err_msg
        if state != 0:
            break
        time.sleep(5)
    rpc.remove_cft(cft_id)
    if state != 1:
        err_code = -1
        return -1, err_msg
    return err_code, err_msg



if __name__ == '__main__':
    pass
