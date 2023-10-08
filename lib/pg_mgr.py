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
@description: PostgerSQL数据库管理模块
"""

import glob
import logging
import os
import pwd
import struct
import time
import traceback

import rpc_utils
import run_lib


def is_running(pgdata):
    """
    检查数据库是否正在运行
    :param pgdata: 数据库的数据目录
    :return: True表示正在运行，False表示没有运行
    """

    pg_pid_file = '%s/postmaster.pid' % pgdata
    if not os.path.exists(pg_pid_file):
        return False

    try:
        fp = open(pg_pid_file, "r")
        lines = fp.readlines()
        fp.close()
        str_pid = lines[0]
        str_pid = str_pid.strip()
        try:
            pid = int(str_pid)
        except ValueError:
            return False
        if not os.path.exists("/proc/%s" % pid):
            return False

        try:
            fp = open("/proc/%d/comm" % pid, "r")
            data = fp.read()
            fp.close()
            data = data.strip()
            # 增加了判断此进程的名称是否为postgres
            if data != 'postgres':
                return False
            else:
                return True
        except Exception:
            return False
    except IOError:
        return False


def pg_stop(pgdata, wait_time=0):
    """
    停止数据库
    :return: 如果成功，则返回True，如果失败则返回False
    """

    if not os.path.exists(pgdata):
        return -1, "directory %s not exists" % pgdata

    try:
        fs = os.stat(pgdata)
        upw = pwd.getpwuid(fs.st_uid)
    except Exception as e:
        return -1, str(e)

    # 把数据库停下来
    cmd = '''su - %s -c 'pg_ctl stop -m fast -w -D %s > /dev/null' ''' % (upw.pw_name, pgdata)
    run_lib.run_cmd(cmd)

    ret = is_running(pgdata)
    if wait_time == 0:
        return 0, ''
    while wait_time > 0:
        ret = is_running(pgdata)
        if ret:
            wait_time -= 1
            time.sleep(1)
        else:
            return 0, ''
    if ret:
        return -1, "Can not stop!"
    else:
        return 0, ''


def is_valid_wal(wal_file: str, data: bytes) -> bool:
    if len(wal_file) < 24:
        return False
    if len(data) < 16:
        return False
    only_file_name = wal_file[-24:]

    bytes_wal_file_size = data[32:36]
    wal_file_size, = struct.unpack('I', bytes_wal_file_size)
    # wal_file_size应该是2的n次方, n&(n-1)的结构是0，则n是2的幂次方
    if ((wal_file_size - 1) & wal_file_size) != 0:
        # 如果不是2的n次数，则不是一个有小的wal文件
        return False

    bytes_lsn = data[8:16]
    lsn, = struct.unpack('Q', bytes_lsn)
    # wal_log_id取lsn-1的高32bit
    wal_log_id = lsn >> 32
    # wal_log_id取lsn -1 的低32bit， 然后除以WAL文件的大小(默认大小)
    wal_log_seg = (lsn & 0xFFFFFFFF) // wal_file_size
    target_wal = "%08X%08X" % (wal_log_id, wal_log_seg)
    if only_file_name[8:24] == target_wal:
        return True
    else:
        return False


def get_last_valid_wal_file(pgdata):
    wal_path = os.path.join(pgdata, 'pg_wal')
    if not os.path.exists(wal_path):
        wal_path = os.path.join(pgdata, 'pg_xlog')
        if not os.path.exists(wal_path):
            return -1, f"wal path({wal_path}) not exist!"

    wal_file_list = glob.glob(f'{wal_path}/????????????????????????')
    wal_file_list = sorted(wal_file_list)
    last_wal_file = None
    for wal_file in wal_file_list:
        try:
            with open(wal_file, 'rb') as fp:
                data = fp.read(512)
                if is_valid_wal(wal_file, data):
                    last_wal_file = wal_file
        except Exception as e:
            logging.error(f"open or read file {wal_file} error:  {str(e)}.")
    if last_wal_file is None:
        return -1, "Can not find last wal in local"
    return 0, last_wal_file


def get_valid_wal_list_le_pt(pgdata, pt):
    """
    获得大于等于指定点(pt)的WAL文件，pt是把timeline去掉后的部分，如WAL文件，0000000100000002000000CC，则pt为: 00000002000000CC
    :param pgdata:
    :param pt:
    :return:
    """

    wal_path = os.path.join(pgdata, 'pg_wal')
    if not os.path.exists(wal_path):
        wal_path = os.path.join(pgdata, 'pg_xlog')
        if not os.path.exists(wal_path):
            return -1, f"wal path({wal_path}) not exist!"

    ret_wal_file_list = []
    wal_file_list = glob.glob(f'{wal_path}/????????????????????????')
    wal_file_list = sorted(wal_file_list)
    for wal_file in wal_file_list:
        try:
            fn = wal_file[-16:]
            if fn >= pt:
                with open(wal_file, 'rb') as fp:
                    data = fp.read(512)
                    if is_valid_wal(wal_file, data):
                        ret_wal_file_list.append(wal_file)

        except Exception as e:
            err_msg = f"open or read file {wal_file} error: {str(e)}"
            logging.error(err_msg)
            return -1, err_msg
    return 0, ret_wal_file_list


def cp_delayed_wal_from_pri(pri_ip, pri_pgdata, stb_pgdata):
    """
    直接通过rpc拷贝的方式把本standby落后的wal从主库上拷贝过来，通常希望主库是停止的。
    :param pri_ip:
    :param pri_pgdata:
    :param stb_pgdata:
    :return: err_code, err_msg
    """

    # 先把备库关掉，然后把主库上比较新的xlog文件都拷贝过来：
    err_code, err_msg = pg_stop(stb_pgdata, 30)
    if err_code != 0:
        return -1, "database is running, can not stop: %s" % err_msg

    # 获得数据目录的信息，主要是为了获得数据目录的属主和组，以便后面拷贝的文件也需要改成这个pgdata的属主和组
    fs = os.stat(stb_pgdata)

    wal_path = os.path.join(stb_pgdata, 'pg_wal')
    if not os.path.exists(wal_path):
        wal_path = os.path.join(stb_pgdata, 'pg_xlog')
        if not os.path.exists(wal_path):
            return -1, f"wal path({wal_path}) not exist!"


    # 先遍历本地的WAL文件，找出最后一个有效的WAL文件
    err_code, last_wal_file = get_last_valid_wal_file(stb_pgdata)
    if err_code != 0:
        return err_code, last_wal_file

    # 把远程主库中WAL文件名中LSN号大于等于last_wal_file的文件都拷贝过来
    last_wal_file = last_wal_file[-24:]
    pt = last_wal_file[-16:]
    err_code, err_msg = rpc_utils.pg_get_valid_wal_list_le_pt(pri_ip, pri_pgdata, pt)
    if err_code != 0:
        return err_code, err_msg

    try:
        pri_wal_list = sorted(err_msg)
        for pri_wal_file in pri_wal_list:
            dst_wal_file = os.path.join(wal_path, pri_wal_file[-24:])
            logging.info(f"copy {pri_wal_file} from {pri_ip} to {dst_wal_file}...")
            dst_fd = os.open(dst_wal_file, os.O_RDWR | os.O_CREAT, 0o600)
            offset = 0
            while True:
                err_code, data = rpc_utils.os_read_file(pri_ip, pri_wal_file, offset, 4194304)
                if err_code != 0:
                    os.close(dst_fd)
                    return err_code, data
                if len(data) == 0:
                    break
                os.write(dst_fd, data)
                offset += len(data)
            os.close(dst_fd)
            os.chown(dst_wal_file, fs.st_uid, fs.st_gid)
            return 0, ''
    except Exception:
        err_msg = traceback.format_exc()
        return -1, err_msg
