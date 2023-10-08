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
@description: mount模块,管理文件系统的mount和umount操作
"""

import logging

import traceback

import run_lib


def is_mount(mount_path):
    try:
        mounted_list = []
        with open('/proc/mounts') as fp:
            lines = fp.readlines()
            for line in lines:
                line = line.strip()
                mounted_list.append(line.split()[1])
        if mount_path in mounted_list:
            return 0, True
        else:
            return 0, False
    except Exception as e:
        logging.error(f"when check is mount error: {traceback.format_exc()}.")
        return -1, str(e)


def mount(dev_path, mount_path):
    cmd = "mount %s %s " % (dev_path, mount_path)
    err_code, err_msg, _out_msg = run_lib.run_cmd_result(cmd)
    return err_code, err_msg


def umount(mount_path):
    """
    删除指定的文件
    :return:
    """
    err_code, mount_flag = is_mount(mount_path)
    if err_code != 0:
        return err_code, mount_flag
    if not mount_flag:
        return 0, "already umounted!"
    try:
        cmd = "fuser -km %s" % mount_path
        run_lib.run_cmd(cmd)
        cmd = "umount %s" % mount_path
        err_code, err_msg, _out_msg = run_lib.run_cmd_result(cmd)
        return err_code, err_msg
    except Exception as e:
        logging.error(f"umount error: {traceback.format_exc()}.")
        return -1, str(e)
