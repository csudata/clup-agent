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
@description: 一些通用的函数
"""

import os
import re
from decimal import Decimal

import psutil

import run_lib


def get_unit_size(unit_size):
    """取带K、M、G、T单位的配置项

    @return 返回数字
    """

    if len(unit_size) <= 0:
        return 0

    unit_char = unit_size[-1].lower()
    if unit_char.isdigit():
        num_size = int(unit_size)
    else:
        if unit_char == 't':
            unit_value = 1024 * 1024 * 1024 * 1024
        elif unit_char == 'g':
            unit_value = 1024 * 1024 * 1024
        elif unit_char == 'm':
            unit_value = 1024 * 1024
        elif unit_char == 'k':
            unit_value = 1024
        else:
            unit_value = 1
        num_size = int(float(unit_size[0:-1]) * unit_value)
    return num_size


def get_cpu_info():
    """
    获得CPU的信息
    :return: 总的物理核数和逻辑核数（即超线程数）
    """
    cpu_dict = {}
    core_dict = {}
    with open('/proc/cpuinfo', 'r') as fp:
        data = fp.read()
        lines = data.split('\n')
        for line in lines:
            line = line.strip()
            cells = line.split(':')
            if len(cells) != 2:
                continue
            key = cells[0].strip()
            val = cells[1].strip()

            if key == 'processor':
                core_dict = {}
                cpu_dict[val] = core_dict
                continue
            core_dict[key] = val
    return cpu_dict


def get_mem_size():
    """
    获得总内存的大小
    :return:
    """
    with open('/proc/meminfo', 'r') as fp:
        data = fp.read()
        lines = data.split('\n')
        cells = lines[0].split()
        unit_char = cells[2][:1].lower()
        if unit_char == 't':
            unit_value = 1024 * 1024 * 1024 * 1024
        elif unit_char == 'g':
            unit_value = 1024 * 1024 * 1024
        elif unit_char == 'm':
            unit_value = 1024 * 1024
        elif unit_char == 'k':
            unit_value = 1024
        else:
            unit_value = 1
        return int(cells[1]) * unit_value


def get_machine_sn():
    """通过运行dmidecode获得机器的序列号
    """

    os_cmd = "dmidecode"
    out_msg = run_lib.open_cmd(os_cmd)
    lines = out_msg.split('\n')

    find_session = False
    for line in lines:
        if line == 'System Information':
            find_session = True
        if not find_session:
            continue

        line = line.strip()

        if line[:13] == "Serial Number":
            cells = line.split(':')
            return cells[1].strip()

    return None


def get_os_type():
    """通过运行/etc/os-release获得操作系统类型
    """

    if not os.path.exists('/etc/os-release'):
        return 'unknow_os'

    os_release_dict = {}
    with open('/etc/os-release') as fp:
        lines = fp.readlines()
        for line in lines:
            line = line.strip()
            pos = line.find('=')
            if pos < 0:
                continue
            key = line[:pos]
            val = line[pos + 1:]
            if val[0] == '"' and val[-1] == '"':
                val = val[1:-1]
            os_release_dict[key] = val
    if 'ID' not in os_release_dict or 'VERSION_ID' not in os_release_dict:
        return 'unknow_os'
    return f"{os_release_dict['ID']} {os_release_dict['VERSION_ID']}"


def get_vg_dict(vg_name):
    cmd = 'vgdisplay %s' % vg_name
    err_code, _err_msg, out_msg = run_lib.run_cmd_result(cmd)
    if err_code != 0:
        return -1, out_msg
    lines = out_msg.split('\n')
    vg_dict = {}
    for line in lines:
        #   PE Size               4.00 MiB
        m = re.match(r'\s+PE Size\s+(\d+\.\d+) ([KMGkmg])iB', line)
        if m:
            if m.group(2).upper() == 'G':
                vg_dict['pe_size'] = int(Decimal(m.group(1)) * 1024 * 1024 * 1024)
            elif m.group(2).upper() == 'M':
                vg_dict['pe_size'] = int(Decimal(m.group(1)) * 1024 * 1024)
            elif m.group(2).upper() == 'K':
                vg_dict['pe_size'] = int(Decimal(m.group(1)) * 1024)
        # Total PE              12799
        m = re.match(r'\s+Total PE\s+(\d+)', line)
        if m:
            vg_dict['total_pe'] = int(m.group(1))

        # Total PE              12799
        m = re.match(r'\s+Total PE\s+(\d+)', line)
        if m:
            vg_dict['total_pe'] = int(m.group(1))
        # Free  PE / Size       12799 / <50.00 GiB
        m = re.match(r'\s+Free  PE / Size\s+(\d+) /.+', line)
        if m:
            vg_dict['free_pe'] = int(m.group(1))
    return 0, vg_dict


def get_block_dev_size(dev_path):
    """获得块设备的大小

    传入块设备的路径，返回字节数
    """

    fd = os.open(dev_path, os.O_RDONLY)
    try:
        return os.lseek(fd, 0, os.SEEK_END)
    finally:
        os.close(fd)



def check_port_used(port):
    """检查端口是否被占用
    """
    # 获得所有连接的端口
    if not isinstance(port, int):
        return -1, "port is not integer!"
    try:
        conns = psutil.net_connections()
        port_dict = {}
        for conn in conns:
            port_dict[conn.laddr.port] = 1
        if port in port_dict:
            return 0, True
        else:
            return 0, False
    except Exception as e:
        return -1, repr(e)
