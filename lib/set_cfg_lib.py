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
@description: 修改配置文件的库
"""

import os
import re
import shutil
import time


def modify_config_type1(config_file, modify_item_dict, deli_type=1, is_backup=True):
    """
    修改以等号或空格分隔的配置文件,默认为等号分隔的文件即(k = v)格式的配置文件,如postgresql.conf, /etc/sysctl.conf这些文件都是这种格式。
    :param config_file:
    :param modify_item_dict:
    :param deli_type: 1表示以等号分隔的配置文件,2表示以空格分隔的配置文件
    :param is_backup:
    :return:

    修改配置文件:
      1. 如果在文件中只有相应被注释掉的配置项,则新的配置项加在注释掉的配置项后面。
      2. 如果已存在配置项,则替换原有的配置项。
      3. 如果文件中不存在的配置项,则添加到文件尾
    例如modify_item_dict={'port':'5444'},配置文件中只存在port的注释掉的配置:
      ...
      listen_addresses = '*'
      #port = 5432                            # (change requires restart)
      max_connections = 100                   # (change requires restart)
      ...

    执行后的结果是在此被注释掉的配置项后面加上新的配置项,结果变为：
      ...
      listen_addresses = '*'
      #port = 5432                            # (change requires restart)
      port = 5444
      max_connections = 100                   # (change requires restart)
      ...

    如果配置文件中存在port的注释掉的配置项和未被注释掉的相应的配置项：
      ...
      listen_addresses = '*'
      #port = 5432                            # (change requires restart)
      port = 5433
      max_connections = 100                   # (change requires restart)
      ...

    执行后的结果是在此被注释掉的配置项后面加上新的配置项,结果变为：
      ...
      listen_addresses = '*'
      #port = 5432                            # (change requires restart)
      port = 5444
      max_connections = 100                   # (change requires restart)
      ...

    :param config_file:
    :param modify_item_dict:
    :return:
    """

    fp = open(config_file)
    ori_lines = fp.readlines()
    fp.close()

    # 下面的操作先找各个配置项的位置
    # item_line_num_dict1和item_line_num_dict2分别记录相应的配置项在文件中的行号。
    #   只是item_line_num_dict1字典中key是行号,而value是相应的配置项名称
    #   而item_line_num_dict2字典中key是配置项名称,而value是相应的行号
    item_line_num_dict1 = {}
    item_line_num_dict2 = {}

    # item_comment_line_num_dict1和item_comment_line_num_dict2分别记录配置文件中被注释掉的配置项在文件中的行号。
    item_comment_line_num_dict1 = {}
    item_comment_line_num_dict2 = {}

    i = 0
    for line in ori_lines:
        line = line.strip()
        if deli_type == 1:
            cells = line.split('=')
        else:
            cells = line.split()
        if len(cells) < 2:
            i += 1
            continue
        item_name = cells[0].strip()
        if item_name[0] == '#':
            if item_name[1:].strip() in modify_item_dict:
                item_comment_line_num_dict1[i] = item_name[1:].strip()
                item_comment_line_num_dict2[item_name[1:].strip()] = i
        if item_name in modify_item_dict:
            item_line_num_dict1[i] = item_name.strip()
            item_line_num_dict2[item_name.strip()] = i
        i += 1

    # 如果已存在相应的配置项,即使也存在注释掉的配置项,则就不能在已注释掉的配置项后再加上新配置项了,需要替换掉的已存在的配置项
    for item_name in item_comment_line_num_dict2:
        if item_name in item_line_num_dict2:
            i = item_comment_line_num_dict2[item_name]
            del item_comment_line_num_dict1[i]

    # 如果配置项在item_line_num_dict1中存在或在item_comment_line_num_dict1,则添加新配置项
    i = 0
    new_lines = []
    for line in ori_lines:
        line = line.strip()
        if i in item_line_num_dict1:
            if deli_type == 1:
                new_line = "%s = %s" % (item_line_num_dict1[i], modify_item_dict[item_line_num_dict1[i]])
            else:
                new_line = "%s %s" % (item_line_num_dict1[i], modify_item_dict[item_line_num_dict1[i]])
            new_lines.append(new_line)
        elif i in item_comment_line_num_dict1:
            # 如新行加到注释行的下一行处
            new_lines.append(line)
            item_name = item_comment_line_num_dict1[i]
            if deli_type == 1:
                new_line = "%s = %s" % (item_name, modify_item_dict[item_name])
            else:
                new_line = "%s %s" % (item_name, modify_item_dict[item_name])
            new_lines.append(new_line)
        else:
            new_lines.append(line)
        i += 1

    # 把配置文件中不存在的配置项,添加到文件尾（按item_name排序）
    items = sorted(modify_item_dict.keys())
    for item_name in items:
        if item_name not in item_line_num_dict2 and item_name not in item_comment_line_num_dict2:
            if deli_type == 1:
                new_line = "%s = %s" % (item_name, modify_item_dict[item_name])
            else:
                new_line = "%s %s" % (item_name, modify_item_dict[item_name])
            new_lines.append(new_line)

    if is_backup:
        config_file_bak = "%s.%s" % (config_file, time.strftime('%Y%m%d%H%M%S'))
        shutil.copy(config_file, config_file_bak)
        # 把备份的文件的属主改成原先文件的属主,mode也修改成原先的文件一样
        st = os.stat(config_file)
        os.chown(config_file_bak, st.st_uid, st.st_gid)
        os.chmod(config_file_bak, st.st_mode)

    new_lines.append("")
    fp = open(config_file, 'w')
    content = '\n'.join(new_lines)
    fp.write(content)
    fp.close()


def modify_config_type2(config_file, modify_item_dict, is_backup=True, append_if_not=False):
    """
    通过正则表达式匹配进行更新配置文件,如果匹配了,则替换,如果没有匹配上,会跳过。
    如果key在整个文件中都没有match中,同时append_if_not设置为真,则会在末尾添加。
    例子：
    ci_dict = {"^\*\s+soft\s+nproc\s+\d+$": "*          soft    nproc     131072"}
    modify_config_type2(nproc_conf, ci_dict)

    :param config_file:
    :param modify_item_dict: 这是一个字典,k为一个正则表达式,v为要替换的行
    :param is_backup:
    :return:
    """

    fp = open(config_file)
    ori_lines = fp.readlines()
    fp.close()

    matched_keys = {}
    new_lines = []
    for line in ori_lines:
        line = line.strip()
        matched = False
        for k in modify_item_dict:
            if re.match(k, line):
                new_lines.append(modify_item_dict[k])
                matched = True
                matched_keys[k] = 1
                break

        if not matched:
            new_lines.append(line)

    if is_backup:
        config_file_bak = "%s.%s" % (config_file, time.strftime('%Y%m%d%H%M%S'))
        shutil.copy(config_file, config_file_bak)
        # 把备份的文件的属主改成原先文件的属主
        st = os.stat(config_file)
        os.chown(config_file_bak, st.st_uid, st.st_gid)
        os.chmod(config_file_bak, st.st_mode)

    if append_if_not:  # 如果append_if_not为值,则把没有match到的行加到末尾
        for k in modify_item_dict:
            if k not in matched_keys:
                new_lines.append(modify_item_dict[k])

    new_lines.append("")
    fp = open(config_file, 'w')
    content = '\n'.join(new_lines)
    fp.write(content)
    fp.close()


def config_file_set_tag_content(file_name, tag_line, set_contents):
    """
    修改配置文件中由指定tag标记的内容,如果没有tag,则把内容添加到文件末尾
    example:
    set_contents = "* soft nofile 65536\n"\
                      "* hard nofile 65536\n"\
                      "* soft nproc 131072\n"\
                      "* hard nproc 131072"
    config_file_set_tag_content("/etc/security/limits.conf", "# Add by clup", set_contents)

    :param file_name:
    :param tag_line: 这是一个标识字符串,用于标识要替换的内容
    :param set_contents: 要设置的内容
    :return:
    """

    begin_tag_line = f"{tag_line} **do not modify** begin"
    end_tag_line = f"{tag_line} **do not modify** end"

    with open(file_name) as fp:
        content = fp.read()
        fp.close()
    lines = content.split('\n')
    tag_is_begin = False
    tag_is_end = False
    head_lines = []
    tail_lines = []
    for line in lines:
        if line == begin_tag_line:
            tag_is_begin = True
            continue
        if line == end_tag_line:
            tag_is_end = True
            continue

        if not tag_is_begin:
            head_lines.append(line)
        if tag_is_begin and tag_is_end:
            tail_lines.append(line)

    file_contents = '\n'.join(head_lines) + '\n'
    file_contents += begin_tag_line + '\n'
    file_contents += set_contents + '\n'
    file_contents += end_tag_line + '\n'
    if tail_lines:
        file_contents += '\n'.join(tail_lines) + '\n'
    with open(file_name, "w") as fp:
        fp.write(file_contents)


def config_file_set_tag_in_head(file_name, tag_line, set_contents):
    """
    修改配置文件中由指定tag标记的内容,如果没有tag,则把内容添加到文件头部
    example:
    set_contents = "* soft nofile 65536\n"\
                      "* hard nofile 65536\n"\
                      "* soft nproc 131072\n"\
                      "* hard nproc 131072"
    config_file_set_tag_content("/home/postgres/.bashrc", "# Add by clup", set_contents)

    :param file_name:
    :param tag_line: 这是一个标识字符串,用于标识要替换的内容
    :param set_contents: 要设置的内容
    :return:
    """

    begin_tag_line = f"{tag_line} **do not modify** begin"
    end_tag_line = f"{tag_line} **do not modify** end"

    with open(file_name) as fp:
        content = fp.read()
        fp.close()
    lines = content.split('\n')
    tag_is_begin = False
    tag_is_end = False
    head_lines = []
    tail_lines = []
    for line in lines:
        if line == begin_tag_line:
            tag_is_begin = True
            continue
        if line == end_tag_line:
            tag_is_end = True
            continue

        if not tag_is_begin:
            head_lines.append(line)
        if tag_is_begin and tag_is_end:
            tail_lines.append(line)

    if (not tag_is_begin) and (not tag_is_end):
        file_contents = begin_tag_line + '\n'
        file_contents += set_contents + '\n'
        file_contents += end_tag_line + '\n'
        file_contents += '\n'.join(head_lines)
    else:
        file_contents = ''
        if head_lines:
            file_contents += '\n'.join(head_lines) + '\n'
        file_contents += begin_tag_line + '\n'
        file_contents += set_contents + '\n'
        file_contents += end_tag_line + '\n'
        if tail_lines:
            file_contents += '\n'.join(tail_lines)
    with open(file_name, "w") as fp:
        fp.write(file_contents)


def read_config_items(config_file, read_item_list, read_all=False):
    """
    读取以等号或空格分隔的配置文件,默认为等号分隔的文件即（k = v）格式的配置文件,如postgresql.conf, /etc/sysctl.conf这些文件都是这种格式。
    :param config_file:
    :param read_item_list:
    :param read_all:  读取已启用所有配置
    :return:
    """
    try:
        fp = open(config_file)
        ori_lines = fp.readlines()
        fp.close()
        item_dict = {}

        i = 0
        for line in ori_lines:
            line = line.strip()
            if line[:8] == 'include ':
                include_file = line[8:].strip()
                if include_file[0] == "'":
                    include_file = include_file[1:]
                if include_file[-1] == "'":
                    include_file = include_file[:-1]
                if include_file[0] == '/':
                    full_path_include_file = include_file
                else:
                    full_path_include_file = os.path.join(os.path.dirname(config_file), include_file)
                err_code, err_msg = read_config_items(full_path_include_file, read_item_list, read_all)
                if err_code != 0:
                    return err_code, err_msg
                child_item_dict = err_msg
                item_dict.update(child_item_dict)
                continue

            cells = line.split('=', 1)
            if len(cells) < 2:
                i += 1
                continue
            item_name = cells[0].strip()
            if item_name[0] == '#':
                name = item_name[1:].strip()
                if name in item_dict.keys():
                    continue
                if name in read_item_list:
                    # 如果是注释掉的配置项就把值给空字符串""
                    item_dict[name] = ''
                    continue
            if item_name in read_item_list or read_all:
                val = cells[1].split('#')[0].strip()
                item_dict[item_name] = val
            i += 1
    except Exception as e:
        return -1, str(e)

    return 0, item_dict

