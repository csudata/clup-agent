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
@description: RPC服务模块
"""

import glob
import grp
import logging
import os
import pathlib
import pwd
import re
import shutil
import signal
import tarfile
import tempfile

import config
import cross_host_pipe
import csu_file_trans
import csuapp  # pylint: disable=import-error
import csurpc  # pylint: disable=import-error
import ip_lib
import long_term_cmd
import mount_lib
import pg_mgr
import psutil
import run_lib
import set_cfg_lib
import utils
import version

# import traceback


def merge_handler(all_handler, prefix, handler):
    attr_name_list = dir(handler)
    for attr_name in attr_name_list:
        if attr_name[0] == '_':
            continue
        attr = getattr(handler, attr_name)
        if callable(attr):
            new_attr_name = "%s_%s" % (prefix, attr_name)
            setattr(all_handler, new_attr_name, attr)
    return all_handler


def get_tmp_dir():
    src_dir = (pathlib.Path(os.getenv('PWD')).parent).as_posix()
    tmp_dir = os.path.join(src_dir, 'temp')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    return tmp_dir


class ServiceHandle:
    def __init__(self):
        pass

    @staticmethod
    def copy_file(src_file, dst_file):
        """
        拷贝文件接品
        :return:
        """
        try:
            shutil.copy(src_file, dst_file)
        except Exception as e:
            return -1, str(e)
        return 0, ''

    @staticmethod
    def delete_file(file_path):
        """
        删除指定的文件或目录,
        如果file_path是一个链接文件（不管指向文件还是目录）,都是删除则链接文件本身。
        :return:
        """
        if os.path.islink(file_path):
            is_exists = os.path.lexists(file_path)
        else:
            is_exists = os.path.exists(file_path)

        if is_exists:
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                return 0, ''
            except Exception as e:
                return -1, str(e)
        else:
            err_msg = 'file not exists'
            return 1, err_msg

    @staticmethod
    def change_file_name(old_file, new_file):
        """
        删除指定的文件
        :return:
        """
        if os.path.exists(old_file):
            cmd = 'mv %s %s' % (old_file, new_file)
            err_code, err_msg, _out_msg = run_lib.run_cmd_result(cmd)
            if err_code != 0:
                return -1, err_msg
            return 0, ''
        else:
            return 1, err_msg

    @staticmethod
    def os_path_exists(file_path):
        """
        查看指定路径或文件是否存在
        :return:
        """
        if os.path.exists(file_path):
            return True
        else:
            return False

    @staticmethod
    def get_file_size(file_path):
        """
        获得文件的大小
        :return:
        """
        if not os.path.exists(file_path):
            return -1
        else:
            try:
                with open(file_path, 'rb') as fp:
                    size = fp.seek(0, 2)
                    return size
            except Exception:
                return -1


    @staticmethod
    def os_read_file(file_path, offset, read_len):
        """
        读取除指定的文件
        :return:
        """
        fd = -1
        try:
            fd = os.open(file_path, os.O_RDONLY)
            os.lseek(fd, offset, os.SEEK_SET)
            data = os.read(fd, read_len)
            return 0, data
        except Exception as e:
            return -1, str(e)
        finally:
            if fd != -1:
                os.close(fd)

    @staticmethod
    def os_write_file(file_path, offset, data):
        """
        写入指定的文件
        :return:
        """
        fd = -1
        try:
            fd = os.open(file_path, os.O_RDWR | os.O_CREAT, 0o644)
            os.lseek(fd, offset, os.SEEK_SET)
            os.write(fd, data)
            return 0, ''
        except Exception as e:
            return -1, str(e)
        finally:
            if fd != -1:
                os.close(fd)


    @staticmethod
    def path_is_dir(dir_path):
        """
        判断是否是目录
        :return:
        """
        if os.path.isdir(dir_path):
            return True
        else:
            return False


    @staticmethod
    def dir_is_empty(dir_path):
        """
        判断是目录是否为空
        :return:
        """
        if not os.listdir(dir_path):
            return True
        else:
            return False


    @staticmethod
    def os_listdir(dir_path):
        """
        判断是目录是否为空
        :return:
        """
        try:
            fn_list = os.listdir(dir_path)
        except FileNotFoundError:
            fn_list = None
        return fn_list


    @staticmethod
    def os_user_exists(os_user):
        """
        判断是用户是否存在
        :return:
        """
        try:
            user_info = pwd.getpwnam(os_user)
            return user_info.pw_uid
        except KeyError:
            return 0


    @staticmethod
    def run_cmd(cmd):
        err_code = run_lib.run_cmd(cmd)
        return err_code


    @staticmethod
    def run_cmd_result(cmd):
        """
        拷贝文件接品
        :return:
        """
        err_code, err_msg, out_msg = run_lib.run_cmd_result(cmd)
        return err_code, err_msg, out_msg


    @staticmethod
    def mount_dev(dev_path, mount_path):
        """
        删除指定的文件
        :return:
        """
        err_code, err_msg = mount_lib.mount(dev_path, mount_path)
        return err_code, err_msg

    @staticmethod
    def umount_dev(mount_path):
        """
        删除指定的文件
        :return:
        """
        err_code, err_msg = mount_lib.umount(mount_path)
        return err_code, err_msg

    @staticmethod
    def check_is_mount(mount_path):
        """
        删除指定的文件
        :return:
        """
        err_code, err_msg = mount_lib.is_mount(mount_path)
        return err_code, err_msg

    @staticmethod
    def check_and_mount(dev_path, mount_path):
        """
        删除指定的文件
        :return:
        """
        err_code, ret = mount_lib.is_mount(mount_path)
        if err_code != 0:
            return err_code, ret
        if ret:
            return 0, ''
        err_code, err_msg = mount_lib.mount(dev_path, mount_path)
        return err_code, err_msg

    @staticmethod
    def vip_exists(vip):
        """
        拷贝文件接品
        :return:
        """
        ret = ip_lib.vip_exists(vip)
        return 0, ret

    @staticmethod
    def check_and_add_vip(vip):
        """
        拷贝文件接品
        :return:
        """
        err_code = ip_lib.check_and_add_vip(vip)
        return err_code, ""

    @staticmethod
    def check_and_del_vip(vip):
        """
        检查vip是否存在,存在则删除
        :return:
        """
        _vip_nic = ip_lib.check_and_del_vip(vip)
        return 0, ""


    @staticmethod
    def pg_get_last_valid_wal_file(pgdata):
        err_code, err_msg = pg_mgr.get_last_valid_wal_file(pgdata)
        return err_code, err_msg

    @staticmethod
    def pg_get_valid_wal_list_le_pt(pgdata, pt):
        err_code, err_msg = pg_mgr.get_valid_wal_list_le_pt(pgdata, pt)
        return err_code, err_msg

    @staticmethod
    def pg_cp_delay_wal_from_pri(pri_ip, pri_pgdata, stb_pgdata):
        err_code, err_msg = pg_mgr.cp_delayed_wal_from_pri(pri_ip, pri_pgdata, stb_pgdata)
        return err_code, err_msg

    # @staticmethod
    # def set_vote_disk(vote_disk):
    #     config.set('vote_disk', vote_disk)
    #     return 0, ''

    @staticmethod
    def check_os_env():
        """
        删除指定的文件
        :return:
        """
        err_result = []
        if not os.path.exists('/usr/sbin/ip') and not os.path.exists('/sbin/ip'):
            err_result.append(["找不到/usr/sbin/ip 或 /sbin/ip", "请安装iproute包"])
        if not os.path.exists('/usr/sbin/arping'):
            err_result.append(["找不到/usr/sbin/arping", "请安装iputils包"])
        if not os.path.exists('/usr/sbin/fuser') and not os.path.exists('/sbin/fuser'):
            err_result.append(["找不到/usr/sbin/fuser 或 /sbin/fuser", "请安装psmisc包"])
        return 0, err_result

    @staticmethod
    def get_data_disk_use(directory):
        cmd = f"df {directory}"
        err_code, err_msg, out_msg = run_lib.run_cmd_result(cmd)
        return err_code, err_msg, out_msg


    @staticmethod
    def modify_hba_conf(_user, pgdata, repl_user, subnet_range):
        conf_file = '%s/pg_hba.conf' % pgdata
        conf = "host  replication   %s   %s   trust" % (repl_user, subnet_range)
        with open(conf_file, 'r') as r_f:
            content = r_f.read()
        if conf in content:
            return 0, '', ''
        cmd = "echo '%s' >> %s" % (conf, conf_file)
        return run_lib.run_cmd_result(cmd)


    @staticmethod
    def modify_standby_delay(pdict):
        """
        更改备库延迟
        :param pdict:
        pgdata
        db_user
        delay
        :return:
        """
        file = '%s/recovery.conf' % pdict['pgdata']
        conf = "\nrecovery_min_apply_delay = '%s'" % pdict['delay']
        with open(file, 'r') as fr:
            content = fr.read()
        pattern = re.compile("\nrecovery_min_apply_delay = '.*?'")
        ret = pattern.findall(content)
        if ret:
            new_content = content.replace(ret[0], conf)
            cmd = """echo "%s" > %s && su - %s -c 'pg_ctl restart -D %s'""" % (new_content, file, pdict['db_user'],
                                                                               pdict['pgdata'])
        else:
            cmd = """echo "%s" >> %s && su - %s -c 'pg_ctl restart -D %s' """ % (conf, file, pdict['db_user'],
                                                                                 pdict['pgdata'])

        err_code, err_msg, out_msg = run_lib.run_cmd_result(cmd)
        if err_code != 0:
            return err_code, err_msg, out_msg
        return 0, '', ''


    @staticmethod
    def receive_file(file_name, content):
        """
        file_name: 文件绝对路径
        content: 文件内容
        """
        file_path = os.path.dirname(file_name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        try:
            with open(file_name, 'wb') as fw:
                fw.write(content)
        except Exception as e:
            return -1, '文件(%s)接收失败: %s' % (file_name, repr(e))
        return 0, 'ok'

    @staticmethod
    def restart_agent():
        """
        重启agent
        """
        if os.path.exists('/usr/bin/systemctl'):
            cmd = " systemctl restart clup-agent"
        else:
            # 这是没有systemctl的操作系统,如centos6、alpine linux等等
            cmd = " service clup-agent restart"
        err_code, err_msg, _out_msg = run_lib.run_cmd_result(cmd)
        return err_code, err_msg

    @staticmethod
    def extract_file(file_name, tar_dir):
        """
        解压tar包,
        file_name: 文件绝对路径
        tar_dir: 解压目录
        """
        if not tarfile.is_tarfile(file_name):
            return -1, '%s is not a tar file' % file_name

        if not os.path.exists(tar_dir):
            os.makedirs(tar_dir)
        # t = tarfile.open(file_name, 'r:')
        # try:
        #     t.extractall(path = '/path/to/extractdir/')
        #     t.close()
        # except Exception as e:
        #     return -1, 'extract file failed: %s' % repr(e)
        # finally:
        #     t.close()
        cmd = "tar -xvf %s -C %s" % (file_name, tar_dir)
        run_lib.run_cmd_result(cmd)
        return 0, ''

    @staticmethod
    def get_agent_version():
        return version.get_version()

    @staticmethod
    def chp_create_pipe_out_cmd(cmd_dict):
        return cross_host_pipe.create_pipe_out_cmd(cmd_dict)

    @staticmethod
    def chp_remove_pipe_out_cmd(cmd_id):
        return cross_host_pipe.remove_pipe_out_cmd(cmd_id)

    @staticmethod
    def chp_send_pipe_out_data(cmd_id, req, data):
        return cross_host_pipe.recv_pipe_out_data(cmd_id, req, data)


    @staticmethod
    def create_chp(local_cmd, remote_host, remote_cmd):
        return cross_host_pipe.create_chp(local_cmd, remote_host, remote_cmd)

    @staticmethod
    def remove_chp(cmd_id):
        return cross_host_pipe.remove_chp(cmd_id)


    @staticmethod
    def get_chp_state(cmd_id):
        return cross_host_pipe.get_chp_state(cmd_id)


    @staticmethod
    def create_cft(src_dir, dst_host, dst_dir, task_id=None, big_file_size=768 * 1024, trans_block_size=512 * 1024):
        return csu_file_trans.create_cft(src_dir, dst_host, dst_dir, task_id, big_file_size, trans_block_size)

    @staticmethod
    def get_cft_state(cft_id):
        return csu_file_trans.get_cft_state(cft_id)

    @staticmethod
    def remove_cft(cft_id):
        return csu_file_trans.remove_cft(cft_id)

    @staticmethod
    def cft_batch_cmd(req_list):
        return csu_file_trans.cft_batch_cmd(req_list)

    @staticmethod
    def set_file_attr(file_path, attr):
        return csu_file_trans.set_file_attr(file_path, attr)

    @staticmethod
    def check_port_used(port):
        """
        检查端口是否已经被使用
        """
        return utils.check_port_used(port)

    @staticmethod
    def os_uid_exists(uid):
        """
        检查端口是否已经被使用
        """

        try:
            pwd.getpwuid(uid)
            return 1
        except KeyError:
            return 0

    @staticmethod
    def get_pg_bin_path_list(pg_bin_path_string_list):
        """ 获得pg软件的目录列表

        Args:
            pg_bin_path_string_list ([type]): [description]
        """
        check_path_list = []
        try:
            for path_string in pg_bin_path_string_list:
                bin_path_list = glob.glob(path_string)
                check_path_list += bin_path_list
            # 去重
            check_path_list = list(set(check_path_list))
            ret_path_list = []
            for k in check_path_list:
                if os.path.exists(os.path.join(k, 'postgres')) and os.path.exists(os.path.join(k, 'initdb')):
                    ret_path_list.append(k)
            return 0, ret_path_list
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def read_config_file_items(config_file, read_item_list, read_all=False):
        err_code, ret_dict = set_cfg_lib.read_config_items(config_file, read_item_list, read_all=read_all)
        return err_code, ret_dict


    @staticmethod
    def config_file_set_tag_content(config_file, tag_line, content):
        set_cfg_lib.config_file_set_tag_content(config_file, tag_line, content)


    @staticmethod
    def config_file_set_tag_in_head(config_file, tag_line, content):
        set_cfg_lib.config_file_set_tag_in_head(config_file, tag_line, content)


    @staticmethod
    def modify_config_type1(config_file, modify_item_dict, deli_type=1, is_backup=True):
        set_cfg_lib.modify_config_type1(config_file, modify_item_dict, deli_type, is_backup)


    @staticmethod
    def modify_config_type2(config_file, modify_item_dict, is_backup=True, append_if_not=False):
        set_cfg_lib.modify_config_type2(config_file, modify_item_dict, is_backup, append_if_not)


    @staticmethod
    def file_read(file_path, mode='r'):
        """
        读取整个文件的内容
        :return:
        """
        try:
            with open(file_path, mode) as fp:
                data = fp.read()
            return 0, data
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def file_write(file_path, data, mode='w'):
        """
        写内容到指定的文件
        :return:
        """
        try:
            with open(file_path, mode) as fp:
                fp.write(data)
            return 0, ''
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def pwd_getpwnam(os_user):
        try:
            pwd_entry = pwd.getpwnam(os_user)
            # pw_name='postgres', pw_passwd='x', pw_uid=701, pw_gid=701, pw_gecos='', pw_dir='/home/postgres', pw_shell='/bin/bash'
            ret_dict = {
                "pw_name": pwd_entry.pw_name,
                "pw_passwd": pwd_entry.pw_passwd,
                "pw_uid": pwd_entry.pw_uid,
                "pw_gid": pwd_entry.pw_gid,
                "pw_gecos": pwd_entry.pw_gecos,
                "pw_dir": pwd_entry.pw_dir,
                "pw_shell": pwd_entry.pw_shell
            }
            return 0, ret_dict
        except KeyError:
            return 1, f"user {os_user} not exists!"
            # os_user_exists = False
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def pwd_getpwuid(os_uid):
        try:
            pwd_entry = pwd.getpwuid(os_uid)
            # pw_name='postgres', pw_passwd='x', pw_uid=701, pw_gid=701, pw_gecos='', pw_dir='/home/postgres', pw_shell='/bin/bash'
            ret_dict = {
                "pw_name": pwd_entry.pw_name,
                "pw_passwd": pwd_entry.pw_passwd,
                "pw_uid": pwd_entry.pw_uid,
                "pw_gid": pwd_entry.pw_gid,
                "pw_gecos": pwd_entry.pw_gecos,
                "pw_dir": pwd_entry.pw_dir,
                "pw_shell": pwd_entry.pw_shell
            }
            return 0, ret_dict
        except KeyError:
            return 1, f"user(uid={os_uid}) not exists!"
        except Exception as e:
            return -1, str(e)

    @staticmethod
    def grp_getgrall():
        try:
            grp_list = grp.getgrall()
            ret_list = []
            for g in grp_list:
                gr_dict = {
                    "gr_name": g.gr_name,
                    "gr_passwd": g.gr_passwd,
                    "gr_gid": g.gr_gid,
                    "gr_mem": g.gr_mem,
                }
                ret_list.append(gr_dict)
            return 0, ret_list
        except Exception as e:
            return -1, str(e)



    @staticmethod
    def os_rename(src, dst):
        try:
            _st = os.rename(src, dst)
            return 0, ''
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def os_stat(path, follow_symlinks=True):
        try:
            st = os.stat(path, follow_symlinks=follow_symlinks)
            # os.stat_result(st_mode=17407, st_ino=75, st_dev=2050, st_nlink=16, st_uid=0, st_gid=0, st_size=16384,
            # st_atime=1640496216, st_mtime=1640921401, st_ctime=1640921401)
            st_dict = {
                "st_mode": st.st_mode,
                "st_ino": st.st_ino,
                "st_dev": st.st_dev,
                "st_rdev": st.st_rdev,
                "st_nlink": st.st_nlink,
                "st_uid": st.st_uid,
                "st_gid": st.st_gid,
                "st_size": st.st_size,
                "st_atime": st.st_atime,
                "st_mtime": st.st_mtime,
                "st_ctime": st.st_ctime,
                "st_blksize": st.st_blksize,
                "st_blocks": st.st_blocks
            }
            return 0, st_dict
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def os_chown(file_path, os_uid, os_gid, follow_symlinks=True):
        try:
            os.chown(file_path, os_uid, os_gid, follow_symlinks=follow_symlinks)
            return 0, ''
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def os_chmod(path, mode, follow_symlinks=True):
        try:
            os.chmod(path, mode, follow_symlinks=follow_symlinks)
            return 0, ''
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def os_makedirs(name, mode=511, exist_ok=False):
        try:
            os.makedirs(name, mode, exist_ok=exist_ok)
            return 0, ''
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def os_readlink(path):
        try:
            real_path = os.readlink(path)
            return 0, real_path
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def os_real_path(path):
        try:
            real_path = os.path.realpath(path)
            return 0, real_path
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def append_file(file_path, content):
        """
        往指定的文件中添加内容
        :return:
        """
        try:
            with open(file_path, "a") as fp:
                fp.write(content)
            return 0, ''
        except Exception as e:
            return -1, repr(e)

    @staticmethod
    def mktemp(dir):
        try:
            tmp_dir = tempfile.mktemp(dir)
            return 0, tmp_dir
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def run_long_term_cmd(cmd, output_qsize=10, output_timeout=600):
        cmd_id = long_term_cmd.run_long_term_cmd(cmd, output_qsize=output_qsize, output_timeout=output_timeout)
        return cmd_id


    @staticmethod
    def get_long_term_cmd_state(cmd_id):
        state, err_code, err_msg, stdout_lines, stderr_lines = long_term_cmd.get_long_term_cmd_state(cmd_id)
        return state, err_code, err_msg, stdout_lines, stderr_lines


    @staticmethod
    def remove_long_term_cmd(cmd_id):
        err_code, err_msg = long_term_cmd.remove_long_term_cmd(cmd_id)
        return err_code, err_msg


    @staticmethod
    def terminate_long_term_cmd(cmd_id):
        err_code, err_msg = long_term_cmd.terminate_long_term_cmd(cmd_id)
        return err_code, err_msg


    @staticmethod
    def get_log_level(logger_name):
        try:
            tmp_logger = logging.getLogger(logger_name)
            return 0, tmp_logger.level
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def set_log_level(logger_name, level):
        try:
            tmp_logger = logging.getLogger(logger_name)
            tmp_logger.setLevel(level)
            return 0, ''
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def os_kill(pid, signal=signal.SIGTERM):
        try:
            os.kill(pid, signal)
            return 0, ''
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def get_child_pid_list(pid):
        try:
            p = psutil.Process(pid)
            child_obj_list = p.children()
            ret_list = [child.pid for child in child_obj_list]
            return 0, ret_list
        except Exception as e:
            return -1, str(e)


    @staticmethod
    def send_to_exec(cmd, stdin_data):
        """[summary]

        Args:
            cmd ([type]): [description]
        """
        err_code, err_msg, out_msg = run_lib.send_to_exec(cmd, stdin_data)
        return err_code, err_msg, out_msg


def run_service():
    try:
        agent_rpc_port = config.get('agent_rpc_port')
        all_handler = ServiceHandle()
        # all_handler = merge_handler(all_handler, 'cvault', handler_cvault)

        srv = csurpc.Server('dbagent-service', all_handler, csuapp.is_exit,
                            password=config.get('internal_rpc_pass'),
                            thread_count=10, debug=1)
        agent_rpc_address = "tcp://0.0.0.0:%s" % agent_rpc_port
        logging.info(f"clup-agent listen in {agent_rpc_address}.")
        srv.bind(agent_rpc_address)
        srv.run()
    except Exception as e:
        logging.error(f"rpc service stopped with unexpected error,{str(e)}.")
