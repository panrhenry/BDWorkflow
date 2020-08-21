# coding=utf-8
"""
进程工具类
@author jiangbing
"""
import subprocess
import multiprocessing
import paramiko
import chardet


class ProcUtil:
    def __init__(self):
        pass

    def single_pro(self, cmd, succ=None, fail=None, before=None):
        """单进程"""
        # pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pid = pro.pid
        if before is not None:
            before(pid)
        outs, errs = pro.communicate()
        if outs is not None:
            if chardet.detect(outs)["encoding"] is not None:
                outs = outs.decode(chardet.detect(outs)["encoding"], errors='ignore')
        else:
            outs = ''
        if errs is not None:
            if chardet.detect(errs)["encoding"] is not None:
                errs = errs.decode(chardet.detect(errs)["encoding"], errors='ignore')
        else:
            errs = ''
        returncode = pro.returncode
        if returncode != 0:
            if fail is not None:
                fail(pid, returncode, outs, errs)
        else:
            if succ is not None:
                succ(pid, returncode, outs, errs)
        return returncode

    def single_pro_realtime(self, cmd, succ=None, fail=None, realtime_out=None):
        """单进程"""
        pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pid = pro.pid
        outs = ''
        errs = ''
        while True:
            out = pro.stdout.readline()
            # err = pro.stderr.readline()
            pro.stdout.flush()
            # pro.stderr.flush()
            if out is not None:
                if chardet.detect(out)["encoding"] is not None:
                    outs += out.decode(chardet.detect(out)["encoding"], errors='ignore')
            # if err is not None:
            #     if chardet.detect(err)["encoding"] is not None:
            #         errs += err.decode(chardet.detect(err)["encoding"], errors='ignore')
            if realtime_out is not None:
                realtime_out(out, '')
            returncode = subprocess.Popen.poll(pro)
            if returncode is not None:  # 判断进程是否结束
                break
        if returncode != 0:
            if fail is not None:
                fail(pid, returncode, outs, errs)
        else:
            if succ is not None:
                succ(pid, returncode, outs, errs)
        return returncode

    def multi_pro(self, list_cmd):
        """多进程"""
        pros = multiprocessing.Pool(processes=4)
        for cmd in list_cmd:
            pros.apply_async(self.single_pro, args=(cmd,))

        print('所有进程开始...')
        pros.close()
        pros.join()
        print('所有进程结束')

    def ssh_exec_cmd(self, hostname, port, username, password, cmd, succ=None, fail=None):
        """ssh远程连接"""
        try:
            ssh_fd = paramiko.SSHClient()
            ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_fd.connect(hostname, int(port), username, password)
            stdin, stdout, stderr = ssh_fd.exec_command(cmd)
            outs = stdout.read()
            errs = stderr.read()
            returncode = stdout.channel.recv_exit_status()
            if outs is not None:
                if chardet.detect(outs)["encoding"] is not None:
                    outs = outs.decode(chardet.detect(outs)["encoding"], errors='ignore')
            else:
                outs = ''
            if errs is not None:
                if chardet.detect(errs)["encoding"] is not None:
                    errs = errs.decode(chardet.detect(errs)["encoding"], errors='ignore')
            else:
                errs = ''
            if returncode == 0:
                if succ is not None:
                    succ(returncode, outs, errs)
            else:
                if fail is not None:
                    fail(returncode, outs, errs)
            ssh_fd.close()
            return returncode
        except Exception as e:
            if fail is not None:
                fail(-1, None, e)
            return -1
