# coding=utf-8
"""
进程工具类
@author jiangbing
"""
import subprocess
import multiprocessing


class ProcUtil:
    def __init__(self):
        pass

    def single_pro(self, cmd, succ=None, fail=None):
        """单进程"""
        pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # 进程通讯
        out = None
        errs = None
        if pro.stdout is not None:
            out = pro.stdout.read()
        if pro.stderr is not None:
            errs = pro.stderr.read()

        returncode = subprocess.Popen.poll(pro)
        if returncode is None:
            returncode = subprocess.Popen.wait(pro)
        pid = pro.pid
        # returncode非0失败
        if returncode != 0:
            if fail is not None:
                fail(pid, returncode, out, errs)
            # print("run: %s 执行失败 %s" % (pid, returncode))
        else:
            if succ is not None:
                succ(pid, returncode, out, errs)
            # print("run: %s 执行完成 %s" % (pid, returncode))
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
