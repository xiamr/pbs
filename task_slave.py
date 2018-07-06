#!/usr/bin/env python3

# scheduler slave program

import sys, os
import asyncio
import json
import socket
import signal
import ssl
import asyncio.subprocess
import pwd
from datetime import *
import shutil
import aiomas
from common import *
import logging
import logging.handlers
import argparse
import platform
import multiprocessing
import subprocess
import pty
import fcntl
import termios
from ctypes import *

libc = cdll.LoadLibrary("libc.so.6")

def posix_openpt():
    return libc.posix_openpt(c_int(0o2 | 0o400))


class NoFoundEUID(Exception): pass
class NoFoundCGroup(Exception): pass
class CGroupError(Exception): pass


cg_path = None

with open("/proc/mounts") as f:
    for line in f:
        field = line.split()
        if field[0] == "cgroup":
            if "cpu" in field[3].split(","):
                cg_path = os.path.dirname(field[1])


if cg_path is None:
    print("CGroup not started")
    exit(1)

def geteuid(pid : int) -> int:
    with open("/proc/%s/status") as f:
        for line in f:
            field = line.split()
            if field[0] == "Uid:":
                return int(field[2])

    raise NoFoundEUID("can not find euid of process : pid = %s" % pid)

def getcgcpu(pid : int) -> str:
    with open("/proc/%s/cgroup") as f:
        for line in f:
            field = line.split(":")
            if "cpu" in field[1].split(","):
                return field[2]
    errstr = "can not find cgroup of process : pid = %s" % pid
    logging.error(errstr)

def lsAllProcess():
    with open(os.path.join(cg_path,"cpu/tasks")) as f:
        for line in f:
            yield int(line)


def cgcreate(cgroup : [str,int]):
    code = subprocess.run("cgcreate -g cpu:%s" % cgroup, shell=True)
    if code.returncode != 0:
        errstr = "can not create cgroup cpu:%s" % cgroup
        logging.error(errstr)

def cgset_shareds(cgroup: str, value : int):
    code = subprocess.run("cgset -r cpu.shares=%s %s" % (value, cgroup),shell=True)
    if code.returncode != 0:
        errstr = "can not set cgroup cpu:%s" % cgroup
        logging.error(errstr)

def cgdelete(cgroup : str):
    code = subprocess.run("cgdelete cpu:%s" % cgroup, shell=True)
    if code.returncode != 0:
        errstr = "can not delete cgroup cpu:%s" % cgroup
        logging.error(errstr)


def cgclassify(cgroup : str, pid : int):
    code = subprocess.run("cgclassify -g cpu:%s %s" % (cgroup,pid), shell=True)
    if code.returncode != 0:
        errstr = "can not move pid = %s to cgroup cpu:%s" % (pid,cgroup)
        logging.error(errstr)

def move_pid_to_other(pid : int):
    logging.info("cgroute movemnt")
    if not os.path.exists(os.path.join(cg_path,"cpu/other")):
        cgcreate("other")
        cgset_shareds("other",1)
    cgclassify("other",pid)

class Job:
    def __init__(self):
        self.job_id: int = 0
        self.workdir: int = None
        self.sgefile: str = None
        self.job_name: str = None
        self.status = STATUS_QUEUE
        self.cores: str = 0
        self.cmd_obj: asyncio.Process = None
        self.owner_uid: int = None
        self.owner_gid: int = None
        self.submit_time: datetime = None
        self.start_time: datetime = None
        self.finish_time: datetime = None
        self.priority: int = None
        self.bind_node: List[str] = []
        self.queue_name: str = "main"
        self.shell: str = None
        self.env: Dict[str,str] = None
        self.run_type = BATCH

class Slave():
    router = aiomas.rpc.Service()

    def __init__(self):
        self.name: str = None
        self.running_list: List[Job] = []
        self.queue_list: List[Job] = []
        self.master_ip: str = None
        self.master_port: int = None
        self.rpc = None
        self.total_cores: int = None
        self.queue_name: str = "main"
        self.search_all_process_task = None

    def init_job_env(self, job : Job, stdout_path, stderr_path):
        job.env["ARC"] = "x86_64"
        job.env["SGE_O_HOST"] = self.name
        try:
            job.env["SGE_O_HOME"] = job.env["HOME"]
            job.env["SGE_O_LOGNAME"] = job.env["LOGNAME"]
            job.env["SGE_O_PATH"] = job.env["PATH"]
            job.env["SGE_O_SHELL"] = job.env["SHELL"]
        except:
            pass
        job.env["SGE_O_WORKDIR"] = job.workdir
        job.env["NSLOTS"] = str(job.cores)
        job.env["JOB_ID"] = str(job.job_id)
        job.env["JOB_NAME"] = job.job_name
        job.env["SGE_STDOUT_PATH"] = stdout_path
        job.env["SGE_STDERR_PATH"] = stderr_path
        job.env["ENVIRONMENT"] = job.run_type


    async def run_job(self, job: Job) -> None:
        if job not in self.queue_list: return
        logging.info(
            "Prepere to run job : job_id = %s, job_name = %s, workdir = %s , sgefile = %s, cores = %s, owner_uid = %s"
            % (job.job_id, job.job_name, job.workdir, job.sgefile, job.cores, job.owner_uid))


        stdouterr_filename = "%s.oe%s" % (job.job_name, job.job_id)

        logging.info("Start to create master pty : job_id = %s" % job.job_id)

        master_pty_fd = posix_openpt()
        libc.grantpt(master_pty_fd)
        libc.unlockpt(master_pty_fd)
        libc.ptsname.restype = c_char_p
        SlaveName =  libc.ptsname(master_pty_fd).decode()


        os.set_blocking(master_pty_fd,False)
        attr = list(job.tty_attr)
        attr[6] = list(attr[6])


        def prepare():
            os.setsid()
            os.close(master_pty_fd)
            slave_pty_fd = os.open(SlaveName,os.O_RDWR)

        #    termios.tcsetattr(slave_pty_fd, termios.TCSANOW, attr)

            print("Here")
            os.dup2(slave_pty_fd,0)
            os.dup2(slave_pty_fd,1)
            os.dup2(slave_pty_fd,2)

            cgclassify(job.job_id, os.getpid())
            if os.getuid() == 0:
                os.setgid(job.owner_gid)
                os.setuid(job.owner_uid)


            os.chdir(job.workdir)


        job.master_pty_fd = master_pty_fd
        if job.winsize:
            fcntl.ioctl(job.master_pty_fd, termios.TIOCSWINSZ, job.winsize)


      #  fcntl.ioctl(slave_pty_fd,termios.TIOCSCTTY,0)

        queue = asyncio.Queue()
        event = asyncio.Event()
        event.clear()
        prep_to_break = False

        async def send_to_master():
            while True:
                await event.wait()
                event.clear()
                if prep_to_break:
                    return
                while True:
                    try:
                        data = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    logging.info("send ptyout job_id = %s" % job.job_id)
                    job.pty_out_buffer.append(data)
                    await self.rpc.remote.RelayPtyOut(Request_Master_JobPtyOut(job.job_id, data))

        def read_pty():
            while True:
                try:
                    data = os.read(master_pty_fd,1024)
                    if len(data) != 0:
                        queue.put_nowait(data)
                        job.outerrfile.write(data)
                        event.set()
                    else:
                        return
                except:
                    return


        os.chdir(job.workdir)
        self.init_job_env(job,os.path.join(job.workdir,stdouterr_filename),
                          os.path.join(job.workdir,stdouterr_filename))

        with open(stdouterr_filename, "wb") as outerrfile:
            logging.info("Start to create subprocess : job_id = %s" % job.job_id)
            os.chown(stdouterr_filename, job.owner_uid, job.owner_gid)
            cgcreate(job.job_id)
            cgset_shareds(job.job_id, job.cores * 1024)
            process = await asyncio.create_subprocess_exec(
                job.shell, job.sgefile,
                preexec_fn=prepare,
                env=job.env)
            job.start_time = datetime.now()
            logging.info("Succeed in creating subprocess : job_id = %s, pid = %s" % (job.job_id, process.pid))
            job.cmd_obj = process
            self.queue_list.remove(job)
            self.running_list.append(job)
            job.status = STATUS_RUNNING
            asyncio.get_event_loop().add_reader(master_pty_fd,read_pty)
            self.send_task = asyncio.ensure_future(send_to_master())
            job.outerrfile = outerrfile
            try:
                await self.rpc.remote.JobStarted(
                    Request_Master_JobStarted(job.job_id, job.start_time.timestamp(), job.queue_name))
            except:
                job.err = True
            else:
                job.err = False

            logging.info("Wait subprocess to finish : job_id = %s, pid = %s" % (job.job_id, process.pid))
            await process.communicate()


        await self.clean_job(job)
        asyncio.get_event_loop().remove_reader(master_pty_fd)
        os.close(master_pty_fd)
        cgdelete(job.job_id)
        job.status = STATUS_FINISH
        job.finish_time = datetime.now()
        logging.info("Job finished : job_id = %s, returncode = %s" % (job.job_id, process.returncode))
        while True:
            try:
                if job.err: await self.rpc.remote.JobStarted(
                    Request_Master_JobStarted(job.job_id, job.start_time.timestamp(), job.queue_name))
                job.err = False
                await self.rpc.remote.JobComplete(
                    Request_Master_JobComplete(job.job_id, job.finish_time.timestamp(),job.queue_name))
            except:
                await asyncio.sleep(60)
            else:
                break

        job.cmd_obj = None
        self.running_list.remove(job)
        prep_to_break = True
        event.set()


    async def exec_job(self, job: Job) -> None:
        if job not in self.queue_list: return
        logging.info(
            "Prepere to exec job : job_id = %s, job_name = %s, workdir = %s , sgefile = %s, cores = %s, owner_uid = %s"
            % (job.job_id, job.job_name, job.workdir, job.sgefile, job.cores, job.owner_uid))

        os.chdir(job.workdir)

        def prepare():
            cgclassify(job.job_id,os.getpid())
            #os.setpgrp()
            if os.getuid() == 0:
                os.setgid(job.owner_gid)
                os.setuid(job.owner_uid)

        stdout_filename = "%s.o%s" % (job.job_name, job.job_id)
        stderr_filename = "%s.e%s" % (job.job_name, job.job_id)
        self.init_job_env(job,os.path.join(job.workdir,stdout_filename),os.path.join(job.workdir,stderr_filename))
        with open(stdout_filename, "w") as outfile, open(stderr_filename, "w") as errfile:
            os.chown(stdout_filename, job.owner_uid, job.owner_gid)
            os.chown(stderr_filename, job.owner_uid, job.owner_gid)
            logging.info("Start to create subprocess : job_id = %s" % job.job_id)
            cgcreate(job.job_id)
            cgset_shareds(job.job_id,job.cores*1024)
            process = await asyncio.create_subprocess_exec(
                job.shell,job.sgefile,
                preexec_fn=prepare,
                cwd=job.workdir,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=outfile,
                stderr=errfile,
                env=job.env)
            job.start_time = datetime.now()
            logging.info("Succeed in creating subprocess : job_id = %s, pid = %s" % (job.job_id, process.pid))
            job.cmd_obj = process
            self.queue_list.remove(job)
            self.running_list.append(job)
            job.status = STATUS_RUNNING
            logging.info("Wait subprocess to finish : job_id = %s, pid = %s" % (job.job_id, process.pid))
            try:
                await self.rpc.remote.JobStarted(
                    Request_Master_JobStarted(job.job_id, job.start_time.timestamp(), job.queue_name))
            except:
                job.err = True
            else:
                job.err = False
            await process.communicate()


        await self.clean_job(job)
        cgdelete(job.job_id)
        job.status = STATUS_FINISH
        job.finish_time = datetime.now()
        logging.info("Job finished : job_id = %s, returncode = %s" % (job.job_id, process.returncode))
        while True:
            try:
                if job.err:await self.rpc.remote.JobStarted(
                    Request_Master_JobStarted(job.job_id, job.start_time.timestamp(), job.queue_name))
                job.err = False
                await self.rpc.remote.JobComplete(
                    Request_Master_JobComplete(job.job_id, job.finish_time.timestamp(),job.queue_name))
            except:
                await asyncio.sleep(60)
            else:
                break

        job.cmd_obj = None
        self.running_list.remove(job)



    # async def search_all_process(self):
    #     while True:
    #         for pid in lsAllProcess():
    #             if getcgcpu(pid) == "/":
    #                 if geteuid(pid) != 0:
    #                     move_pid_to_other(pid)
    #
    #         await asyncio.sleep(10)

    async def run(self):
        try:
            logging.info("connect to %s:%s ..." % (self.master_ip, self.master_port))
            self.rpc = await aiomas.rpc.open_connection(
                (self.master_ip, PORT), rpc_service=self, codec = aiomas.MsgPackBlosc,
                extra_serializers=EXTRA_SERIALIZERS)
        except ConnectionRefusedError as e:
            logging.error("%s [will try again after 10s]" % e)
            await asyncio.sleep(10)
            asyncio.ensure_future(self.run())
            return

        logging.info("connection established")
        sock = self.rpc.channel.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # set TCP heartbeat
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)
        job_info = []
        for job in self.running_list:
            job_info.append(Master_SlaveRegister_Info(job.job_id,job.workdir,job.sgefile,job.job_name,
                                                      job.status,job.cores,job.owner_uid,job.owner_gid,
                                                      job.submit_time.timestamp(),job.start_time.timestamp(),
                                                      job.priority,job.bind_node, job.queue_name,
                                                      job.shell,job.env, job.run_type))
        ret = await self.rpc.remote.SlaveRegister(
            self,Request_Master_SlaveRegister(self.name, self.total_cores,self.queue_name, job_info))

        def master_disconnected(exc):
            logging.error("connection loss to %s:%s" % (self.master_ip, PORT))
            asyncio.ensure_future(self.run())

        self.rpc.on_connection_reset(master_disconnected)
  #      if self.search_all_process_task is None:
    #        self.search_all_process_task = asyncio.ensure_future(self.search_all_process())

    @aiomas.expose
    async def SubJob(self, req: Request_Slave_SubJob) -> Response_Slave_SubJob:
        logging.info(
            "new job from master: job_id = %s, job_name = %s, cores = %s" % (req.job_id, req.job_name, req.cores))
        job = Job()
        job.job_id = req.job_id
        job.workdir = req.workdir
        job.sgefile = req.sgefile
        job.job_name = req.job_name
        job.status = req.status
        job.cores = req.cores
        job.owner_uid = req.owner_uid
        job.owner_gid = req.owner_gid
        job.submit_time = datetime.fromtimestamp(req.submit_time)
        job.priority = req.priority
        job.bind_node = req.bind_node
        job.queue_name = req.queue_name
        job.shell = req.shell
        job.env = req.env
        self.queue_list.append(job)
        job.run_type = req.run_type
        if req.run_type == BATCH:
            asyncio.ensure_future(self.exec_job(job))
        else:
            job.winsize = req.winsize
            job.tty_attr = req.tty_attr
            asyncio.ensure_future(self.run_job(job))
        return Response_Slave_SubJob()

    @aiomas.expose
    async def DelJob(self, req: Request_Slave_DelJob) -> Response_Slave_DelJob:
        logging.info("job delete request from master : job_id = %s", req.job_id)
        done = False
        for job in self.queue_list:
            if job.job_id == req.job_id:
                self.queue_list.remove(job)
                logging.debug("job delete from queue_list: job_id = %s, job_name = %s" % (job.job_id, job.job_name))
                done = True
                break
        if done is False:
            for job in self.running_list:
                if job.job_id == req.job_id:
                    logging.debug("send signal to kill job: job_id = %s, job_name = %s" % (job.job_id, job.job_name))
                    asyncio.ensure_future(self.kill_job(job))
                    job.stauts = STATUS_CANCELING
                    break
        return Response_Slave_DelJob()

    @aiomas.expose
    async def pty_job_input(self, req : Request_Slave_JobInput) -> Response_Slave_JobInput:
        for job in self.running_list:
            if job.job_id == req.job_id:
                os.write(job.master_pty_fd,req.data)
                job.outerrfile.write(req.data)
                return Response_Slave_JobInput()


    @aiomas.expose
    async def pty_change_winsize(self, req : Request_Slave_WINSZ) -> Response_Slave_WINSZ:
        for job in self.running_list:
            if job.job_id == req.job_id:
                fcntl.ioctl(job.master_pty_fd,termios.TIOCSWINSZ,req.winsize)
                return Response_Slave_WINSZ()
    @aiomas.expose
    async def pty_new_signal(self, req : Request_Slave_PtySignal) -> Response_Slave_PtySignal:
        for job in self.running_list:
            if job.job_id == req.job_id:
                pass
    @aiomas.expose
    async def pty_get_out_buf_list(self, req : Request_Slave_JobPtyOut_List) -> Response_Slave_JobPtyOut_List:
        for job in self.running_list:
            if job.job_id == req.job_id:
                return Response_Slave_JobPtyOut_List(job.pty_out_buffer)
        return Response_Slave_JobPtyOut_List([])

    async def kill_job(self, job: Job):
        logging.debug("kill job of job_id = %s" %  job.job_id)
        await self.clean_job(job)


    async def clean_job(self, job: Job):
        try:
            with open(os.path.join(cg_path,"cpu/%s/tasks" % job.job_id)) as f:
                for pid in f:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                    except ProcessLookupError:
                        pass

            pp_num = 0
            with open(os.path.join(cg_path, "cpu/%s/tasks" % job.job_id)) as f:
                for pid in f:
                    pp_num += 1

            if pp_num == 0: return
            await asyncio.sleep(10)
            with open(os.path.join(cg_path, "cpu/%s/tasks" % job.job_id)) as f:
                for pid in f:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        except:
            pass


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')
  #  logging.getLogger().addHandler(logging.handlers.SysLogHandler())
    parser = argparse.ArgumentParser(description="Slave for cluster dispatch system")
    parser.add_argument("-s","--master_ip", type=str,required=True,help="Ip address of master server")
    parser.add_argument("-p","--master_port",type=check_no_negative_integer,help="port of master service")
    parser.add_argument("-n","--name", type=str, default=platform.node(), help="slave name")
    parser.add_argument("-t","--total", type=check_no_negative_integer,
                        default=multiprocessing.cpu_count(),help="total cpu cores of this node")
    parser.add_argument("-q","--queue_name",type=str, default="main", help="Queue name")
    args = parser.parse_args()
    slave = Slave()
    slave.master_ip = args.master_ip
    slave.master_port = args.master_port
    slave.name = args.name
    slave.total_cores = args.total
    slave.queue_name = args.queue_name
    asyncio.ensure_future(slave.run())
    aiomas.run()


if __name__ == "__main__":
    main()

