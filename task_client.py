#!/usr/bin/env python3

import signal

def handler(signum, frame):
    exit(0)

# handle Ctrl-C keyboard interrupt
signal.signal(signal.SIGINT, handler)

import sys
import os
import asyncio
import json
import argparse
import ssl
import pwd
import os.path
from datetime import *
from common import *
import aiomas
import atexit
import fcntl
import termios
import os.path
import tty
import socket
import math

class Client:
    router = aiomas.rpc.Service()

    def __init__(self):
        self.username = pwd.getpwuid(os.getuid())[0]
        with open(os.path.join(KEY_PATH,"%s.json" %  self.username)) as f:
            key = json.load(f)
            self.mykey = key["key"]

        self.read_task = None
        self.prev_data = b''

    async def run(self):
        self.rpc = await aiomas.rpc.open_connection(("127.0.0.1", PORT),
                                                    rpc_service=self, codec = aiomas.MsgPackBlosc,
                                                    extra_serializers=EXTRA_SERIALIZERS)
        sock = self.rpc.channel.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # set TCP heartbeat
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)

        if args.command_type == "sub":
            await self.SubJob()
        elif args.command_type == "run":
            await self.RunJob()
            return
        elif args.command_type == "attach":
            await self.Attach()
        elif args.command_type == "status":
            await self.Status()
        elif args.command_type == "del":
            await self.DelJob()
        elif args.command_type == "cluster":
            await self.Cluster()
        elif args.command_type == "queue":
            await self.QueueList()
        elif args.command_type == "history":
            await self.History()
        elif args.command_type == "hours":
            await self.MachineHours()
        elif args.command_type == "userlimit":
            await self.UserLimit()
        elif args.command_type == "alter_cluster":
            await self.AlterCluster()
        elif args.command_type == "alter_userlimit":
            await self.AlterUserLimit()
        elif args.command_type == "change_priority":
            await self.AlterPriority()
        elif args.command_type == "loadkey":
            await self.ReloadKey()
        elif args.command_type == "alter_bind_node":
            await self.AlterBindNode()
        elif args.command_type == "statc":
            await self.Status()
            await self.Cluster()
        elif args.command_type == "new_queue":
            await self.NewQueue()
        elif args.command_type == "del_queue":
            await self.DelQueue()
        elif args.command_type == "close_queue":
            await self.CloseQueue()

        asyncio.get_event_loop().stop()

    async def SubJob(self):
        for sge_filename in args.sge_file:
            if not os.path.isfile(sge_filename):
                print("The sge file not exist!")
                return 1
            if sge_filename[-4:] != ".sge" and sge_filename[-3:] != ".sh":
                print("The suffix is wrong!")
                return 2

            job_name = None
            cores = None
            env = None
            shell = 'bash'
            with open(sge_filename) as f:
                for line in f:
                    if line[0:3] == "#$ ":
                        field = line.split()
                        if field[1] == "-N":
                            job_name = field[2]
                        elif field[1] == "-pe":
                            cores = int(field[3])
                        elif field[1] == "-V":
                            env = dict(os.environ)
                        elif field[1] == "-S":
                            shell = field[2]
            if job_name is None or cores is None:
                print("The sge syntax is wrong")
                return 3
            bind_node = []
            if args.bind_node is not None:
                bind_node = args.bind_node.split(",")


            ret = await self.rpc.remote.SubJob(
                Request_Master_SubJob(os.path.join(os.getcwd(),os.path.dirname(sge_filename)),
                                      os.path.basename(sge_filename),
                                      job_name, cores, args.priority, bind_node, self.mykey,
                                      args.qname,shell,env))
            if ret.result == RESULT_SUCCESS:
                print("You have submitted one job : job_name =  %s, job_id = %s" % (
                    job_name, ret.extra[RESPONSE_JOBID]))
            elif ret.result == RESULT_FAILURE:
                print("Job not submuted. Reason: %s " % ret.extra[RESPONSE_REASON])


    async def RunJob(self):
        if not os.path.isfile(args.sge_file):
            print("The sge file not exist!")
            return 1
        if args.sge_file[-4:] != ".sge" and args.sge_file[-3:] != ".sh":
            print("The suffix is wrong!")
            return 2

        job_name = None
        cores = None
        env = None
        shell = 'bash'
        with open(args.sge_file) as f:
            for line in f:
                if line[0:3] == "#$ ":
                    field = line.split()
                    if field[1] == "-N":
                        job_name = field[2]
                    elif field[1] == "-pe":
                        cores = int(field[3])
                    elif field[1] == "-V":
                        env = dict(os.environ)
                    elif field[1] == "-S":
                        shell = field[2]
        if job_name is None or cores is None:
            print("The sge syntax is wrong")
            return 3
        bind_node = []
        if args.bind_node is not None:
            bind_node = args.bind_node.split(",")


        ret = await self.rpc.remote.RunJob(self,
            Request_Master_RunJob(os.path.join(os.getcwd(), os.path.dirname(args.sge_file)),
                                  os.path.basename(args.sge_file),
                                  job_name, cores, args.priority, bind_node, self.mykey,
                                  args.qname, shell, env))
        if ret.result == RESULT_SUCCESS:
            print("You have submitted one job : job_name =  %s, job_id = %s" % (
                job_name, ret.extra[RESPONSE_JOBID]))
            print("Waiting for output ...")
            print("Enter raw mode..")
            fd = sys.stdin.fileno()
            os.set_blocking(fd,False)
            self.old_settings = termios.tcgetattr(fd)
            #atexit.register(self.atexit)
            asyncio.get_event_loop().add_signal_handler(signal.SIGINT,self.atexit)
            asyncio.get_event_loop().add_signal_handler(signal.SIGWINCH,self.winchanged)
            tty.setraw(sys.stdin)
            self.job_id =  ret.extra[RESPONSE_JOBID]
            self.queue = asyncio.Queue()
            def loose_connect(exc):
                self.atexit()
            self.rpc.on_connection_reset(loose_connect)
            asyncio.ensure_future(self.send_to_master())
            asyncio.get_event_loop().add_reader(sys.stdin.fileno(),self.ReadFromScreen)

        elif ret.result == RESULT_FAILURE:
            print("Job not submuted. Reason: %s " % ret.extra[RESPONSE_REASON])

    async def Attach(self):
        winsize = fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, b'\0' * 8)
        ret = await self.rpc.remote.AttachJob(self,Request_Master_Attach(args.job_id,self.mykey,winsize))
        if ret.result == RESULT_SUCCESS:
            print("Attach OK !")
            print("Enter raw mode..")
            fd = sys.stdin.fileno()
            os.set_blocking(fd, False)
            self.old_settings = termios.tcgetattr(fd)
            # atexit.register(self.atexit)
            asyncio.get_event_loop().add_signal_handler(signal.SIGINT, self.atexit)
            asyncio.get_event_loop().add_signal_handler(signal.SIGWINCH, self.winchanged)
            tty.setraw(sys.stdin)
            self.job_id = args.job_id
            self.queue = asyncio.Queue()

            def loose_connect(exc):
                self.atexit()

            self.rpc.on_connection_reset(loose_connect)
            asyncio.ensure_future(self.send_to_master())
            asyncio.get_event_loop().add_reader(sys.stdin.fileno(), self.ReadFromScreen)
        elif ret.result == RESULT_FAILURE:
            print("Job attach failed, Reason: %s" % ret.extra)
            asyncio.get_event_loop().stop()



    async def Status(self):
        ret = await self.rpc.remote.Status(Request_Master_Status(args.qname))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
            exit(1)
        if args.verbose:
            if len(ret.running_list) + len(ret.queue_list) == 0:
                print("There is no job in the list")
                return
            first = True
            for job in ret.running_list:
                username = pwd.getpwuid(job.owner_uid)[0]
                if args.username is not None:
                    if username != args.username:
                        continue
                if args.job_id is not None:
                    if job.job_id != args.job_id:
                        continue
                if first:
                    first = False
                else:
                    print(30 * "-")
                print("id          :", job.job_id)
                print("sgefile     :", job.sgefile)
                print("workdir     :", job.workdir)
                print("name        :", job.job_name)
                print("status      :", job.status)
                print("cores       :", job.cores)
                print("user        :", username)
                print("slave       :", job.slave)
                print("shell       :", job.shell)
                print("priority    :", job.priority)
                submit_dt = datetime.fromtimestamp(job.submit_time)
                print("submit_time :", "%s-%02d-%02d %02d:%02d:%02d" % (
                    submit_dt.year, submit_dt.month, submit_dt.day, submit_dt.hour,
                    submit_dt.minute, submit_dt.second))
                if job.status == STATUS_LANCHING:
                    pass
                else:
                    start_dt = datetime.fromtimestamp(job.start_time)
                    print("start_time  :", "%s-%02d-%02d %02d:%02d:%02d" % (
                        start_dt.year, start_dt.month, start_dt.day, start_dt.hour,
                        start_dt.minute, start_dt.second))

            for job in ret.queue_list:
                username = pwd.getpwuid(job.owner_uid)[0]
                if args.username is not None:
                    if username != args.username:
                        continue
                if args.job_id is not None:
                    if job.job_id != args.job_id:
                        continue
                if first:
                    first = False
                else:
                    print(30 * "-")
                print("id          :", job.job_id)
                print("sgefile     :", job.sgefile)
                print("workdir     :", job.workdir)
                print("name        :", job.job_name)
                print("status      :", job.status)
                print("cores       :", job.cores)
                print("user        :", username)
                bind_node = ""
                if len(job.bind_node) > 0:
                    for b in job.bind_node:
                        bind_node += "%s " % b
                print("bind_node   :", bind_node)
                print("shell       :", job.shell)
                print("priority    :", job.priority)
                submit_dt = datetime.fromtimestamp(job.submit_time)
                print("submit_time :", "%s-%02d-%02d %02d:%02d:%02d" % (
                    submit_dt.year, submit_dt.month, submit_dt.day, submit_dt.hour,
                    submit_dt.minute, submit_dt.second))
        else:
            try:
                ts = os.get_terminal_size()
                col = ts.columns
            except:
                col = 100
            max_id_len = len("id")
            max_binding_node_len = len("binding node")
            max_jobname_len = len("name")
            max_user_len = len("user")
            max_time_len = len("2018-05-21 14:18:33")
            max_core_len = len("core")
            max_prio_len = len("pr")
            for job in ret.running_list:
                job.username = pwd.getpwuid(job.owner_uid)[0]
                if args.username is not None:
                    if job.username != args.username:
                        continue
                max_id_len = max(len(str(job.job_id)),max_id_len)
                max_binding_node_len = max(len(job.slave),max_binding_node_len)
                max_jobname_len = max(len(job.job_name),max_jobname_len)
                max_user_len = max(len(job.username),max_user_len)
                max_core_len = max(len(str(job.cores)),max_core_len)
            for job in ret.queue_list:
                job.username = pwd.getpwuid(job.owner_uid)[0]
                if args.username is not None:
                    if job.username != args.username:
                        continue
                max_id_len = max(len(str(job.job_id)),max_id_len)
                if len(job.bind_node) > 0:
                    max_binding_node_len = max(len(ob.bind_node[0]),max_binding_node_len)
                max_jobname_len = max(len(job.job_name),max_jobname_len)
                max_user_len = max(len(username),max_user_len)
                max_core_len = max(len(str(job.cores)),max_core_len)

            max_id_len += 1
            max_binding_node_len += 1
            max_jobname_len +=1
            max_user_len += 1
            max_time_len += 1
            max_prio_len += 1
            max_other_len = max_id_len + max_binding_node_len + max_user_len + max_time_len +max_prio_len
            if max_jobname_len  > col- max_other_len:
                max_jobname_len = col- max_other_len
            max_other_len = max_id_len + max_binding_node_len + max_user_len + \
                            max_time_len + max_core_len + max_prio_len
            real_col_len = max_other_len + max_jobname_len
            print(int(real_col_len/2-4)* "-", "Running", math.ceil(real_col_len/2-5)* "-")
            print("%-*s%-*s%-*s%-*s%-*s%-*s%*s" %(max_id_len,"id", max_binding_node_len,"node",
                                                   max_jobname_len,"name", max_user_len,"user",
                                                    max_time_len,"start time", max_core_len,"core",
                                                    max_prio_len,"st"))
            print(real_col_len * "-")
            for job in ret.running_list:
                if args.username is not None:
                    if job.username != args.username:
                        continue
                dt = datetime.fromtimestamp(job.start_time)
                dt_str = "%s-%02d-%02d %02d:%02d:%02d" % (
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
                job_name = job.job_name
                if len(job_name) > max_jobname_len - 1:
                    job_name = job_name[:max_jobname_len-4] + "..."
                prio = job.priority
                if args.black:
                    print("%-*s%-*s%-*s%-*s%-*s%*s%*s"%
                          ( max_id_len, job.job_id, max_binding_node_len, job.slave,
                              max_jobname_len,job_name,max_user_len, job.username,
                              max_time_len, dt_str, max_core_len,job.cores,
                              max_prio_len,ST[job.status]))
                else:
                    print("\033[32m%-*s\033[m\033[35m%-*s\033[m%-*s%-*s%*s\033[33;1m%*s\033[m%*s" %
                           ( max_id_len, job.job_id, max_binding_node_len, job.slave,
                              max_jobname_len,job_name,max_user_len, job.username,
                              max_time_len, dt_str, max_core_len,job.cores,
                              max_prio_len,ST[job.status]))

            print(int(real_col_len / 2 - 3) * "-", "Queue", math.ceil(real_col_len / 2 - 4) * "-")
            print("%-*s%-*s%-*s%-*s%-*s%-*s%*s"%
                  (max_id_len,"id", max_binding_node_len,"binding node", max_jobname_len,"name",
                   max_user_len,"user", max_time_len,"submit time",max_core_len,"core",
                   max_prio_len,"pr"))
            print(real_col_len * "-")
            for job in ret.queue_list:
                if args.username is not None:
                    if job.username != args.username:
                        continue
                dt = datetime.fromtimestamp(job.submit_time)
                dt_str = "%s-%02d-%02d %02d:%02d:%02d" % (
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
                job_name = job.job_name
                if len(job_name) > max_jobname_len - 1:
                    job_name = job_name[:max_jobname_len-4] + "..."
                prio = job.priority
                if len(job.bind_node) == 0:
                    bind_node = ""
                elif len(job.bind_node) == 1:
                    bind_node = job.bind_node[0]
                else:
                    bind_node = job.bind_node[0][:min(max_binding_node_len-2, len(job.bind_node[0]))] + ".."
                if args.black:
                    print("%-*s%-*s%-*s%-*s%-*s%*s%*s"% (max_id_len,job.job_id,
                      max_binding_node_len, bind_node,max_jobname_len, job_name,
                      max_user_len,job.username, max_time_len,dt_str, max_core_len, job.cores,max_prio_len,prio))
                else:
                    print("\033[32m%-*s\033[m\033[35m%-*s\033[m%-*s%-*s%-*s\033[33;1m%*s\033[m%*s"%
                        (max_id_len,job.job_id,max_binding_node_len, bind_node,max_jobname_len, job_name,
                      max_user_len,job.username, max_time_len,dt_str, max_core_len, job.cores,max_prio_len,prio))

    async def DelJob(self):
        for jobID in args.job_id:
            ret = await self.rpc.remote.QueryJobQName(Request_Master_Query_Queue_Name(jobID))
            if ret.result == RESULT_FAILURE:
                print('Failed to find qname. Reason: %s' % ret.extra[RESPONSE_REASON])
                return 0
            qname = ret.extra['QNAME']
            ret = await self.rpc.remote.DelJob(Request_Master_DelJob(self.mykey, jobID,qname))
            if ret.result == RESULT_FAILURE:
                print("Failed to delete job. Reason: %s" % ret.extra[RESPONSE_REASON])
            elif ret.result == RESULT_SUCCESS:
                print(ret.extra[RESPONSE_INFO], "job id = %s" % jobID)

        return 0

    async def AlterPriority(self):
        for jobID in args.job_id:
            ret = await self.rpc.remote.QueryJobQName(Request_Master_Query_Queue_Name(jobID))
            if ret.result == RESULT_FAILURE:
                print('Failed to find qname. Reason: %s' % ret.extra[RESPONSE_REASON])
                return 0
            qname = ret.extra['QNAME']
            ret = await self.rpc.remote.AlterPriority(
                Request_Master_Alter_Priority(self.mykey, jobID, args.priority, qname))
            if ret.result == RESULT_FAILURE:
                print("Failed to change the priority of job. Reason: %s" % ret.extra[RESPONSE_REASON])
            elif ret.result == RESULT_SUCCESS:
                print(ret.extra[RESPONSE_INFO], "job id = %s" % jobID)
        return 0

    async def AlterBindNode(self):
        for jobID in args.job_id:
            ret = await self.rpc.remote.QueryJobQName(Request_Master_Query_Queue_Name(jobID))
            if ret.result == RESULT_FAILURE:
                print('Failed to find qname. Reason: %s' % ret.extra[RESPONSE_REASON])
                return 0
            qname = ret.extra['QNAME']
            bind_node = []
            if args.bind_node is not None:
                bind_node = args.bind_node.split(",")
            ret = await self.rpc.remote.AlterBindNode(
                Request_Master_Alter_BindNode(self.mykey, jobID, bind_node, qname))

            if ret.result == RESULT_FAILURE:
                print("Failed to change the bind node list of job. Reason : %s" %
                      ret.extra[RESPONSE_REASON])
            elif ret.result == RESULT_SUCCESS:
                print(ret.extra[RESPONSE_INFO])

    async def MachineHours(self):
        ret = await self.rpc.remote.MachineHours(Request_Master_MachineHours())
        print('-------- Machine Hours -------')
        print('--- user ----------- Hours----')
        for uid, hours in ret.info:
            print('   %s%18s' % (pwd.getpwuid(uid)[0], int(hours)))


    async def History(self):
        if args.job_id:
            ret = await self.rpc.remote.History(
                Request_Master_History(args.qname,pwd.getpwnam(args.username).pw_uid if args.username else None,
                                       None,args.job_id))
        else:
            ret = await self.rpc.remote.History(
                Request_Master_History(args.qname,pwd.getpwnam(args.username).pw_uid if args.username else None,
                                       args.day,None))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
            exit(1)
        if args.verbose:
            first = True
            for job in ret.item_info_list:
                username = pwd.getpwuid(job.owner_uid)[0]
                dt = datetime.fromtimestamp(job.finish_time)
                delta = datetime.now() - dt
                if first:
                    first = False
                else:
                    print(30 * "-")
                print("id          :", job.job_id)
                print("sgefile     :", job.sgefile)
                print("workdir     :", job.workdir)
                print("name        :", job.job_name)
                print("status      :", job.status)
                print("cores       :", job.cores)
                print("priority    :", job.priority)
                print("user        :", username)
                dt_str = "%s-%02d-%02d %02d:%02d:%02d" % (
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
                submit_dt = datetime.fromtimestamp(job.submit_time)
                print("submit_time :", "%s-%02d-%02d %02d:%02d:%02d" %
                      (submit_dt.year, submit_dt.month, submit_dt.day, submit_dt.hour, submit_dt.minute,
                       submit_dt.second))
                if job.start_time is None:
                    print("start_time  : Not started")
                else:
                    start_dt = datetime.fromtimestamp(job.start_time)
                    print("start_time  :", "%s-%02d-%02d %02d:%02d:%02d" %
                          (start_dt.year, start_dt.month, start_dt.day,
                           start_dt.hour, start_dt.minute, start_dt.second))

                print("finish_time :", dt_str, " ", end="")
                if delta.days > 0:
                    if delta.days == 1:
                        print(delta.days, " day ", end="")
                    else:
                        print(delta.days, " days ", end="")
                if delta.seconds >= 3600:
                    if delta.seconds == 3600:
                        print(int(delta.seconds / 3600), " hour ", end="")
                    else:
                        print(int(delta.seconds / 3600), " hours ", end="")
                if delta.seconds % 3600 >= 60:
                    if delta.seconds % 3600 == 60:
                        print(int((delta.seconds % 3600) / 60), "minute", end=" ")
                    else:
                        print(int((delta.seconds % 3600) / 60), "minutes", end=" ")
                if (delta.seconds % 3600) % 60 > 0:
                    if (delta.seconds % 3600) % 60 == 1:
                        print(int((delta.seconds % 3600) % 60), "second", end="")
                    else:
                        print(int((delta.seconds % 3600) % 60), "seconds", end="")
                print(" ago")

        else:
            try:
                ts = os.get_terminal_size()
                col = ts.columns
            except:
                col = 100

            max_id_len = len("id")
            max_name_len = len("name")
            max_user_len = len("user")
            max_finish_time_len = len("finish_time")
            max_st_len = len("ST")

            for job in ret.item_info_list:
                job.username = pwd.getpwuid(job.owner_uid)[0]
                dt = datetime.fromtimestamp(job.finish_time)
                job.dt_str = "%s-%02d-%02d %02d:%02d:%02d" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
                max_id_len = max(len(str(job.job_id)),max_id_len)
                max_name_len = max(len(job.job_name),max_name_len)
                max_user_len = max(len(job.username),max_user_len)
                max_finish_time_len = max(len(job.dt_str),max_finish_time_len)

            max_id_len += 1
            max_user_len += 1
            max_finish_time_len += 1
            max_name_len += 1

            max_other_len = max_id_len + max_user_len + max_finish_time_len + max_st_len
            if max_other_len + max_name_len > col:
                max_name_len = col - max_other_len
            col = max_name_len + max_other_len
            print(col * "-")
            print("%-*s%-*s%-*s%-*s%*s" % (max_id_len,"id", max_name_len,"name", max_user_len,"user",
                                           max_finish_time_len,"finish_time", max_st_len,"ST"))
            print(col * "-")
            for job in ret.item_info_list:
                job_name = job.job_name
                if len(job_name) > max_name_len:
                    job_name = job_name[:max_finish_time_len-4] + "..."
                print("%-*s%-*s%-*s%*s%*s" % (max_id_len,job.job_id, max_name_len,job_name,
                                              max_user_len,username, max_finish_time_len,job.dt_str,
                                              max_st_len,ST[job.status]))

    async def Cluster(self):
        ret = await self.rpc.remote.Cluster(Request_Master_Cluster())
        print("Cluster system status")
        print("---------------------")
        print("%-15s%-10s%-10s%-10s%-10s" % ("node","qname","total","used","unused"))
        for node in ret.slave_infos:
            print("%-15s%-10s%-10s%-10s%-10s" % (node.name, node.queue_name, node.total_cores,
                                         node.used_cores, node.total_cores - node.used_cores))

    async def QueueList(self):
        ret = await self.rpc.remote.QueueList(Request_Master_QueueList())
        print("Queue List")
        print("---------------------")
        for qname in ret.queue_list:
            print("%s" % qname)
        print("---------------------")

    async def UserLimit(self):
        ret = await self.rpc.remote.UserLimit(Request_Master_UserLimit(args.qname))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
            exit(1)
        print("User Limit")
        print("--------------------")
        print("username\tusedcore\tmaxcore\tforbid_sub")
        for node in ret.userlimit_infos:
            print("%s   \t%s    \t\t%s\t%s" % (node.username,
                                               node.used_cores, node.max_cores, node.forbid_sub))



    async def AlterCluster(self):
        ret = await self.rpc.remote.AlterCluster(
            Request_Master_Alter_Cluster(self.mykey, args.node, args.total, args.qname))
        if ret.result == RESULT_FAILURE:
            print("Failed to alter the cluster setting. Reason: %s" %
                  ret.extra[RESPONSE_REASON])

    async def AlterUserLimit(self):
        ret = await self.rpc.remote.AlterUserLimit(
            Request_Master_Alter_UserLimit(self.mykey, args.max, args.name, args.forbid_sub, args.qname))

        if ret.result == RESULT_FAILURE:
            print("Failed to alter the userlimit setting. Reason: %s" %
                  ret.extra[RESPONSE_REASON])
        elif ret.reult == RESULT_SUCCESS:
            print(ret.extra[RESPONSE_INFO])

    async def ReloadKey(self):
        ret = await self.rpc.remote.ReloadKey(Request_Master_ReloadKey(self.mykey))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])

    async def NewQueue(self):
        ret = await self.rpc.remote.NewQueue(Request_Master_New_Queue(self.mykey,args.qname))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
        else:
            print(ret.extra[RESPONSE_INFO])

    async def DelQueue(self):
        ret = await self.rpc.remote.DelQueue(Request_Master_Del_Queue(self.mykey,args.qname))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
        else:
            print(ret.extra[RESPONSE_INFO])

    async def CloseQueue(self):
        ret = await self.rpc.remote.CloseQueue(Request_Master_Close_Queue(self.mykey,args.qname))
        if ret.result == RESULT_FAILURE:
            print(ret.extra[RESPONSE_REASON])
        else:
            print(ret.extra[RESPONSE_INFO])

    @aiomas.expose
    async def GetWinSZ(self):
        return fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, b'\0' * 8), \
               termios.tcgetattr(sys.stdin.fileno())


    @aiomas.expose
    async def RunJobComplete(self, req : Request_Client_RunJobComplete) -> Response_Client_RunJobComplete:
        self.atexit()

    @aiomas.expose
    async def JobPtyOut(self, req : Request_Client_JobPtyOut) -> Response_Client_JobPtyOut:
        os.write(sys.stdout.fileno(),req.data)
        return Response_Client_JobPtyOut()

    async def send_to_master(self):
        while True:
            data = await self.queue.get()
            await self.rpc.remote.RelayPtyInput(Request_Master_JobInput(self.job_id, data))

    def ReadFromScreen(self):
        while True:
            try:
                data = os.read(sys.stdin.fileno(),1024)
                if len(data) != 0:
                    self.queue.put_nowait(data)
                    if b'\x01\x64' in self.prev_data+data:
                        self.atexit()
                    self.prev_data = data
                else:
                    return
            except:
                return


    def atexit(self):
        termios.tcsetattr(sys.stdin.fileno(),termios.TCSADRAIN, self.old_settings)
        print("\nexit system")
        asyncio.get_event_loop().stop()

    def send_sigint_to_master(self):
        asyncio.ensure_future(self.notidy_slave_sigint())

    async def notidy_slave_sigint(self):
        await self.rpc.remote.RelayPtySignal(Request_Master_PtySignal(self.job_id,signal.SIGINT))

    def winchanged(self):
        asyncio.ensure_future(self.notify_slave_winsize())

    async def notify_slave_winsize(self):
        winsize = fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, b'\0' * 8)
        await self.rpc.remote.RelayPtyWINSZ(Request_Master_WINSZ(self.job_id, winsize))


def check_postive_integer(string):
    try:
        value = int(string)
    except TypeError as e:
        raise argparse.ArgumentTypeError(e.args)

    if value <= 0:
        msg = "%r is not a positive number!" % string
        raise argparse.ArgumentTypeError(msg)
    return value


def check_prio_range(string):
    try:
        value = int(string)
    except TypeError as e:
        raise argparse.ArgumentTypeError(e.args)
    if value < 0 or value > 10:
        msg = "priority must in range of 0-10"
        raise argparse.ArgumentTypeError(msg)
    return value


parser = argparse.ArgumentParser(
    description="Client for cluster dispatch system")
subparsers = parser.add_subparsers(dest="command_type")

subparsers_sub = subparsers.add_parser(
    "sub", description="submit a job", help="submit a job")
subparsers_sub.add_argument(
    "-b", "--bind_node", help="can be a list connected by comma")
subparsers_sub.add_argument("-q","--qname", default="main", help="queue name")

subparsers_sub.add_argument(
    "-p", "--priority", type=check_prio_range, help="range(0-10) integer value", default=0)

subparsers_sub.add_argument("sge_file", nargs='+')

subparsers_run = subparsers.add_parser(
    "run", description="interactive run a job", help="run a job")
subparsers_run.add_argument(
    "-b", "--bind_node", help="can be a list connected by comma")
subparsers_run.add_argument("-q","--qname", default="main", help="queue name")

subparsers_run.add_argument(
    "-p", "--priority", type=check_prio_range, help="range(0-10) integer value", default=0)

subparsers_run.add_argument("sge_file")


subparsers_attach = subparsers.add_parser(
    "attach", description="attach to interactive job", help="attach to a interactive job")

subparsers_attach.add_argument("job_id", type=check_postive_integer)

subparsers_status = subparsers.add_parser("status", description="query the queue status",
                                          help="query the queue status")
subparsers_status.add_argument( "-q","--qname", default="main", help="queue name")

subparsers_status.add_argument(
    "-b", "--black", help="output black/white content", action='store_true')
subparsers_status.add_argument(
    "-v", "--verbose", help="print every detail", action='store_true')

subparsers_status.add_argument(
    "-u", "--username", help="username filter")

subparsers_status.add_argument(
    "-i", "--job_id", type=check_postive_integer, help="Job id( only in verbose mode)")

subparsers_del = subparsers.add_parser("del", help="delete the job")
subparsers_del.add_argument("job_id",nargs="+",type=check_postive_integer)

subparsers_cluster = subparsers.add_parser("cluster", description="query the cluster system settings",
                                           help="query the cluster system settings")

subparsers_queue_list = subparsers.add_parser("queue", description="query the queue list",
                                           help="query the queue list")

subparsers_history = subparsers.add_parser("history", description="query finished jobs",
                                           help="query finished jobs")
subparsers_history.add_argument("-q","--qname", default="main", help="queue name")


subparsers_history.add_argument(
    "-v", "--verbose", help="print every detail", default=False, action='store_true')
subparsers_history.add_argument(
    "-u", "--username", help="username filter")
subparsers_history.add_argument(
    "-d", "--days", type=check_postive_integer,default=1, help="Only show in recently days")

subparsers_history.add_argument(
    "-i", "--job_id", type=check_postive_integer, help="Job id( only in verbose mode)")

subparsers_query_userlimit = subparsers.add_parser("userlimit", description="query the userlimit settings",
                                                   help="query the userlimit settings")

subparsers_statuscluster = subparsers.add_parser("statc", description="query the status + cluster system settings",
                                                 help="query the status + cluster system settings")
subparsers_statuscluster.add_argument("-q","--qname", default="main", help="queue name")

subparsers_statuscluster.add_argument(
    "-b", "--black", help="output black/white content", action='store_true')
subparsers_statuscluster.add_argument(
    "-v", "--verbose", help="print every detail", action='store_true')

subparsers_statuscluster.add_argument("-u", "--username", help="username filter")

subparsers_statuscluster.add_argument(
    "-i", "--job_id", type=check_postive_integer, help="Job id( only in verbose mode)")

subparsers_query_userlimit = subparsers.add_parser("userlimit", description="query the userlimit settings",
                                                   help="query the userlimit settings")
subparsers_query_userlimit.add_argument("-q","--qname", default="main", help="queue name")


subparsers_loadkey = subparsers.add_parser("loadkey", description="loadkey",
                                           help="loadkey")

subparsers_alter = subparsers.add_parser("alter_cluster", description="change the cluster system settings",
                                         help="change the cluster system settings")

cluster_node = subparsers_alter.add_argument_group("node settings")
cluster_node.add_argument("-n", "--node", help="the host name", required=True)
cluster_node.add_argument("-t", "--total", help="total node cores",
                          type=check_no_negative_integer, required=True)
cluster_node.add_argument("-q", "--qname", default="main", help="queue name")


subparsers_userlimit = subparsers.add_parser("alter_userlimit", description="change the userlimit settings",
                                             help="change the userlimit settings")

userlimit_node = subparsers_userlimit.add_argument_group("userlimit settings")
userlimit_node.add_argument("-n", "--name", help="the username", required=True)
userlimit_node.add_argument(
    "-m", "--max", help="the max core", type=int, required=True)
userlimit_node.add_argument(
    "-f", "--forbid_sub", help="forbid submit job", type=bool, default=False)
userlimit_node.add_argument("-q","--qname", default="main", help="queue name")


subparsers_priority = subparsers.add_parser("change_priority", description="change the priority settings",
                                            help="change the priority of job")

subparsers_priority.add_argument("job_id", nargs="+", type=check_postive_integer, help="the job_id")
subparsers_priority.add_argument(
    "-p", "--priority", help="the priority of job (0-10)", default=10, type=check_prio_range, required=True)



subparsers_bindnode = subparsers.add_parser("alter_bind_node", description="change the bind node settings",
                                            help="change the bind node list of job")


subparsers_bindnode.add_argument("job_id",nargs='+', type=check_postive_integer, help="the job_id")
subparsers_bindnode.add_argument(
    "-b", "--bind_node", help="can be a list connected by comma", default=None)


subparsers_new_queue = subparsers.add_parser("new_queue", help="new task queue")
subparsers_new_queue.add_argument("-q","--qname", help="queue name")

subparsers_del_queue = subparsers.add_parser("del_queue", help="del task queue")
subparsers_del_queue.add_argument("-q","--qname", help="queue name")

subparsers_close_queue = subparsers.add_parser("close_queue", help="close task queue")
subparsers_close_queue.add_argument("-q","--qname", help="queue name")

subparsers_machine_hours = subparsers.add_parser("hours",
                                                 description="query the machine hours used since program started",
                                                help="query the machine hours used since program started")


args = parser.parse_args()

if len(sys.argv) == 1:
    parser.print_help()
    exit(4)

client = Client()
asyncio.ensure_future(client.run())
aiomas.run()
