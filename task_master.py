#!/usr/bin/env python3

# scheduler master program

import os, sys
import asyncio
import json
import ssl
import pwd
from typing import *
from datetime import *
import heapq
from common import *
import hashlib
import aiomas
import socket
import logging
import sqlite3
import argparse
import signal


class Key:
    def __init__(self):
        self.username: str = None
        self.key: str = None


def loadKeys():
    key_list = []
    with open("key.json") as f:
        keys_json = json.load(f)
    for item in keys_json:
        key = Key()
        key.username = item["username"]
        key.key = item["key"]
        key_list.append(key)
    return key_list


SLAVE_STATUS_CONNECTED = "SLAVE_STATUS_CONNECTED"
SLAVE_STATUS_DISCONNETED = "SLAVE_STATUS_DISCONNETED"


class Slave:
    def __init__(self):
        self.name: str = None
        self.rpc = None
        self.total_cores: int = 0
        self.used_cores: int = 0
        self.running_list: List[Job] = []
        self.queue_name : str = "main"  # the queue name belongs to

        self.status = None


class Job:
    curr_job_id: int = 0

    @classmethod
    def new_job_id(cls):
        cls.curr_job_id += 1
        return cls.curr_job_id

    def __init__(self):
        self.workdir: str = None
        self.sgefile: str = None
        self.status: str = None
        self.cores: int = 0
        self.job_id: int = 0
        self.slave: Slave = None
        self.submit_time: datetime = None
        self.start_time: datetime = None
        self.finish_time: datetime = None
        self.owner_uid: int = None
        self.owner_gid: int = None
        self.job_name: str = None
        self.priority: int = None
        self.bind_node: List[str] = []
        self.userusage: UserUsage = None

        self.queue_name: str = "main"

        self.shell: str = None
        self.env: dict[str,str] = None

        self.run_type = BATCH
        self.buffer_first = False


class UserLimit:
    def __init__(self, queue_name: str):
        self.username: str = None
        self.user_id: int = None
        self.max_cores: int = None
        self.used_cores: int = None
        self.forbid_sub: bool = False

        self.queue_name: str = queue_name


class UserUsage:
    def __init__(self, queue_name: str):
        self.user_id: int = None
        self.used_cores: int = 0
        self.running_list: List[Job] = []
        self.queue_list: List[Job] = []

        self.queue_name: str = queue_name



'''create table if not exists userlimit (user_id integer primary key not null,
                                                 max_cores integer not null check(max_cores >=0),
                                                 forbid_sub integer not null check(forbid_sub = 0 or forbid_sub = 1),
                                                 queue_name text not null)'''

'''create table if not exists task_queue (qid integer primary key not null autoincrement,
                                                  name text not null)
                                                  '''
'''create table if not exists queue_node_relation ( qid integer foreign key references task_queue(qid),
                                                            node_name text not null '''

class Database:
    def __init__(self, filename : str = "task_master.db"):
        self.filename = filename

    def open(self):
        self.db = sqlite3.connect(self.filename)

        # the following two sql DDL statements must be applyed before program execution
        # create table to store evergy details of job used in system
        c = self.db.cursor()
        c.execute('''create table if not exists joblist (id integer primary key not null,
                                 sgefile text not null ,
                                 job_name text not null ,
                                 workdir text not null ,
                                 status text not null ,
                                 cores integer not null ,
                                 priority integer default 0,
                                 owner_uid integer not null ,
                                 owner_gid integer not null ,
                                 bind_node text,
                                 finish_time real,
                                 submit_time real not null ,
                                 start_time real,
                                 queue_name text not null ,
                                 shell text not null ,
                                 run_type text not null ,
                                 env text)''')

        # create view for machine hours used by every user

        c.execute('''create view if not exists machineHours  as select owner_uid, sum(((case
                                                                when finish_time isnull 
                                                                   then strftime('%s','now')
                                                                else finish_time 
                                                              end)- start_time)*cores)/3600 as hours
            from joblist where start_time <> null and status <> 'STATUS_ERROR' group by owner_uid''')
        self.db.commit()
        c.close()

    def close(self):
        self.db.close()

    def getMachineHours(self):
        c = self.db.cursor()
        for row in c.execute("select owner_uid, hours from machineHours"):
            yield row

        self.db.commit()
        c.close()

    def check_integrity(self):
        c = self.db.cursor()

        c.execute("update joblist set status = ?, finish_time = ? where status = ? or status = ? or status = ?",
                      (STATUS_ERROR,datetime.now().timestamp(),STATUS_RUNNING, STATUS_CANCELING, STATUS_LANCHING))
        self.db.commit()
        c.close()

    def add_new_job(self, job : Job):
        c = self.db.cursor()
        c.execute("select count(*) from joblist where id = ?", (job.job_id,))
        count, _ = c.fetchone()
        if count == 0:
            c.execute("INSERT INTO joblist VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (job.job_id, job.sgefile, job.job_name, job.workdir, job.status, job.cores, job.priority,
                       job.owner_uid,job.owner_gid, None if len(job.bind_node) == 0 else json.dumps(job.bind_node),
                       None, job.submit_time.timestamp(),
                       job.start_time.timestamp() if job.start_time is not None else None,
                       job.queue_name, job.shell,job.run_type,json.dumps(job.env)));
        self.db.commit()
        c.close()

    def change_job_priority(self, job : Job):
        c = self.db.cursor()
        c.execute("update joblist set  priority = ? where id = ?", (job.priority, job.job_id))
        self.db.commit()
        c.close()

    def change_job_bindnode(self, job : Job):
        c = self.db.cursor()
        c.execute("update joblist set  bind_node = ? where id = ?",
                  (None if len(job.bind_node) == 0 else json.dumps(job.bind_node), job.job_id))
        self.db.commit()
        c.close()

    def change_job_status(self,Job):
        c = self.db.cursor()
        c.execute("update joblist set  status = ? where id = ?", (job.status, job.job_id))
        self.db.commit()
        c.close()

    def change_job_running(self, job : Job):
        c = self.db.cursor()
        c.execute("update joblist set start_time = ? , status = ? where id = ?",
                  (job.start_time.timestamp(), job.status, job.job_id))
        self.db.commit()
        c.close()


    def change_job_complete(self, job : Job):
        c = self.db.cursor()
        start_time = None
        if job.start_time is not None:
            start_time = job.start_time.timestamp()
        c.execute("update joblist set finish_time = ? , status = ?  where id = ? ",
                  (job.finish_time.timestamp(), job.status ,job.job_id));
        self.db.commit()
        c.close()

    def select_queue_job(self, qname : str):
        c = self.db.cursor()
        for row in c.execute('''select id,sgefile,job_name,workdir,cores,priority,owner_uid, owner_gid,
                              bind_node,submit_time,queue_name,shell,run_type,env from joblist
                              where status = ? and queue_name = ?''',
                             STATUS_QUEUE, qname):
            job = Job()
            job.job_id, job.sgefile, job.job_name, job.workdir, job.cores, job.priority, job.owner_uid, \
            job.owner_gid, job.bind_node, job.submit_time, job.queue_name,job.shell,job.run_type,job.env = row

            job.status = STATUS_QUEUE
            job.submit_time = datetime.fromtimestamp(job.submit_time)
            job.bind_node = json.loads(job.bind_node) if job.bind_node is not None else []
            job.env = json.loads(job.env) if job.env is not None else None
            yield job
        self.db.commit()
        c.close()

    def select_finish_job(self, qname : str, owner_uid : int, days : int, job_id : int):
        c = self.db.cursor()
        sqlstr = '''select id, sgefile, job_name, workdir, status, cores, priority, owner_id, finish_time,
                     submit_time, start_time, qname_name, shell from joblist 
                     where  (status = '%s' or status = '%s' or  status = '%s')''' % (STATUS_DELETE, STATUS_FINISH, STATUS_ERROR)

        if job_id is not None:
            sqlstr += " and id = %s " % job_id
        elif owner_uid is not None:
            sqlstr += " and owner_uid = %s " % owner_uid
        elif days is not None:
            sqlstr += " and  finish_time >= %s " % (datetime.now().timestamp() - days * 24 * 3600.0)
        elif qname is not None:
            sqlstr += " and queue_name = '%s' " % qname

        sqlstr += " order by finish_time "

        for row in c.execute(sqlstr):
            yield row


    def current_max_job_id(self):
        maxvalue  = 0
        c = self.db.cursor()
        try:
            c.execute("select max(id) from joblist")
            row = c.fetchone()
            if row is not None:
                maxvalue, = row
        except:
            pass
        c.close()
        if maxvalue is None: maxvalue = 0
        return maxvalue


class TaskQueue:
    def __init__(self, queue_name: str, master):
        self.slave_list: List[Slave] = []
        self.running_list: List[Job] = []
        self.queue_list: List[Job] = []
        self.userusage_list: List[UserUsage] = []
        self.userlimit_list: List[UserLimit] = []

        self.sched_event = asyncio.Event()
        self.delay = False

        self.moniter = asyncio.ensure_future(self.start_moniter())

        self.queue_name = queue_name

        self.close_queue = False
        self.master = master

        for job in self.master.database.select_queue_job(self.queue_name):
            for usage in self.userusage_list:
                if usage.user_id == job.owner_uid:
                    usage.queue_list.append(job)
                    job.userusage = usage
                    break
            else:
                usage = UserUsage(self.queue_name)
                usage.queue_list.append(job)
                usage.user_id = job.owner_uid
                self.userusage_list.append(usage)
                job.userusage = usage

            self.queue_list.append(job)

    async def start_moniter(self):
        while True:
            await self.sched_event.wait()
            self.sched_event.clear()
            while self.delay:
                self.delay = False
                await self.schedule(False)
                await asyncio.sleep(300)
            await self.schedule()

    async def schedule(self, smallcore = True):
        logging.info("start schedule")
        cont_loop = True
        while cont_loop:
            cont_loop = False
            if len(self.queue_list) == 0:
                return
            #        master.slave_list.sort(key=lambda n: n.total_cores - n.used_cores,reverse=True)
            for slave in self.slave_list:
                cores_left = slave.total_cores - slave.used_cores

                all_jobs = []
                for job in self.queue_list:
                    heapq.heappush(all_jobs,
                                   (job.userusage.used_cores * 1000000 + job.job_id + 1000 * job.priority, job))

                pass_job = False
                if cores_left > 0:
                    while (len(all_jobs) > 0):
                        prio, job = heapq.heappop(all_jobs)
                        if job.cores <= cores_left:
                            for userlimit in self.userlimit_list:
                                if userlimit.user_id == job.owner_uid:
                                    if userlimit.maxcore is not None:
                                        if userlimit.used_cores + job.cores > userlimit.max_cores:
                                            pass_job = True
                                            break
                            if pass_job is True:
                                pass_job = False
                                continue
                            if smallcore is False and job.cores < 12:
                                logging.info("skip job_id = %s",job.job_id)
                                continue

                            if len(job.bind_node) != 0 and slave.name not in job.bind_node:
                                continue
                            logging.info("send job_id = %s", job.job_id)
                            asyncio.ensure_future(self.send_job_to_slave(job, slave))
                            cont_loop = True
                            slave.used_cores += job.cores
                            cores_left -= job.cores
                            job.userusage.used_cores += job.cores
                            for userlimit in self.userlimit_list:
                                if userlimit.user_id == job.owner_uid:
                                    userlimit.used_cores += job.cores
                            self.queue_list.remove(job)
                            job.userusage.queue_list.remove(job)

    async def send_job_to_slave(self, job: Job, slave: Slave):

        logging.info("send job to slave: job_id : %s, job_name : %s, slave_name: %s",job.job_id, job.job_name, slave.name)
        job.status = STATUS_LANCHING
        self.master.database.change_job_status(job)
        if job.run_type == INTERACTIVE:
            try:
                job.winsize, job.tty_attr = await job.rpc.GetWinSZ()
            except:
                job.winsize = None
                job.tty_attr = None
        else:
            job.winsize = None
            job.tty_attr = None
        await slave.rpc.SubJob(
            Request_Slave_SubJob(job.job_id, job.workdir, job.sgefile,
                                 job.job_name, job.status, job.cores, job.owner_uid, job.owner_gid,
                                 job.submit_time.timestamp(),job.priority,job.bind_node,
                                 job.queue_name,job.shell,job.env,job.run_type, job.winsize, job.tty_attr))

        job.slave = slave
        slave.running_list.append(job)
        self.running_list.append(job)
        job.userusage.running_list.append(job)


class Master:
    router = aiomas.rpc.Service()

    def __init__(self):
        self.slave_list: List[Slave] = []
        self.taskqueue_dict: Dict[str, TaskQueue] = {}

        self.key_list: List[Key] = loadKeys()
        self.database = Database()
        self.database.open()
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, self.exit)
        self.database.check_integrity()
        self.taskqueue_dict["main"] = TaskQueue("main", self)
        Job.curr_job_id = self.database.current_max_job_id()

    @aiomas.expose
    async def SlaveRegister(self, rpc, req: Request_Master_SlaveRegister) -> Response_Master_SlaveRegister:
        slave = Slave()
        slave.name = req.name
        slave.total_cores = req.total_cores
        slave.rpc = rpc
        slave.queue_name = req.queue_name
        slave.used_cores = 0
        self.slave_list.append(slave)
        self.taskqueue_dict[slave.queue_name].slave_list.append(slave)

        for job in req.job_info:
            newjob = Job()
            newjob.job_id = job.job_id
            newjob.workdir = job.workdir
            newjob.sgefile = job.sgefile
            newjob.job_name = job.job_name
            newjob.status = job.status
            newjob.cores = job.cores
            newjob.owner_uid = job.owner_uid
            newjob.owner_gid = job.owner_gid
            newjob.submit_time = datetime.fromtimestamp(job.submit_time)
            newjob.start_time = datetime.fromtimestamp(job.start_time)
            newjob.priority = job.priority
            newjob.bind_node = job.bind_node
            newjob.slave = slave
            slave.running_list.append(newjob)
            newjob.queue_name = job.queue_name
            newjob.shell = job.shell
            newjob.env = job.env
            for userlimit in self.taskqueue_dict[newjob.queue_name].userlimit_list:
                if userlimit.user_id == newjob.owner_uid:
                    userlimit.used_cores += newjob.cores
                    break

            for userusage in self.taskqueue_dict[newjob.queue_name].userusage_list:
                if userusage.user_id == newjob.owner_uid:
                    userusage.used_cores += newjob.cores
                    userusage.running_list.append(newjob)
                    newjob.userusage = userusage
                    break

            else:
                userusage = UserUsage(req.queue_name)
                userusage.user_id = newjob.owner_uid
                userusage.used_cores += newjob.cores
                userusage.running_list.append(newjob)
                newjob.userusage = userusage
                self.taskqueue_dict[newjob.queue_name].userusage_list.append(userusage)


            self.taskqueue_dict[newjob.queue_name].running_list.append(newjob)
            slave.used_cores += newjob.cores
            newjob.run_type = job.run_type
            self.database.add_new_job(newjob)
            self.database.change_job_status(job)

        self.taskqueue_dict[slave.queue_name].sched_event.set()
        return Response_Master_SlaveRegister()

    @aiomas.expose
    async def JobComplete(self, req: Request_Master_JobComplete) -> Response_Master_JobComplete:
        qqueue = self.taskqueue_dict[req.queue_name]
        for job in qqueue.running_list:
            if job.job_id == req.job_id:
                job.slave.used_cores -= job.cores
                qqueue.running_list.remove(job)
                job.finish_time = datetime.fromtimestamp(req.finish_time)
                job.status = STATUS_FINISH
                self.database.change_job_complete(job)
                job.userusage.running_list.remove(job)
                job.slave.running_list.remove(job)
                job.userusage.used_cores -= job.cores
                for userlimit in qqueue.userlimit_list:
                    if userlimit.user_id == job.owner_uid:
                        userlimit.used_cores -= job.cores
                if job.cores < 12 and len(job.slave.running_list) != 0 and len(qqueue.queue_list) != 0 \
                        and (job.slave.total_cores - job.slave.used_cores) < 12:
                    for j2 in job.slave.running_list:
                        if j2.cores < 12:
                            for j in qqueue.queue_list:
                                if j.cores == 12:
                                    qqueue.delay = True
                                    break
                            else:
                                continue
                            break

                qqueue.sched_event.set()
                try:
                    await job.rpc.RunJobComplete(Request_Client_RunJobComplete(job.job_id))
                except:
                    pass
                return Response_Master_JobComplete()

    @aiomas.expose
    async def JobStarted(self, req: Request_Master_JobStarted) -> Response_Master_JobStarted:
        qqueue = self.taskqueue_dict[req.queue_name]
        for job in qqueue.running_list:
            if job.job_id == req.job_id:
                job.status = STATUS_RUNNING
                job.start_time = datetime.fromtimestamp(req.start_time)
                self.database.change_job_running(job)
                logging.debug("job started: job_id : %s, queue_name : %s, job_name : %s, slave_name: %s"
                              % (job.job_id, job.queue_name, job.job_name, job.slave.name))
                return Response_Master_JobStarted()

    @aiomas.expose
    async def SubJob(self, req: Request_Master_SubJob) -> Response_Master_SubJob:
        job = Job()
        job.priority = req.priority
        if job.priority < 0 or job.priority > 10:
            return Response_Master_SubJob(RESULT_FAILURE, {RESPONSE_REASON: "Priority value out of range"})
        key = req.key
        for item in self.key_list:
            if item.key == key:
                username = item.username
                job.owner_uid, job.owner_gid = pwd.getpwnam(username)[2:4]

        if req.queue_name not in self.taskqueue_dict:
            return  Response_Master_SubJob(RESULT_FAILURE,
                                           {RESPONSE_REASON:"Queue name %s doesn't exist" % req.queue_name})

        qqueue = self.taskqueue_dict[req.queue_name]
        if qqueue.close_queue:
            return Response_Master_SubJob(RESULT_FAILURE, {RESPONSE_REASON: "Queue was closed"})
        for userlimit in qqueue.userlimit_list:
            if userlimit.user_id == job.owner_uid:
                if userlimit.forbid_sub:
                    return Response_Master_SubJob(RESULT_FAILURE, {RESPONSE_REASON: "Submition permission denied"})
        if job.owner_uid == 0:
            return Response_Master_SubJob(RESULT_FAILURE, {RESPONSE_REASON: "root cannot submit jobs"})

        job.workdir = req.workdir
        job.sgefile = req.sgefile
        job.job_name = req.job_name
        job.cores = req.cores
        job.bind_node = req.bind_node
        job.submit_time = datetime.now()
        for usage in qqueue.userusage_list:
            if usage.user_id == job.owner_uid:
                usage.queue_list.append(job)
                job.userusage = usage
                break
        else:
            usage = UserUsage(req.queue_name)
            usage.queue_list.append(job)
            usage.user_id = job.owner_uid
            qqueue.userusage_list.append(usage)
            job.userusage = usage

        job.job_id = Job.new_job_id()
        job.status = STATUS_QUEUE
        job.shell = req.shell
        job.env = req.env
        job.run_type = BATCH
        qqueue.queue_list.append(job)
        qqueue.sched_event.set()
        self.database.add_new_job(job)

        return Response_Master_SubJob(RESULT_SUCCESS, {RESPONSE_JOBID: job.job_id})

    @aiomas.expose
    async def RunJob(self, rpc, req : Request_Master_RunJob) -> Response_Master_RunJob:
        job = Job()
        job.priority = req.priority
        if job.priority < 0 or job.priority > 10:
            return Response_Master_RunJob(RESULT_FAILURE, {RESPONSE_REASON: "Priority value out of range"})
        key = req.key
        for item in self.key_list:
            if item.key == key:
                username = item.username
                job.owner_uid, job.owner_gid = pwd.getpwnam(username)[2:4]

        if req.queue_name not in self.taskqueue_dict:
            return  Response_Master_RunJob(RESULT_FAILURE,
                                           {RESPONSE_REASON:"Queue name %s doesn't exist" % req.queue_name})

        qqueue = self.taskqueue_dict[req.queue_name]
        if qqueue.close_queue:
            return Response_Master_RunJob(RESULT_FAILURE, {RESPONSE_REASON: "Queue was closed"})
        for userlimit in qqueue.userlimit_list:
            if userlimit.user_id == job.owner_uid:
                if userlimit.forbid_sub:
                    return Response_Master_RunJob(RESULT_FAILURE, {RESPONSE_REASON: "Submition permission denied"})
        if job.owner_uid == 0:
            return Response_Master_RunJob(RESULT_FAILURE, {RESPONSE_REASON: "root cannot submit jobs"})

        job.workdir = req.workdir
        job.sgefile = req.sgefile
        job.job_name = req.job_name
        job.cores = req.cores
        job.bind_node = req.bind_node
        job.submit_time = datetime.now()
        for usage in qqueue.userusage_list:
            if usage.user_id == job.owner_uid:
                usage.queue_list.append(job)
                job.userusage = usage
                break
        else:
            usage = UserUsage(req.queue_name)
            usage.queue_list.append(job)
            usage.user_id = job.owner_uid
            qqueue.userusage_list.append(usage)
            job.userusage = usage

        job.job_id = Job.new_job_id()
        job.status = STATUS_QUEUE
        job.shell = req.shell
        job.env = req.env
        qqueue.queue_list.append(job)
        self.database.add_new_job(job)
        qqueue.sched_event.set()

        job.rpc = rpc
        job.run_type = INTERACTIVE
        return Response_Master_RunJob(RESULT_SUCCESS, {RESPONSE_JOBID: job.job_id})

    @aiomas.expose
    async def Status(self, req: Request_Master_Status) -> Response_Master_Status:
        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_Status(
                RESULT_FAILURE,extra={RESPONSE_REASON:"Queue name %s doesn't exist" % req.queue_name})
        qqueue = self.taskqueue_dict[req.queue_name]
        return Response_Master_Status(
            RESULT_SUCCESS,
            [Status_Queue_List_Info(job.job_id, job.sgefile, job.job_name, job.workdir,
                                    job.status,job.cores, job.owner_uid,
                                    job.submit_time.timestamp(),job.bind_node, job.priority)
                for job in qqueue.queue_list],
            [Status_Running_List_Info(job.job_id, job.sgefile, job.job_name, job.workdir,
                                      job.status, job.cores, job.slave.name, job.owner_uid,
                                      job.submit_time.timestamp(),job.start_time.timestamp(),
                                      job.priority)
                for job in qqueue.running_list])

    @aiomas.expose
    async def QueueList(self, req : Request_Master_QueueList) -> Response_Master_QueueList:
        return  Response_Master_QueueList( [ qname for qname in self.taskqueue_dict ])

    @aiomas.expose
    async def MachineHours(self, req : Request_Master_MachineHours) -> Response_Master_MachineHours:
        return Response_Master_MachineHours([ row for row in self.database.getMachineHours()])

    @aiomas.expose
    async def History(self, req: Request_Master_History) -> Response_Master_History:
    #    if req.queue_name not in self.taskqueue_dict:
    #        return Response_Master_History(
    #            RESULT_FAILURE, extra={RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})
        return Response_Master_History(RESULT_SUCCESS, [History_Item_Info(*row)
               for row in self.database.select_finish_job(req.queue_name, req.owner_uid, req.days, req.job_id)])

    @aiomas.expose
    async def Cluster(self, req: Request_Master_Cluster) -> Response_Master_Cluster:
        return Response_Master_Cluster(
            RESULT_SUCCESS,
            [Cluster_Slave_Info(slave.name, slave.total_cores, slave.used_cores, slave.queue_name)
             for slave in self.slave_list])

    @aiomas.expose
    async def UserLimit(self, req: Request_Master_UserLimit) -> Response_Master_UserLimit:
        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_UserLimit(
                RESULT_FAILURE, extra={RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})

        return Response_Master_UserLimit(
            RESULT_SUCCESS,
                     [UserLimit_User_Info(userlimit.username, userlimit.used_cores,
                                          userlimit.max_cores,
                                          userlimit.forbid_sub) for userlimit in
                      self.taskqueue_dict[req.queue_name].userlimit_list])

    @aiomas.expose
    async def AlterCluster(self, req: Request_Master_Alter_Cluster) -> Response_Master_Alter_Cluster:
        owner_uid = -1
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
        if owner_uid != 0:
            return Response_Master_Alter_Cluster(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})
        for slave in self.slave_list:
            if slave.name == req.name:
                # slave.used_cores = req.used_cores
                slave.total_cores = req.total_cores
                if slave.queue_name != req.queue_name:
                    if req.queue_name not in self.taskqueue_dict:
                        return Response_Master_Alter_Cluster(
                            RESULT_FAILURE,{RESPONSE_REASON:"Queue name %s doesn't exist" % req.queue_name})
                    if slave.queue_name: self.taskqueue_dict[slave.queue_name].slave_list.remove(slave)
                    self.taskqueue_dict[req.queue_name].slave_list.append(slave)
                    slave.queue_name = req.queue_name
                self.taskqueue_dict[slave.queue_name].sched_event.set()
                return Response_Master_Alter_Cluster(RESULT_SUCCESS)
        return Response_Master_Alter_Cluster(RESULT_FAILURE,
                                             {RESPONSE_REASON: "Cannot find name slave %s" % req.name})

    @aiomas.expose
    async def ReloadKey(self, req: Request_Master_ReloadKey) -> Response_Master_ReloadKey:
        owner_uid = -1
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
        if owner_uid != 0:
            return Response_Master_ReloadKey(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})

        self.key_list = loadKeys()
        return Response_Master_ReloadKey(RESULT_SUCCESS)

    @aiomas.expose
    async def AlterUserLimit(self, req: Request_Master_Alter_UserLimit) -> Response_Master_Alter_UserLimit:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
        if owner_uid != 0:
            return Response_Master_Alter_UserLimit(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})
        LimitRemove = False
        if req.max_cores == -1:  LimitRemove = True

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_Alter_UserLimit(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})
        qqueue = self.taskqueue_dict[req.queue_name]
        for userlimit in qqueue.userlimit_list:
            if userlimit.username == req.username:
                if LimitRemove is True:
                    print("remove userlimit...")
                    qqueue.userlimit_list.remove(userlimit)
                    qqueue.sched_event.set()
                    return Response_Master_Alter_UserLimit(RESULT_SUCCESS, {RESPONSE_INFO: "UserLimit Removed"})

                userlimit.max_cores = req.max_cores
                userlimit.forbid_sub = req.forbid_sub
                qqueue.sched_event.set()
                return Response_Master_Alter_UserLimit(RESULT_SUCCESS, {RESPONSE_INFO: "UserLimit changed"})

        userlimit = UserLimit(req.queue_name)
        userlimit.username = req.username
        userlimit.user_id = pwd.getpwnam(userlimit.username)[2]
        userlimit.max_cores = req.max_cores

        core_sum = 0
        for job in qqueue.running_list:
            if job.owner_uid == userlimit.user_id:
                core_sum += job.cores
        userlimit.used_cores = core_sum
        qqueue.userlimit_list.append(userlimit)
        return Response_Master_Alter_UserLimit(RESULT_SUCCESS, {RESPONSE_INFO: "UserLimit Added"})

    @aiomas.expose
    async def AlterPriority(self, req: Request_Master_Alter_Priority) -> Response_Master_Alter_Priority:
        priority = req.priority
        if priority < 0 or priority > 10:
            return Response_Master_Alter_Priority(RESULT_FAILURE,
                                                  {RESPONSE_REASON: "Priority value out of range"})
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]

        if owner_uid == 0:
            check = False
        else:
            check = True

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_Alter_Priority(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})

        job_id = req.job_id
        for job in self.taskqueue_dict[req.queue_name].queue_list:
            if job.job_id == job_id:
                if job.owner_uid != owner_uid and check:
                    return Response_Master_Alter_Priority(
                        RESULT_FAILURE,{RESPONSE_REASON: "Pemission denied (Only change own job's priority)"})
                job.priority = priority
                self.database.change_job_priority(job)
                return Response_Master_Alter_Priority(
                    RESULT_SUCCESS, {RESPONSE_INFO: "Job's priority changed successfully"})

        return Response_Master_Alter_Priority(
            RESULT_FAILURE,{RESPONSE_REASON: "Cannot find job_id = %s in queue list" % job_id})

    @aiomas.expose
    async def AlterBindNode(self, req: Request_Master_Alter_BindNode) -> Response_Master_Alter_BindNode:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break

        if owner_uid == 0:
            check = False
        else:
            check = True

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_Alter_BindNode(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})
        qqueue = self.taskqueue_dict[req.queue_name]
        for job in qqueue.queue_list:
            if job.job_id == req.job_id:
                if job.owner_uid != owner_uid and check:
                    return Response_Master_Alter_BindNode(
                        RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Only change own job's bind node list)"})
                job.bind_node = req.bind_node
                self.database.change_job_bindnode(job)
                qqueue.sched_event.set()
                return Response_Master_Alter_BindNode(
                    RESULT_SUCCESS,{RESPONSE_INFO: "Job's bind node list changed successfully"})

        return Response_Master_Alter_BindNode(
            RESULT_FAILURE, {RESPONSE_REASON: "Cannot find job_id = %s in queue list" % req.job_id})

    @aiomas.expose
    async def DelJob(self, req: Request_Master_DelJob) -> Response_Master_DelJob:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break

        if owner_uid == 0:
            check = False
        else:
            check = True

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_DelJob(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s doesn't exist" % req.queue_name})

        qqueue = self.taskqueue_dict[req.queue_name]
        job_id = req.job_id
        for job in qqueue.queue_list:
            if job.job_id == job_id:
                if job.owner_uid != owner_uid and check:
                    return Response_Master_DelJob(
                        RESULT_FAILURE, {RESPONSE_REASON,"Pemission denied (Only delete own job except root)"})
                job.status = STATUS_DELETE
                qqueue.queue_list.remove(job)
                job.finish_time = datetime.now()
                self.database.change_job_complete(job)
                return Response_Master_DelJob(RESULT_SUCCESS, {RESPONSE_INFO: "Job was deleted"})
        for job in qqueue.running_list:
            if job.job_id == job_id:
                if job.owner_uid != owner_uid and check:
                    return Response_Master_DelJob(
                        RESULT_FAILURE, {RESPONSE_REASON, "Pemission denied (Only delete own job except root)"})
                job.status = STATUS_CANCELING
                self.database.change_job_status(job)
                await job.slave.rpc.DelJob(Request_Slave_DelJob(job_id))
                return Response_Master_DelJob(RESULT_SUCCESS, {RESPONSE_INFO: "Job is cancelling"})

        return Response_Master_DelJob(RESULT_FAILURE, {RESPONSE_REASON: "Job id = %s not found" % job_id})

    @aiomas.expose
    async def QueryJobQName(self, req : Request_Master_Query_Queue_Name) -> Response_Master_Query_Queue_Name:
        for qname, qqueue in self.taskqueue_dict.items():
            for job in qqueue.running_list:
                if job.job_id == req.job_id:
                    return Response_Master_Query_Queue_Name(RESULT_SUCCESS,{"QNAME": qname})
            for job in qqueue.queue_list:
                if job.job_id == req.job_id:
                    return Response_Master_Query_Queue_Name(RESULT_SUCCESS, {"QNAME": qname})

        return Response_Master_Query_Queue_Name(RESULT_FAILURE, {RESPONSE_REASON: "Can not find job: %s" % job_id})

    @aiomas.expose
    async def NewQueue(self, req: Request_Master_New_Queue) -> Response_Master_New_Queue:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break
        if owner_uid != 0:
            return Response_Master_New_Queue(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})

        if req.queue_name in self.taskqueue_dict:
            return Response_Master_New_Queue(RESULT_FAILURE, {RESPONSE_REASON: "Same name already in list"})

        self.taskqueue_dict[req.queue_name] = TaskQueue(req.queue_name)

        return Response_Master_New_Queue(RESULT_SUCCESS, {RESPONSE_INFO: "New task queue created successfully"})

    @aiomas.expose
    async def CloseQueue(self, req: Request_Master_Close_Queue) -> Response_Master_Close_Queue:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break
        if owner_uid != 0:
            return Response_Master_Close_Queue(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_Close_Queue(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s not in list" % req.queue_name})

        qqname = self.taskqueue_dict[req.queue_name]
        qqname.close_queue = True
        return Response_Master_Close_Queue(RESULT_SUCCESS, {RESPONSE_INFO: "Queue was closed"})

    @aiomas.expose
    async def DelQueue(self, req: Request_Master_Del_Queue) -> Response_Master_Del_Queue:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break
        if owner_uid != 0:
            return Response_Master_New_Queue(RESULT_FAILURE, {RESPONSE_REASON: "Pemission denied (Root Only)"})

        if req.queue_name not in self.taskqueue_dict:
            return Response_Master_New_Queue(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue name %s not in list" % req.queue_name})

        qqname = self.taskqueue_dict[req.queue_name]
        if len(qqname.running_list) + len(qqname.queue_list) > 0:
            return Response_Master_New_Queue(
                RESULT_FAILURE, {RESPONSE_REASON: "Queue is not in empty"})

        qqname.moniter.cancel()
        for slave in qqname.slave_list:
            slave.queue_name = None

        del self.taskqueue_dict[req.queue_name]
        return Response_Master_New_Queue(
            RESULT_SUCCESS, {RESPONSE_INFO: "Queue was removed"})


    @aiomas.expose
    async def RelayPtyOut(self, req : Request_Master_JobPtyOut) -> Response_Master_JobPtyOut:
        logging.info("RelayPtyOut job_id = %s " % req.job_id)
        for task_queue in self.taskqueue_dict.values():
            for job in task_queue.running_list:
                if job.job_id == req.job_id:
                    try:
                        if job.buffer_first:
                            await self.FlushBuffer(job)
                        await job.rpc.JobPtyOut(Request_Client_JobPtyOut(req.job_id,req.data))
                    except:
                        pass
                    return Response_Master_JobPtyOut()

    async def FlushBuffer(self, job : Job):
        if job.buffer_first:
            job.buffer_first = False
            try:
                ret = await job.slave.rpc.pty_get_out_buf_list(Request_Slave_JobPtyOut_List(job.job_id))
                for data in ret.data_list:
                    await job.rpc.JobPtyOut(Request_Client_JobPtyOut(job.job_id, data))
            except:
                pass


    @aiomas.expose
    async def RelayPtyInput(self, req : Request_Master_JobInput) -> Response_Master_JobInput:
        logging.info("RelayPtyInput job_id = %s " % req.job_id)
        for task_queue in self.taskqueue_dict.values():
            for job in task_queue.running_list:
                if job.job_id == req.job_id:
                    try:
                        await job.slave.rpc.pty_job_input(Request_Slave_JobInput(req.job_id,req.data))
                    except:
                        pass
                    return Response_Master_JobInput()

    @aiomas.expose
    async def RelayPtyWINSZ(self, req : Request_Master_WINSZ) -> Response_Master_WINSZ:
        for task_queue in self.taskqueue_dict.values():
            for job in task_queue.running_list:
                if job.job_id == req.job_id:
                    try:
                        await job.slave.rpc.pty_change_winsize(Request_Slave_WINSZ(req.job_id,req.winsize))
                    except:
                        pass
                    return Response_Master_WINSZ()
    @aiomas.expose
    async def RelayPtySignal(self, req : Request_Master_PtySignal) -> Response_Master_PtySingal:
        for task_queue in self.taskqueue_dict.values():
            for job in task_queue.running_list:
                if job.job_id == req.job_id:
                    pass

    @aiomas.expose
    async def AttachJob(self, rpc, req : Request_Master_Attach) -> Response_Master_Attach:
        for item in self.key_list:
            if item.key == req.key:
                username = item.username
                owner_uid, owner_gid = pwd.getpwnam(username)[2:4]
                break

        for task_queue in self.taskqueue_dict.values():
            for job in task_queue.running_list:
                if job.job_id == req.job_id:
                    if job.owner_uid == owner_uid:
                        if job.run_type != INTERACTIVE:
                            return Response_Master_Attach(RESULT_FAILURE,
                                                          "Job id = %s is not interactive" % req.job_id)
                        job.rpc = rpc
                        await job.slave.rpc.pty_change_winsize(Request_Slave_WINSZ(req.job_id, req.winsize))
                        job.buffer_first = True
                        asyncio.ensure_future(self.FlushBuffer(job))
                        return Response_Master_Attach(RESULT_SUCCESS)
                    else:
                        return Response_Master_Attach(RESULT_FAILURE,extra="Pemission denied")
        return Response_Master_Attach(RESULT_FAILURE, "Job id = %s not found" % req.job_id)

    def exit(self):
        self.database.close()
        exit(0)

    def client_connected(self, client):
        logging.debug("connection established with %s:%s" % client.channel.get_extra_info('peername'))
        sock = client.channel.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # set TCP heartbeat
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)

        def client_disconnected(exc):
            logging.debug("connection loss ..")
            for slave in self.slave_list:
                if slave.rpc == client.remote:
                    logging.warning("slave %s : connection loss" % slave.name)
                    self.slave_list.remove(slave)
                    for job in slave.running_list:
                        self.running_list.remove(job)
                        job.status = STATUS_ERROR
                        self.database.change_job_status(job)
                    for key, queue_item in self.taskqueue_dict:
                        queue_item.slave_list.remove(slave)

        client.on_connection_reset(client_disconnected)


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')
    parser = argparse.ArgumentParser(description="Master for cluster dispatch system")
    parser.add_argument("-s", "--master_ip", type=str, default="0.0.0.0", help="Ip address of master server")
    parser.add_argument("-p", "--master_port", type=check_no_negative_integer,
                        default=PORT,help="port of master service")
    args = parser.parse_args()
    master = Master()
    logging.info("start to listen %s:%s ..." % (args.master_ip, args.master_port))
    master = aiomas.run(aiomas.rpc.start_server((args.master_ip, args.master_port), master,
                                                master.client_connected, codec = aiomas.MsgPackBlosc,
                                                extra_serializers=EXTRA_SERIALIZERS))
    aiomas.run(master.wait_closed())


if __name__ == "__main__":
    main()
