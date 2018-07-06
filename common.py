import os, sys
import asyncio
import json
import hashlib
from  datetime import *
import aiomas
from typing import *
import logging
import os.path

class BiMap:
    def __init__(self):
        self.forward_map = {}
        self.reverse_map = {}

    def __getitem__(self, item):
        return self.forward_map[item]

    def __getattr__(self, item):
        return self.reverse_map[item]


KEY_PATH= os.path.dirname(os.path.realpath(__file__))

PORT = 20010

STATUS_QUEUE     = "STATUS_QUEUE"
STATUS_LANCHING  = "STATUS_LANCHING"
STATUS_RUNNING   = "STATUS_RUNNING"
STATUS_CANCELING = "STATUS_CANCELING"
STATUS_FINISH    = "STATUS_FINISH"
STATUS_DELETE    = "STATUS_DELETE"
STATUS_ERROR     = "STATUS_ERROR"

BATCH = "BATCH"
INTERACTIVE = "INTERACTIVE"


ST = {
    STATUS_RUNNING: "R",
    STATUS_LANCHING: "S",
    STATUS_CANCELING: "C",
    STATUS_QUEUE: "Q",
    STATUS_FINISH: "F",
    STATUS_DELETE: "D",
    STATUS_ERROR: "E"
}

@aiomas.codecs.serializable
class Request_Master_JobPtyOut:
    def __init__(self, job_id : int, data : bytes):
        self.job_id = job_id
        self.data = data

@aiomas.codecs.serializable
class Response_Master_JobPtyOut:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Slave_JobPtyOut_List:
    def __init__(self, job_id : int):
        self.job_id = job_id


@aiomas.codecs.serializable
class Response_Slave_JobPtyOut_List:
    def __init__(self, data_list : List[bytes]):
        self.data_list = data_list




@aiomas.codecs.serializable
class Request_Client_JobPtyOut:
    def __init__(self, job_id : int, data : bytes):
        self.job_id = job_id
        self.data = data

@aiomas.codecs.serializable
class Response_Client_JobPtyOut:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Master_JobInput:
    def __init__(self, job_id : int, data : bytes):
        self.job_id = job_id
        self.data = data

@aiomas.codecs.serializable
class Response_Master_JobInput:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Slave_JobInput:
    def __init__(self,job_id : int, data : bytes):
        self.job_id = job_id
        self.data = data

@aiomas.codecs.serializable
class Response_Slave_JobInput:
    def __init__(self):
        pass


@aiomas.codecs.serializable
class Request_Master_WINSZ:
    def __init__(self,job_id : int, winsize : bytes):
        self.job_id = job_id
        self.winsize = winsize

@aiomas.codecs.serializable
class Response_Master_WINSZ:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Slave_WINSZ:
    def __init__(self, job_id : int, winsize : bytes):
        self.job_id = job_id
        self.winsize = winsize

@aiomas.codecs.serializable
class Response_Slave_WINSZ:
    def __init__(self):
        pass


@aiomas.codecs.serializable
class Request_Client_RunJobComplete:
    def __init__(self, job_id : int):
        self.job_id = job_id

@aiomas.codecs.serializable
class Response_Client_RunJobComplete:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Master_PtySignal:
    def __init__(self, job_id : int, signum : int):
        self.job_id = job_id
        self.signum = signum

@aiomas.codecs.serializable
class Response_Master_PtySingal:
    pass

@aiomas.codecs.serializable
class Request_Slave_PtySignal:
    def __init__(self, job_id : int, signum : int):
        self.job_id = job_id
        self.signum = signum

@aiomas.codecs.serializable
class Response_Slave_PtySignal:
    pass

@aiomas.codecs.serializable
class Request_Master_Attach:
    def __init__(self,job_id: int, key : str, winsize : bytes):
        self.job_id = job_id
        self.key = key
        self.winsize = winsize

@aiomas.codecs.serializable
class Response_Master_Attach:
    def __init__(self, result : str, extra = None):
        self.result = result
        self.extra = extra




@aiomas.codecs.serializable
class Request_Master_SubJob:
    def __init__(self,workdir : str, sgefile : str, job_name : str, cores : int, priority : int,
            bind_node : List[str], key : str, queue_name: str, shell : str, env : Dict[str,str]):
        self.workdir = workdir
        self.sgefile = sgefile
        self.job_name = job_name
        self.cores = cores
        self.priority = priority
        self.bind_node = bind_node
        self.key = key
        self.queue_name = queue_name
        self.shell = shell
        self.env = env

RESULT_FAILURE = "RESULT_FAILURE"
RESULT_SUCCESS = "RESULT_SUCCESS"

RESPONSE_REASON = "RESPONSE_REASON"
RESPONSE_INFO   = "RESPONSE_INFO"
RESPONSE_JOBID  = "RESPONSE_JOBID"

@aiomas.codecs.serializable
class Response_Master_SubJob:
    def __init__(self, result :str, extra = None):
        self.result = result
        self.extra = extra


@aiomas.codecs.serializable
class Request_Master_RunJob:
    def __init__(self,workdir : str, sgefile : str, job_name : str, cores : int, priority : int,
            bind_node : List[str], key : str, queue_name: str, shell : str, env : Dict[str,str]):
        self.workdir = workdir
        self.sgefile = sgefile
        self.job_name = job_name
        self.cores = cores
        self.priority = priority
        self.bind_node = bind_node
        self.key = key
        self.queue_name = queue_name
        self.shell = shell
        self.env = env

@aiomas.codecs.serializable
class Response_Master_RunJob:
    def __init__(self, result :str, extra = None):
        self.result = result
        self.extra = extra


@aiomas.codecs.serializable
class Request_Master_Status:
    def __init__(self, queue_name : str):
        self.queue_name = queue_name


@aiomas.codecs.serializable
class Request_Master_MachineHours:
    pass

@aiomas.codecs.serializable
class Response_Master_MachineHours:
    def __init__(self, info : List[(int,float)]):
        self.info = info

@aiomas.codecs.serializable
class Request_Master_History:
    def __init__(self, queue_name: str, owner_uid : int, days : int, job_id : int):
        self.queue_name = queue_name
        self.owner_uid = owner_uid
        self.days = days
        self.job_id = job_id

@aiomas.codecs.serializable
class History_Item_Info:
    def __init__(self, job_id : int, sgefile : str, job_name : str, workdir : str, status : str, cores : int,
                  priority : int, owner_uid : int, finish_time : str, submit_time : str, start_time : str,
                 queue_name : str, shell: str):
        self.job_id = job_id
        self.sgefile = sgefile
        self.job_name = job_name
        self.workdir = workdir
        self.status = status
        self.cores = cores
        self.priority = priority
        self.owner_uid = owner_uid
        self.finish_time = finish_time
        self.submit_time = submit_time
        self.start_time = start_time
        self.queue_name = queue_name
        self.shell = shell

@aiomas.codecs.serializable
class Response_Master_History:
    def __init__(self,result : str, item_info_list : List[History_Item_Info] = None , extra= None):
        self.result = result
        self.item_info_list = item_info_list
        self.extra = extra

@aiomas.codecs.serializable
class Status_Queue_List_Info:
    def __init__(self, job_id : int, sgefile : str, job_name : str, workdir : str , status : str, cores : int,
            owner_uid : int, submit_time : str, bind_node : List[str], priority :int):
        self.job_id = job_id
        self.sgefile = sgefile
        self.job_name = job_name
        self.workdir = workdir
        self.status = status
        self.cores = cores
        self.owner_uid = owner_uid
        self.submit_time = submit_time
        self.bind_node = bind_node
        self.priority = priority

@aiomas.codecs.serializable
class Status_Running_List_Info:
    def __init__(self, job_id : int, sgefile : str, job_name : str, workdir : str, status : str, cores :int,
            slave : str, owner_uid : str, submit_time : str, start_time : str, priority : int):
        self.job_id = job_id
        self.sgefile = sgefile
        self.job_name = job_name
        self.workdir = workdir
        self.status = status
        self.cores = cores
        self.slave = slave
        self.owner_uid = owner_uid
        self.submit_time = submit_time
        self.start_time = start_time
        self.priority = priority

@aiomas.codecs.serializable
class Response_Master_Status:
    def __init__(self, result : str, queue_list : List[Status_Queue_List_Info] = None,
            running_list : List[Status_Running_List_Info] = None, extra = None):
        self.result = result
        self.queue_list = queue_list
        self.running_list = running_list
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_Cluster:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Cluster_Slave_Info:
    def __init__(self, name : str, total_cores : int, used_cores : int, queue_name: str):
        self.name = name
        self.total_cores = total_cores
        self.used_cores = used_cores
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Cluster:
    def __init__(self, result : str, slave_infos : List[Cluster_Slave_Info], extra = None):
        self.result = result
        self.slave_infos = slave_infos
        self.extra = extra


@aiomas.codecs.serializable
class Request_Master_UserLimit:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name

@aiomas.codecs.serializable
class UserLimit_User_Info:
    def __init__(self, username : str, used_cores : int, max_cores : int, forbid_sub : bool):
        self.username = username
        self.used_cores = used_cores
        self.max_cores = max_cores
        self.forbid_sub = forbid_sub

@aiomas.codecs.serializable
class Response_Master_UserLimit:
    def __init__(self, result : str, userlimit_infos : List[UserLimit_User_Info] = None, extra = None):
        self.result = result
        self.userlimit_infos = userlimit_infos
        self.extra = extra


@aiomas.codecs.serializable
class Request_Master_Alter_Cluster:
    def __init__(self, key : str, name : str, total_cores : int, queue_name: str):
        self.key = key
        self.name = name
        self.total_cores = total_cores
        # self.used_cores = used_cores
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Alter_Cluster:
    def __init__(self, result : str, extra = None):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_Alter_UserLimit:
    def __init__(self, key : str, maxcore : int, username : str, forbid_sub : bool, queue_name: str):
        self.key = key
        self.maxcore = maxcore
        self.username = username
        self.forbid_sub = forbid_sub
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Alter_UserLimit:
    def __init_(self, result : str, extra = None ):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_DelJob:
    def __init__(self, key : str, job_id : int, queue_name: str):
        self.key = key
        self.job_id = job_id
        self.queue_name =queue_name

@aiomas.codecs.serializable
class Response_Master_DelJob:
    def __init__(self, result : str, extra = None):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_Alter_Priority:
    def __init__(self, key : str, job_id : int, priority : int, queue_name: str):
        self.key = key
        self.job_id = job_id
        self.priority = priority
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Alter_Priority:
    def __init__(self, result : str, extra = None ):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_ReloadKey:
    def __init__(self, key : str):
        self.key = key


@aiomas.codecs.serializable
class Response_Master_ReloadKey:
    def __init__(self, result, extra = None):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_Alter_BindNode:
    def __init__(self, key : str, job_id : int, bind_node : List[str], queue_name: str):
        self.key = key
        self.job_id = job_id
        self.bind_node = bind_node
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Alter_BindNode:
    def __init__(self, result, extra = None):
        self.result = result
        self.extra = extra


@aiomas.codecs.serializable
class Master_SlaveRegister_Info:
    def __init__(self, job_id: int, workdir: str, sgefile: str, job_name: str, status: str,
                 cores: int, owner_uid: int, owner_gid: int, submit_time: str, start_time: str,
                 priority: int, bind_node: List[str], queue_name: str, shell: str,
                 env : Dict[str,str], run_type : str):
        self.job_id = job_id
        self.workdir = workdir
        self.sgefile = sgefile
        self.job_name = job_name
        self.status = status
        self.cores = cores
        self.owner_uid = owner_uid
        self.owner_gid = owner_gid
        self.submit_time = submit_time
        self.start_time = start_time
        self.priority = priority
        self.bind_node = bind_node
        self.queue_name = queue_name
        self.shell = shell
        self.env = env
        self.run_type = run_type


@aiomas.codecs.serializable
class Request_Master_SlaveRegister:
    def __init__(self, name : str, total_cores : int,
                 queue_name: str, job_info: List[Master_SlaveRegister_Info]):
        self.name = name
        self.total_cores = total_cores
        self.queue_name = queue_name
        self.job_info = job_info

@aiomas.codecs.serializable
class Response_Master_SlaveRegister:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Request_Master_JobComplete:
    def __init__(self, job_id : int, finish_time : float, queue_name : str):
        self.job_id = job_id
        self.finish_time = finish_time
        self.queue_name = queue_name


@aiomas.codecs.serializable
class Response_Master_JobComplete:
    pass

@aiomas.codecs.serializable
class Request_Master_JobStarted:
    def __init__(self, job_id : int, start_time : str, queue_name: str):
        self.job_id = job_id
        self.start_time = start_time
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_JobStarted:
    pass

@aiomas.codecs.serializable
class Request_Master_Query_Queue_Name:
    def __init__(self, job_id: int):
        self.job_id = job_id

@aiomas.codecs.serializable
class Response_Master_Query_Queue_Name:
    def __init__(self,result : str, extra):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_QueueList:
    def __init__(self):
        pass

@aiomas.codecs.serializable
class Response_Master_QueueList:
    def __init__(self, queue_list: List[str]):
        self.queue_list = queue_list


@aiomas.codecs.serializable
class Request_Master_New_Queue:
    def __init__(self, key: str, queue_name: str):
        self.key = key
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_New_Queue:
    def __init__(self, result, extra):
        self.result = result
        self.extra = extra


@aiomas.codecs.serializable
class Request_Master_Del_Queue:
    def __init__(self, key: str, queue_name: str):
        self.key = key
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Del_Queue:
    def __init__(self, result, extra):
        self.result = result
        self.extra = extra

@aiomas.codecs.serializable
class Request_Master_Close_Queue:
    def __init__(self, key: str, queue_name: str):
        self.key = key
        self.queue_name = queue_name

@aiomas.codecs.serializable
class Response_Master_Close_Queue:
    def __init__(self, result, extra):
        self.result = result
        self.extra = extra


@aiomas.codecs.serializable
class Request_Slave_SubJob:
    def __init__(self, job_id : int, workdir : str, sgefile : str, job_name : str, status : str,
            cores : int, owner_uid : int, owner_gid : int, submit_time : str, priority: int,
                 bind_node: List[str], queue_name: str, shell : str, env : Dict[str,str],
                 run_type : str, winsize : bytes = None, tty_attr = None):
        self.job_id = job_id
        self.workdir = workdir
        self.sgefile = sgefile
        self.job_name = job_name
        self.status = status
        self.cores = cores
        self.owner_uid = owner_uid
        self.owner_gid = owner_gid
        self.submit_time = submit_time
        self.priority = priority
        self.bind_node = bind_node
        self.queue_name =queue_name
        self.shell = shell
        self.env = env
        self.run_type = run_type
        self.winsize = winsize
        self.tty_attr = tty_attr

@aiomas.codecs.serializable
class Response_Slave_SubJob:
    pass

@aiomas.codecs.serializable
class Request_Slave_DelJob:
    def __init__(self, job_id : int ):
        self.job_id = job_id

@aiomas.codecs.serializable
class Response_Slave_DelJob:
    pass



EXTRA_SERIALIZERS = [
        Request_Master_JobPtyOut.__serializer__,
        Response_Master_JobPtyOut.__serializer__,
        Request_Client_JobPtyOut.__serializer__,
        Response_Client_JobPtyOut.__serializer__,
        Request_Slave_JobPtyOut_List.__serializer__,
        Response_Slave_JobPtyOut_List.__serializer__,
        Request_Master_JobInput.__serializer__,
        Response_Master_JobInput.__serializer__,
        Request_Slave_JobInput.__serializer__,
        Response_Slave_JobInput.__serializer__,
        Request_Master_WINSZ.__serializer__,
        Response_Master_WINSZ.__serializer__,
        Request_Slave_WINSZ.__serializer__,
        Response_Slave_WINSZ.__serializer__,
        Request_Client_RunJobComplete.__serializer__,
        Response_Client_RunJobComplete.__serializer__,
        Request_Master_PtySignal.__serializer__,
        Response_Master_PtySingal.__serializer__,
        Request_Slave_PtySignal.__serializer__,
        Response_Slave_PtySignal.__serializer__,
        Request_Master_Attach.__serializer__,
        Response_Master_Attach.__serializer__,
        Request_Master_RunJob.__serializer__,
        Response_Master_RunJob.__serializer__,
        Request_Master_SubJob.__serializer__ ,
        Response_Master_SubJob.__serializer__ ,
        Request_Master_Status.__serializer__,
        Status_Queue_List_Info.__serializer__,
        Status_Running_List_Info.__serializer__,
        Response_Master_Status.__serializer__ ,
        Request_Master_Cluster.__serializer__,
        Cluster_Slave_Info.__serializer__,
        Response_Master_Cluster.__serializer__,
        Request_Master_QueueList.__serializer__,
        Response_Master_QueueList.__serializer__,
        Request_Master_UserLimit.__serializer__,
        UserLimit_User_Info.__serializer__,
        Response_Master_UserLimit.__serializer__,
        Request_Master_Alter_Cluster.__serializer__,
        Response_Master_Alter_Cluster.__serializer__,
        Request_Master_Alter_UserLimit.__serializer__,
        Response_Master_Alter_UserLimit.__serializer__,
        Request_Master_DelJob.__serializer__,
        Response_Master_DelJob.__serializer__,
        Request_Master_Alter_Priority.__serializer__,
        Response_Master_Alter_Priority.__serializer__,
        Master_SlaveRegister_Info.__serializer__,
        Request_Master_SlaveRegister.__serializer__,
        Response_Master_SlaveRegister.__serializer__,
        Request_Master_JobComplete.__serializer__,
        Response_Master_JobComplete.__serializer__,
        Request_Master_JobStarted.__serializer__,
        Response_Master_JobStarted.__serializer__,
        Request_Master_ReloadKey.__serializer__,
        Response_Master_ReloadKey.__serializer__,
        Request_Master_Alter_BindNode.__serializer__,
        Response_Master_Alter_BindNode.__serializer__,
        Request_Master_MachineHours.__serializer__,
        Response_Master_MachineHours.__serializer__,
        Request_Master_History.__serializer__,
        History_Item_Info.__serializer__,
        Response_Master_History.__serializer__,
        Request_Master_Query_Queue_Name.__serializer__,
        Response_Master_Query_Queue_Name.__serializer__,
        Request_Master_New_Queue.__serializer__,
        Response_Master_New_Queue.__serializer__,
        Request_Master_Del_Queue.__serializer__,
        Response_Master_Del_Queue.__serializer__,
        Request_Slave_SubJob.__serializer__,
        Response_Slave_SubJob.__serializer__,
        Request_Slave_DelJob.__serializer__,
        Response_Slave_DelJob.__serializer__ ]



def check_no_negative_integer(string):
    try:
        value = int(string)
    except TypeError as e:
        raise argparse.ArgumentTypeError(e.args)

    if value < 0:
        msg = "%r is a negative number!" % string
        raise argparse.ArgumentTypeError(msg)
    return value
