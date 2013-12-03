'''
Created on Dec 1, 2013

@author: Haichuan Wang

'''

import Queue
import threading
from threading import Lock
from sets import Set
import time

WRITE_OP = 0
DELETE_OP = 1
QUIT_OP = 2 #the last task

class CloudFileOpThread(threading.Thread):
    
    def __init__(self, cloudfs):
        super(CloudFileOpThread, self).__init__()
        self.cloud = cloudfs
        self.queue = FileOpQueue()
    
    def run(self):
        print '[CFS][FileOpThread]Starts successfully!'
        #get a task
        task = self.queue.get_task()
        while task.op != QUIT_OP:
            self.execute(task)
            task = self.queue.get_task()
        print '[CFS][FileOpThread]Quit successfully!'
        
    def execute(self, task):
        print '[CFS][FileOpThread]try execute task %s %d' %(task.filename, task.op)
        if task.op == WRITE_OP:
            with open(task.filename, 'r') as f:
                self.cloud.write(task.filename, f.read())

        if task.op == DELETE_OP:
            self.cloud.delete(task.filename)
        
    def add_task(self, filename, op):
        # print '[CFS][FileOpThread]try addd task %s %d' %(filename, op)
        self.queue.add_task(filename, op)

class FileOpTask:
    
    def __init__(self, filename, op):
        self.filename = filename
        self.valid = True
        self.op = op
        
class FileOpQueue:
    
    def __init__(self):
        self.queue = Queue.Queue()
        self.rwlock = Lock()
        self.filemap = {}

    def add_task(self, filename, op):
        new_task = FileOpTask(filename, op)
        'Also put into a dict'
        with self.rwlock:
            if filename in self.filemap.keys():
                old_tasks = self.filemap[filename]
                for task in old_tasks:
                    task.valid = False #invalid the task
                old_tasks.clear() #clean all old tasks
                old_tasks.add(new_task)
            else:
                self.filemap[filename] = Set([new_task])
        self.queue.put(new_task, block = True)
        
    def get_task(self):
        while True:
            cur_task = self.queue.get(block=True)
            #remove the task from the filename map
            with self.rwlock:
                old_tasks = self.filemap[cur_task.filename]
                old_tasks.remove(cur_task)
            if cur_task.valid:
                return cur_task
            #else:
                # print 'task of %s is invliad, try anotherone' % cur_task.filename
            
if __name__ == "__main__":
    file_op_queue = FileOpQueue()
    file_op_queue.add_task('/tmp/f1', WRITE_OP)
    file_op_queue.add_task('/tmp/f2', WRITE_OP)
    file_op_queue.add_task('/tmp/f1', WRITE_OP)
    file_op_queue.add_task('/tmp/f1', WRITE_OP)
    file_op_queue.add_task('/tmp/f1', WRITE_OP)
    file_op_queue.add_task('/tmp/f1', WRITE_OP)
    
    t = file_op_queue.get_task()
    print t.filename
    t = file_op_queue.get_task()
    print t.filename
    