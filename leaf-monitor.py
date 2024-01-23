#!/usr/bin/env python3

import os
import threading
import time
import json
import yaml

from queue import Queue, Empty
from pcaspy import SimpleServer, Driver
from pvs import pvdb

class FifoThread(threading.Thread):
   def __init__(self, qlist, fifo, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None) 

      self.name = "FifoThread"
      self.qlist = qlist
      self.fifo = fifo
      self.daemon = True

   def run(self):
      print(f'{threading.current_thread().name}')

      while True:
         with open(self.fifo) as fifo:
            while True:
               data = fifo.read()
               if len(data) == 0:
                  break
               for line in data.splitlines():
                  self.publish(line)

   def publish(self, msg):
      for q in qlist:
         q.put(msg)


class ConsoleThread(threading.Thread):
   def __init__(self, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)
         
      self.name = "ConsoleThread"
      self.queue = Queue()
      self.daemon = True

   def run(self):
      print(f'{threading.current_thread().name}')
      while True:
         msg = self.queue.get()
         print(f'{threading.current_thread().name}, Received {msg}')

class EpicsDriver(Driver):
   def __init__(self):
      super(EpicsDriver, self).__init__()

   def read(self, reason):
      if reason in pvdb:
         return pvdb[reason]['value']

   def write(self, reason, value):
      # disable PV write (caput)
      return True 

class EpicsThread(threading.Thread):
   def __init__(self, pvprefix, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)

      self.name = "EpicsThread"
      self.queue = Queue()
      self.daemon = True

      self.server = SimpleServer()
      self.server.createPV(pvprefix, pvdb)
      self.driver = EpicsDriver()

   def run(self):
      print(f'{threading.current_thread().name}')
      while True:
         try:
            msg = self.queue.get_nowait()
         except Empty:
            self.server.process(0.1)
         else:
            print(f'{threading.current_thread().name}, Received {msg}')
            try:
               data = json.loads(msg)
            except:
               print(f'error parsing JSON: {msg}')
            else:
               for k,v in pvdb.items():
                  if pvdb[k]["event"] == data["event"] and data["type"] in pvdb[k]["metric"]:
                     pvdb[k]["value"] = data[pvdb[k]["valattr"]]

if __name__ == '__main__':

   # named FIFO in $IOC_FIFO_PATH
   fifo = "/opt/configio-master/spikes/fifopipe"
   if os.environ.get('IOC_FIFO_PATH'):
      fifo = os.environ.get('IOC_FIFO_PATH')

   try:
      fd = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
   except Exception as e:
      print(e)
      exit(-1)

   # read config file to setup and start backend threads

   config = {}
   with open("config.yaml", "r") as stream:
      try:
         config = yaml.safe_load(stream)
      except yaml.YAMLError as e:
         print(e)
         exit(-1)

   threads = []

   for backend in config:

      if 'epics' in backend:
         if backend['epics'].get('enable', False):
            threads.append(EpicsThread(backend['epics'].get('prefix', 'SENS:')))
            
      if 'console' in backend:
         if backend['console'].get('enable', False):
            threads.append(ConsoleThread())

   if len(threads) == 0:
      print("ERROR: enable at least one backend in config file")
      exit(-1)

   qlist = []

   for t in threads:
      qlist.append(t.queue)

   threads.append(FifoThread(qlist, fifo))

   for t in threads:
      t.start()
      time.sleep(0.1)

   for t in threads:
      t.join()
