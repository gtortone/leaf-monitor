#!/usr/bin/env python3

import os
import re
import sys
import threading
import time
import json
import yaml
import socket
import requests
requests.packages.urllib3.disable_warnings()

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
            #print(f'{threading.current_thread().name}, Received {msg}')
            try:
               data = json.loads(msg)
            except:
               print(f'error parsing JSON: {msg}')
            else:
               for k,v in pvdb.items():
                  if pvdb[k]["event"] == data["event"] and data["type"] in pvdb[k]["metric"]:
                     pvdb[k]["value"] = data[pvdb[k]["valattr"]]

class HttpThread(threading.Thread):
   def __init__(self, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)
         
      self.name = "HttpThread"
      self.queue = Queue()
      self.cache = Queue()
      self.hostname = kwargs['hostname']
      self.url = kwargs['url']
      self.username = kwargs.get('username', None)
      self.password = kwargs.get('password', None)
      self.daemon = True
   
   def run(self):
      print(f'{threading.current_thread().name}')
      while True:
         try:
            msg = self.queue.get_nowait()
         except Empty:
            if self.cache.empty() == False:
               msg = self.cache.get_nowait()
            else:
               time.sleep(0.1)
         else:
            #print(f'{threading.current_thread().name}, Received {msg}')
            try:
               data = json.loads(msg)
            except:
               print(f'error parsing JSON: {msg}')
            else:
               payload = self.get_influx_payload(data)
               res = None
               if payload:
                  print(payload)
                  try:
                     res = requests.post(self.url, auth=(self.username, self.password), data=payload, verify=False)
                  except:
                     self.cache.put(msg)
                  else:
                     if res.ok == False:
                       self.cache.put(msg)
                     #else:
                     #   print(f'{threading.current_thread().name}: {msg}')
   
   def get_influx_payload(self, data):
      timestamp = None
      measurement = None
      taglist = []
      valuelist = []

      ignore = ['unit']
      tags = ['type']
      
      for k,v in data.items():
         if k in ignore:
            continue
         if k == 'timestamp':
            # InfluxDB timestamp in ns
            timestamp = int(v * 1E9)
         elif k == 'event':
            if v == 'corr':      # skip 'corr' events (wait for patch)
               return None
            measurement = v
         elif k in tags:
            taglist.append(f'{k}={v}')
         else:
            if type(v) == str:
               v = f'"{v}"'
            valuelist.append(f'{k}={v}')

      # sanity check 
      if timestamp and measurement and len(taglist) and len(valuelist):
         payload = f'{measurement},' + f'host={self.hostname},' + ",".join(taglist) + ' ' + ",".join(valuelist) + ' ' + f'{timestamp}'
      else:
         print(f'{threading.current_thread().name}: error get_influx_payload')

      return payload

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

   # get hostname
   hostname = socket.gethostname().split(".")[0] 

   # read config file to setup and start backend threads

   config = {}
   with open(f"{sys.path[0]}/config.yaml", "r") as stream:
      try:
         config = yaml.safe_load(stream)
      except yaml.YAMLError as e:
         print(e)
         exit(-1)

   threads = []

   for backend in config:

      if 'console' in backend:
         if backend['console'].get('enable', False):
            threads.append(ConsoleThread())

      if 'epics' in backend:
         if backend['epics'].get('enable', False):
            # resolve macro
            prefix = backend['epics'].get('prefix', 'SENS:')
            prefix = re.sub('\$hostname', hostname, prefix.lower()).upper()
            threads.append(EpicsThread(prefix))

      if 'http' in backend:
         if backend['http'].get('enable', False):
            args = {}
            args['hostname'] = hostname
            args['url'] = backend['http'].get('url', None)
            if args['url'] is None:
               print("ERROR: HTTP backend enabled but 'url' parameter is not provided")
               exit(-1)
            args['username'] = backend['http'].get('username', None)
            args['password'] = backend['http'].get('password', None)
            threads.append(HttpThread(kwargs=args))

   if len(threads) == 0:
      print("ERROR: enable at least one backend in config file")
      exit(-1)

   qlist = []

   for t in threads:
      qlist.append(t.queue)

   threads.append(FifoThread(qlist, fifo))

   for t in threads:
      t.start()
      
   for t in threads:
      t.join()
