#!/usr/bin/env python3

import os
import re
import sys
import threading
import json
import time
import yaml
import socket

from queue import Queue, Empty
from pvs import pvdb

from backends.console import ConsoleThread
from backends.epics import EpicsThread
from backends.http import HttpThread

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

if __name__ == '__main__':

   # named FIFO in $IOC_FIFO_PATH
   fifo = "/opt/configio-master/spikes/fifopipe"
   if os.environ.get('IOC_FIFO_PATH'):
      fifo = os.environ.get('IOC_FIFO_PATH')

   # wait for FIFO file
   fifoerror = False
   while True:
      try:
         fd = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
      except Exception as e:
         if fifoerror == False:
            print(f'file {fifo} not available - waiting for it...')
            fifoerror = True
         time.sleep(1)
      else:
         fifoerror = False
         print(f'file {fifo} ready')
         break;

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
            fpgatype = backend['epics'].get('fpgatype', None)
            if fpgatype is None:
               print("ERROR: 'fpgatype' for EPICS backend is not provided")
               exit(-1)
            if pvdb.get(fpgatype, None) is None:
               print(f"ERROR: {fpgatype} not found for EPICS backend")
               exit(-1)
            # resolve macro
            prefix = backend['epics'].get('prefix', 'SENS:')
            prefix = re.sub('\$hostname', hostname, prefix.lower()).upper()
            args = {}
            args['prefix'] = prefix
            args['pvdb'] = pvdb[fpgatype]
            threads.append(EpicsThread(kwargs=args))

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
