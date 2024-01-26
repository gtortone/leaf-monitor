import os
import threading
import json

from queue import Queue, Empty
from pcaspy import SimpleServer, Driver
from pvs import pvdb

class EpicsDriver(Driver):
   def __init__(self, db):
      super(EpicsDriver, self).__init__()
      self.db = db

   def read(self, reason):
      if reason in self.db:
         return self.db[reason]['value']

   def write(self, reason, value):
      # disable PV write (caput)
      return True 

class EpicsThread(threading.Thread):
   def __init__(self, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)

      self.name = "EpicsThread"
      self.queue = Queue()
      self.daemon = True
      self.db = kwargs['pvdb']

      self.server = SimpleServer()
      self.server.createPV(kwargs['prefix'], self.db)
      self.driver = EpicsDriver(self.db)

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
               for k,v in self.db.items():
                  if self.db[k]["event"] == data["event"] and data["type"] in self.db[k]["metric"]:
                     self.db[k]["value"] = data[self.db[k]["valattr"]]

