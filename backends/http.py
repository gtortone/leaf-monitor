import threading
import time
import json
import requests
requests.packages.urllib3.disable_warnings()

from queue import Queue, Empty

class HttpThread(threading.Thread):
   def __init__(self, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)
         
      self.name = "HttpThread"
      self.queue = Queue()
      self.hostname = kwargs['hostname']
      self.url = kwargs['url']
      self.session = requests.Session()
      self.username = kwargs.get('username', None)
      self.password = kwargs.get('password', None)
      self.payloads = []
      self.httperror = False
      self.daemon = True
   
   def run(self):
      print(f'{threading.current_thread().name}')

      self.session.auth = (self.username, self.password)
      self.session.verify = False

      while True:
         try:
            msg = self.queue.get_nowait()
         except Empty:
            self.send()
            time.sleep(1)
         else:
            try:
               data = json.loads(msg)
            except:
               print(f'error parsing JSON: {msg}')
            else:
               self.payloads.append(self.get_influx_payload(data))
               self.send()

      print(f'{threading.current_thread().name}: HTTP error (400): {res.text}')

   def send(self):
      #if len(self.payloads) >= 100:
      if len(self.payloads) >= 5:            ########################
         try:
            res = self.session.post(self.url, data='\n'.join(self.payloads[0:5]))         #################
         except Exception as e:
            if self.httperror == False:
               print(f'{time.ctime()}: {e}')
               self.httperror = True
         else:
            if self.httperror == True:
               print(f'{time.ctime()}: HTTP connection recovered')
               self.httperror = False
            if res.ok == True and res.status_code != 400:
               del(self.payloads[0:5])    #######################3
            else:
               print(f'{time.ctime()}: HTTP error {res.text}')
      
   def get_influx_payload(self, data):
      timestamp = None
      measurement = None
      taglist = []
      valuelist = []
      payload = []

      ignore = ['unit', 'bitoffset', 'direction']
      tags = ['type']

      if data['event'] == 'corr':
         for i in range(0,data['nofupsets']):
            taglist.append(f'bitoffset={data["bitoffset"][i]}')
            valuelist.append(f'direction=\"{data["direction"][i]}\"')
            for k,v in data.items():
               if k in ignore:
                  continue
               if k == 'timestamp':
                  timestamp = int(v * 1E9)
               elif k == 'event':
                  measurement = v
               elif k in tags:
                  taglist.append(f'{k}={v}')
               elif k == 'frad':
                  valuelist.append(f'{k}={int(v,16)}')
               else:
                  if type(v) == str:
                     v = f'"{v}"'
                  valuelist.append(f'{k}={v}')

            # sanity check 
            if timestamp and measurement and len(taglist) and len(valuelist):
               payload.append(f'{measurement},' + f'host={self.hostname},' + ",".join(taglist) + ' ' + \
                  ",".join(valuelist) + ' ' + f'{timestamp}')
            else:
               print(f'{threading.current_thread().name}: error get_influx_payload')
         
      else:

         for k,v in data.items():
            if k in ignore:
               continue
            if k == 'timestamp':
               # InfluxDB timestamp in ns
               timestamp = int(v * 1E9)
            elif k == 'event':
               if v == 'xadc':
                  v = 'xadc_sensor'
               measurement = v
            elif k in tags:
               taglist.append(f'{k}={v}')
            else:
               if type(v) == str:
                  v = f'"{v}"'
               valuelist.append(f'{k}={v}')

         # sanity check 
         if timestamp and measurement and len(taglist) and len(valuelist):
            payload.append(f'{measurement},' + f'host={self.hostname},' + ",".join(taglist) + ' ' + \
               ",".join(valuelist) + ' ' + f'{timestamp}')
         else:
            print(f'{threading.current_thread().name}: error get_influx_payload')

      return '\n'.join(payload)

