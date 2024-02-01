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
            try:
               data = json.loads(msg)
            except:
               print(f'error parsing JSON: {msg}')
            else:
               payload = self.get_influx_payload(data)
               res = None
               if payload:
                  try:
                     res = requests.post(self.url, auth=(self.username, self.password), data=payload, verify=False)
                  except:
                     self.cache.put(msg)
                  else:
                     if res.ok == False:
                        self.cache.put(msg)

                     # check Influx line protocol errors (400: bad request)
                     if res.status_code == 400:
                        print(f'{threading.current_thread().name}: HTTP error (400): {res.text}')
   
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
                  if v == 'xadc':
                     v = 'xadc_sensor'
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

