import json
import threading
from queue import Queue, Empty

class ConsoleThread(threading.Thread):
   def __init__(self, events, kwargs=None):
      threading.Thread.__init__(self, args=(), kwargs=None)
         
      self.name = "ConsoleThread"
      self.queue = Queue()
      self.daemon = True
      self.events = events

   def run(self):
      print(f'{threading.current_thread().name}')
      while True:
         msg = self.queue.get()
         data = json.loads(msg)
         if self.events is None or data.get('event', None) in self.events:
            print(f'{threading.current_thread().name}, Received {msg}')

