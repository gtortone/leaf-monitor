import threading
from queue import Queue, Empty

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

