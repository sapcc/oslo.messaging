import logging
import threading, queue
import statsd

LOG = logging.getLogger(__name__)

class Metrics():
   def __init__(self, host, port):
      self._port = port
      self.queue = queue.Queue()
      self.statsd = statsd.StatsClient(host, port)

   def start(self):
      threading.Thread(target=self.counter, daemon=True).start()

   def counter(self):
      while True:
         event = self.queue.get()
         method = event.message.get('method')

         stat = "oslo.messaging.{}.{}".format(method, event.message.tag)
         self.statsd.incr(stat)