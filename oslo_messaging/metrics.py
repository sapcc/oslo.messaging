import logging
import threading
try:
    import queue
except ImportError:
    import Queue as queue

import statsd

LOG = logging.getLogger(__name__)

class Metrics(object):
   def __init__(self, host, port):
      self.stop_event = threading.Event()
      self.queue = queue.Queue()
      self.statsd = statsd.StatsClient(host, port)

   def start(self):
      threading.Thread(target=self.counter, daemon=True).start()

   def stop(self):
      self.stop_event.set()

   def counter(self):
      while not self.stop_event.is_set():
         try:
            incoming = self.queue.get(timeout=1.0, block=True)
         except queue.Empty:
            continue

         try:
            message = incoming['message'].message
            method = message.get('method')
            stat = "oslo.messaging.{}.{}".format(method, incoming['tag'])
            self.statsd.incr(stat)
         except Exception:
            LOG.exception('Exception during message metrics handling')
         finally:
            self.queue.task_done()
