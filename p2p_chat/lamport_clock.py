import threading

class LamportClock:
    def __init__(self):
        self.clock = 0
        self._lock = threading.Lock()

    def send_event(self):
        with self._lock:
            self.clock += 1
            return self.clock

    def receive_event(self, received_time):
        with self._lock:
            self.clock = max(
                self.clock,
                received_time
            ) + 1
            return self.clock

    def get_time(self):
        with self._lock:
            return self.clock