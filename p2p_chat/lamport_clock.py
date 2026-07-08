import threading

class LamportClock:
    def __init__(self):
        self.clock = 0
        self._lock = threading.Lock()  # Protects the clock value from concurrent access by multiple threads

    def send_event(self):
        # Called before sending a message: increments the clock and returns the new value
        with self._lock:
            self.clock += 1
            return self.clock

    def receive_event(self, received_time):
        # Called when receiving a message: clock becomes max(local, received) + 1,
        # which is the core Lamport clock rule for keeping causal order across peers
        with self._lock:
            self.clock = max(
                self.clock,
                received_time
            ) + 1
            return self.clock

    def get_time(self):
        # Returns the current logical time without modifying it
        with self._lock:
            return self.clock