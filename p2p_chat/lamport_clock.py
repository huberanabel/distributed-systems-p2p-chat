class LamportClock:

    def __init__(self):
        self.clock = 0

    def send_event(self):
        self.clock += 1
        return self.clock

    def receive_event(self, received_time):
        self.clock = max(
            self.clock,
            received_time
        ) + 1
        return self.clock

    def get_time(self):
        return self.clock