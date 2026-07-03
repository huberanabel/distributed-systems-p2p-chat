import json
from dataclasses import dataclass


@dataclass
class Message:
    sender_id: str
    sender_name: str
    text: str
    lamport_time: int

    def to_json(self):
        return json.dumps({
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "text": self.text,
            "lamport_time": self.lamport_time
        })

    @staticmethod
    def from_json(data):
        content = json.loads(data)
        return Message(
            sender_id=content["sender_id"],
            sender_name=content["sender_name"],
            text=content["text"],
            lamport_time=content["lamport_time"]
        )