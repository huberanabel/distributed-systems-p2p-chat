import json
from dataclasses import dataclass


@dataclass
class Message:
    sender_id: str
    sender_name: str
    text: str
    lamport_time: int  # Logical timestamp from the Lamport clock, used to order chat messages

    def to_json(self):
        # Serializes the message to a JSON string for sending over the socket
        return json.dumps({
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "text": self.text,
            "lamport_time": self.lamport_time
        })

    @staticmethod
    def from_json(data):
        # Deserializes a JSON string received from a peer back into a Message object
        content = json.loads(data)
        return Message(
            sender_id=content["sender_id"],
            sender_name=content["sender_name"],
            text=content["text"],
            lamport_time=content["lamport_time"]
        )