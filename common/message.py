"""
Message protocol for the distributed chat system.
All messages are serialized as JSON with a standard envelope.
"""

import json
import time
import uuid


class Message:
    """Standard message envelope for all system communication."""

    def __init__(self, msg_type, sender_id, payload=None, msg_id=None,
                 timestamp=None, sequence_num=None, vector_clock=None):
        self.msg_id = msg_id or str(uuid.uuid4())[:8]
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.payload = payload or {}
        self.timestamp = timestamp or time.time()
        self.sequence_num = sequence_num
        self.vector_clock = vector_clock or {}

    def serialize(self):
        return json.dumps({
            "msg_id": self.msg_id,
            "msg_type": self.msg_type,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "sequence_num": self.sequence_num,
            "vector_clock": self.vector_clock,
        }).encode("utf-8")

    @classmethod
    def deserialize(cls, data):
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        obj = json.loads(data)
        return cls(
            msg_type=obj["msg_type"],
            sender_id=obj["sender_id"],
            payload=obj.get("payload", {}),
            msg_id=obj.get("msg_id"),
            timestamp=obj.get("timestamp"),
            sequence_num=obj.get("sequence_num"),
            vector_clock=obj.get("vector_clock", {}),
        )

    def __repr__(self):
        return (f"Message(type={self.msg_type}, sender={self.sender_id}, "
                f"seq={self.sequence_num}, id={self.msg_id})")
