import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, server: ClientEnd):
        self.server = server
        # You will have to modify this class.

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server.call("KVServer.Get", args)
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments. and reply must be passed as a pointer.
    def get(self, key: str) -> str:
        # You will have to modify this function.
        return ""

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.server.call("KVServer."+op, args)
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments. and reply must be passed as a pointer.
    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.
        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
