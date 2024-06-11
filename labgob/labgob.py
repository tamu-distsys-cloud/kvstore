import pickle
import io
import threading
import inspect
import logging
import unicodedata
from typing import List, Dict, Tuple, Any

# Initialize global variables
mu = threading.Lock()
error_count = 0  # for TestCapital
checked = {}

class LabEncoder:
    def __init__(self, w):
        self.pickle = pickle.Pickler(w)

    def encode(self, e):
        check_value(e)
        self.pickle.dump(e)

class LabDecoder:
    def __init__(self, r):
        self.pickle = pickle.Unpickler(r)

    def decode(self):
        e = self.pickle.load()
        check_value(e)
        check_default(e)
        return e

def check_value(value):
    check_type(type(value))

def check_type(t):
    global checked, error_count
    if t in checked:
        return
    checked[t] = True

    if isinstance(t, type) and not t.__name__[0].isupper():
        print(f"labgob error: lower-case field {t.__name__} in RPC or persist/snapshot will break your Raft")
        error_count += 1

def check_default(value):
    if value is None:
        return
    check_default1(value, 1, "")

def check_default1(value, depth, name):
    global error_count
    if depth > 3:
        return

    t = type(value)
    if isinstance(value, dict):
        for key, val in value.items():
            name1 = f"{name}.{key}" if name else key
            check_default1(val, depth + 1, name1)
    elif isinstance(value, list):
        for i, val in enumerate(value):
            name1 = f"{name}[{i}]" if name else f"[{i}]"
            check_default1(val, depth + 1, name1)
    elif isinstance(value, tuple):
        for i, val in enumerate(value):
            name1 = f"{name}({i})" if name else f"({i})"
            check_default1(val, depth + 1, name1)
    elif hasattr(value, '__dict__'):
        for attr, val in value.__dict__.items():
            name1 = f"{name}.{attr}" if name else attr
            check_default1(val, depth + 1, name1)
    else:
        if value != type(value)():
            if error_count < 1:
                what = name or t.__name__
                print(f"labgob warning: Decoding into a non-default variable/field {what} may not work")
            error_count += 1

