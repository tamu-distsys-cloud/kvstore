import porcupine
import logging
from collections import defaultdict

from porcupine.model import Model

class KvInput:
    def __init__(self, op, key, value=0):
        self.op = op  # 0 => get, 1 => put, 2 => append
        self.key = key
        self.value = value

class KvOutput:
    def __init__(self, value):
        self.value = value

def partition(history):
    m = defaultdict(list)
    for v in history:
        key = v.input.key
        m[key].append(v)

    keys = sorted(m.keys())
    ret = [m[k] for k in keys]
    return ret

def init():
    # Note: we are modeling a single key's value here;
    # we're partitioning by key, so this is okay
    return ""

def step(state, input, output):
    inp = input
    out = output
    st = state
    if inp.op == 0:
        # get
        return out.value == st, state
    elif inp.op == 1:
        # put
        return True, inp.value
    elif inp.op == 2:
        # append
        return True, (st + inp.value)
    else:
        # append with return value
        return out.value == st, (st + inp.value)

def describe_operation(input, output):
    inp = input
    out = output
    if inp.op == 0:
        return f"get('{inp.key}') -> '{out.value}'"
    elif inp.op == 1:
        return f"put('{inp.key}', '{inp.value}')"
    elif inp.op == 2:
        return f"append('{inp.key}', '{inp.value}')"
    else:
        return "<invalid>"

KvModel = Model(
    partition=partition,
    init=init,
    step=step,
    describe_operation=describe_operation
)
