import logging
import random
import time
import threading
from typing import Any, List, Tuple
import unittest
from concurrent.futures import ThreadPoolExecutor
import queue
import gc
import psutil

from porcupine.model import Operation
from porcupine.porcupine import check_operations_verbose
from models.kv import KvInput, KvOutput, KvModel
from config import make_config

linearizability_check_timeout = 1  # in seconds
MiB = 1024 * 1024

class OpLog:
    def __init__(self):
        self.operations = []
        self.lock = threading.Lock()

    def append(self, op: Operation):
        with self.lock:
            self.operations.append(op)

    def read(self) -> List[Operation]:
        with self.lock:
            return list(self.operations)

# to make sure timestamps use the monotonic clock, we measure time relative to t0
t0 = time.monotonic()

# get/put/putappend that keep counts
def get(cfg, ck, key: str, log: OpLog, cli: int) -> str:
    start = int((time.monotonic() - t0) * 1e9)
    v = ck.get(key)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=0, key=key),
            output=KvOutput(value=v),
            call_time=start,
            response_time=end,
            client_id=cli
        ))
    return v

def put(cfg, ck, key: str, value: str, log: OpLog, cli: int):
    start = int((time.monotonic() - t0) * 1e9)
    ck.put(key, value)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=1, key=key, value=value),
            output=KvOutput(),
            call_time=start,
            response_time=end,
            client_id=cli
        ))

def append(cfg, ck, key: str, value: str, log: OpLog, cli: int) -> str:
    start = int((time.monotonic() - t0) * 1e9)
    last = ck.append(key, value)
    end = int((time.monotonic() - t0) * 1e9)
    cfg.op()
    if log:
        log.append(Operation(
            input=KvInput(op=3, key=key, value=value),
            output=KvOutput(value=last),
            call_time=start,
            response_time=end,
            client_id=cli
        ))
    return last

# a client runs the function f and then signals it is done
def run_client(t: unittest.TestCase, cfg, me: int, ca, fn):
    ok = False
    try:
        ck = cfg.make_client()
        fn(me, ck, t)
        ok = True
    finally:
        ca.put(ok)
        cfg.delete_client(ck)

# spawn ncli clients and wait until they are all done
def spawn_clients_and_wait(t: unittest.TestCase, cfg, ncli: int, fn):
    with ThreadPoolExecutor(max_workers=ncli) as executor:
        futures = [executor.submit(run_client, t, cfg, cli, queue.Queue(), fn) for cli in range(ncli)]
        for future in futures:
            if not future.result():
                t.fail("failure")

# predict effect of append(k, val) if old value is prev
def next_value(prev: str, val: str) -> str:
    return prev + val

# check that for a specific client all known appends are present in a value,
# and in order
def check_clnt_appends(t: unittest.TestCase, clnt: int, v: str, count: int):
    lastoff = -1
    for j in range(count):
        wanted = f"x {clnt} {j} y"
        off = v.find(wanted)
        if off < 0:
            t.fail(f"{clnt} missing element {wanted} in append result {v}")
        off1 = v.rfind(wanted)
        if off1 != off:
            t.fail(f"duplicate element {wanted} in append result")
        if off <= lastoff:
            t.fail(f"wrong order for element {wanted} in append result")
        lastoff = off

# check that all known appends are present in a value,
# and are in order for each concurrent client
def check_concurrent_appends(t: unittest.TestCase, v: str, counts: List[int]):
    nclients = len(counts)
    for i in range(nclients):
        lastoff = -1
        for j in range(counts[i]):
            wanted = f"x {i} {j} y"
            off = v.find(wanted)
            if off < 0:
                t.fail(f"{i} missing element {wanted} in append result {v}")
            off1 = v.rfind(wanted)
            if off1 != off:
                t.fail(f"duplicate element {wanted} in append result")
            if off <= lastoff:
                t.fail(f"wrong order for element {wanted} in append result")
            lastoff = off

# is ov in nv?
def in_history(ov: str, nv: str) -> bool:
    return nv.find(ov) != -1

def rand_value(n: int) -> str:
    letter_bytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join(random.choice(letter_bytes) for _ in range(n))

# Basic test is as follows: one or more clients submitting Append/Get
# operations to the server for some period of time.  After the period
# is over, test checks that all appended values are present and in
# order for a particular key.  If unreliable is set, RPCs may fail.
def generic_test(t: unittest.TestCase, nclients: int, unreliable: bool, randomkeys: bool):
    NITER = 3
    TIME = 1  # in seconds

    title = "Test: "
    if unreliable:
        title += "unreliable net, "
    if randomkeys:
        title += "random keys, "
    if nclients > 1:
        title += "many clients"
    else:
        title += "one client"

    cfg = make_config(t, unreliable)
    try:
        cfg.begin(title)
        op_log = OpLog()

        ck = cfg.make_client()

        done_clients = threading.Event()
        clnts = [queue.Queue() for _ in range(nclients)]

        def client_task(cli, myck, t):
            j = 0
            try:
                last = ""  # only used when not randomkeys
                if not randomkeys:
                    put(cfg, myck, str(cli), last, op_log, cli)
                while not done_clients.is_set():
                    if randomkeys:
                        key = str(random.randint(0, nclients - 1))
                    else:
                        key = str(cli)
                    nv = f"x {cli} {j} y"
                    if random.randint(0, 1000) < 500:
                        l = append(cfg, myck, key, nv, op_log, cli)
                        if not randomkeys:
                            if j > 0:
                                o = f"x {cli} {j-1} y"
                                if not in_history(o, l):
                                    t.fail(f"error: old {o} not in return\n{l}\n")
                            if in_history(nv, l):
                                t.fail(f"error: new value {nv} in returned values\n{l}\n")
                            last = next_value(last, nv)
                        j += 1
                    elif randomkeys and random.randint(0, 1000) < 100:
                        put(cfg, myck, key, nv, op_log, cli)
                        j += 1
                    else:
                        v = get(cfg, myck, key, op_log, cli)
                        if not randomkeys and v != last:
                            t.fail(f"get wrong value, key {key}, wanted:\n{last}\n, got\n{v}\n")
            finally:
                clnts[cli].put(j)

        for i in range(NITER):
            done_clients.clear()
            with ThreadPoolExecutor(max_workers=nclients) as executor:
                futures = [executor.submit(client_task, cli, cfg.make_client(), t) for cli in range(nclients)]
                time.sleep(TIME)
                done_clients.set()
                for cli in range(nclients):
                    j = clnts[cli].get()
                    key = str(cli)
                    v = get(cfg, ck, key, op_log, 0)
                    if not randomkeys:
                        check_clnt_appends(t, cli, v, j)

        res, info = check_operations_verbose(KvModel, op_log.read(), linearizability_check_timeout)
        if res == "Illegal":
            t.fail("history is not linearizable")
        elif res == "Unknown":
            print("info: linearizability check timed out, assuming history is ok")

    finally:
        cfg.cleanup()
        cfg.end()

# Test one client
class TestBasic(unittest.TestCase):
    def test_basic2(self):
        generic_test(self, 1, False, False)

# Test many clients
class TestConcurrent2(unittest.TestCase):
    def test_concurrent2(self):
        generic_test(self, 5, False, False)

# Test many clients, with unreliable network
class TestConcurrentReliable2(unittest.TestCase):
    def test_concurrent_unreliable2(self):
        generic_test(self, 5, True, False)

# Test: unreliable net, many clients, one key
class TestUnreliableOneKey2(unittest.TestCase):
    def test_unreliable_one_key2(self):
        cfg = make_config(self, True)
        ck = cfg.make_client()

        cfg.begin("Test: concurrent append to same key, unreliable")

        put(cfg, ck, "k", "", None, -1)

        nclient = 5
        upto = 10

        def client_func(me, myck, t):
            n = 0
            while n < upto:
                nv = f"x {me} {n} y"
                ov = append(cfg, myck, "k", nv, None, -1)
                n += 1
                if in_history(nv, ov):
                    t.fail(f"error: nv {nv} in returned values\n{ov}\n")

        spawn_clients_and_wait(self, cfg, nclient, client_func)

        counts = [upto for _ in range(nclient)]

        vx = get(cfg, ck, "k", None, -1)
        check_concurrent_appends(self, vx, counts)

        cfg.end()

class TestMemGet2(unittest.TestCase):
    def test_mem_get2(self):
        MEM = 10  # in MiB
        cfg = make_config(self, True)

        ck0 = cfg.make_client()
        ck1 = cfg.make_client()

        cfg.begin("Test: memory use get")

        rd_val = rand_value(MiB * MEM)
        ck0.put("k", rd_val)

        if len(ck0.get("k")) != len(rd_val):
            self.fail(f"error: incorrect len {len(ck0.get('k'))}\n")

        if len(ck1.get("k")) != len(rd_val):
            self.fail(f"error: incorrect len {len(ck1.get('k'))}\n")

        ck0.put("k", "0")

        gc.collect()
        st = psutil.virtual_memory()

        m = st.used / MiB
        if m >= MEM:
            self.fail(f"error: server using too much memory {m}\n")

        cfg.end()

class TestMemPut2(unittest.TestCase):
    def test_mem_put2(self):
        MEM = 10  # in MiB
        cfg = make_config(self, False)

        cfg.begin("Test: memory use put")

        ck0 = cfg.make_client()
        ck1 = cfg.make_client()

        rd_val = rand_value(MiB * MEM)
        ck0.put("k", rd_val)
        ck1.put("k", "")

        gc.collect()

        st = psutil.virtual_memory()
        m = st.used / MiB
        if m >= MEM:
            self.fail(f"error: server using too much memory {m}\n")
        cfg.end()

class TestMemAppend2(unittest.TestCase):
    def test_mem_append2(self):
        MEM = 10  # in MiB
        cfg = make_config(self, False)

        cfg.begin("Test: memory use append")

        ck0 = cfg.make_client()
        ck1 = cfg.make_client()

        rd_val0 = rand_value(MiB * MEM)
        ck0.append("k", rd_val0)
        rd_val1 = rand_value(MiB * MEM)
        ck1.append("k", rd_val1)

        gc.collect()
        st = psutil.virtual_memory()
        m = st.used / MiB
        if m > 3 * MEM:
            self.fail(f"error: server using too much memory {m}\n")
        cfg.end()

class TestMemPutManyClients(unittest.TestCase):
    def test_mem_put_many_clients(self):
        NCLIENT = 100_000
        MEM = 1000

        cfg = make_config(self, False)
        v = rand_value(MEM)

        cks = [cfg.make_client() for _ in range(NCLIENT)]

        time.sleep(1)

        cfg.begin("Test: memory use many put clients")

        gc.collect()
        st = psutil.virtual_memory()
        m0 = st.used

        for ck in cks:
            ck.put("k", v)

        gc.collect()
        time.sleep(1)
        gc.collect()

        st = psutil.virtual_memory()
        m1 = st.used
        f = (m1 - m0) / NCLIENT
        if m1 > m0 + (NCLIENT * 200):
            self.fail(f"error: server using too much memory {m0} {m1} ({f:.2f} per client)\n")

        for ck in cks:
            cfg.delete_client(ck)

        cfg.end()

class TestMemGetManyClients(unittest.TestCase):
    def test_mem_get_many_clients(self):
        NCLIENT = 100_000

        cfg = make_config(self, False)
        cfg.begin("Test: memory use many get client")

        ck = cfg.make_client()
        ck.put("0", "")
        cfg.delete_client(ck)

        cks = [cfg.make_client() for _ in range(NCLIENT)]

        time.sleep(1)

        gc.collect()
        st = psutil.virtual_memory()
        m0 = st.used

        for ck in cks:
            ck.get("0")

        gc.collect()
        time.sleep(1)
        gc.collect()

        st = psutil.virtual_memory()
        m1 = st.used
        f = (m1 - m0) / NCLIENT
        if m1 >= m0 + NCLIENT * 10:
            self.fail(f"error: server using too much memory m0 {m0} m1 {m1} ({f:.2f} per client)\n")

        for ck in cks:
            cfg.delete_client(ck)

        cfg.end()

class TestMemManyAppends(unittest.TestCase):
    def test_mem_many_appends(self):
        N = 1000
        MEM = 1000

        cfg = make_config(self, False)
        cfg.begin("Test: memory use many appends")

        ck = cfg.make_client()
        rd_val = rand_value(MEM)

        gc.collect()
        st = psutil.virtual_memory()
        m0 = st.used

        for _ in range(N):
            ck.append("k", rd_val)

        gc.collect()
        time.sleep(1)
        gc.collect()

        st = psutil.virtual_memory()
        m1 = st.used
        if m1 >= 3 * MEM * N:
            self.fail(f"error: server using too much memory m0 {m0} m1 {m1}\n")

        logging.info(f"m0 {m0} m1 {m1}\n")

        cfg.delete_client(ck)
        cfg.end()
