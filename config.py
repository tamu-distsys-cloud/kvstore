import os
import random
import math
import time
import threading
import unittest
import logging
import base64

from labrpc.labrpc import Network, Service, Server
from client import Clerk
from server import KVServer

SERVERID = 0

def randstring(n):
    b = os.urandom(2 * n)
    s = base64.urlsafe_b64encode(b).decode('utf-8')
    return s[:n]

def make_seed():
    max_val = 1 << 62
    bigx = random.randint(0, max_val)
    return bigx

class Config:
    def __init__(self, t: unittest.TestCase):
        self.mu = threading.Lock()
        self.t = t
        self.net = Network()
        self.kvserver = None
        self.endname = ""
        self.clerks = {}
        self.next_client_id = SERVERID + 1
        self.start = time.time()
        self.t0 = None
        self.rpcs0 = 0
        self.ops = 0

    def cleanup(self):
        with self.mu:
            self.net.cleanup()

    def make_client(self):
        with self.mu:
            endname = randstring(20)
            end = self.net.make_end(endname)
            self.net.connect(endname, SERVERID)
            ck = Clerk(end)
            self.clerks[ck] = endname
            self.next_client_id += 1
            self.connect_client_unlocked(ck)
        return ck

    def delete_client(self, ck):
        with self.mu:
            v = self.clerks[ck]
            self.net.delete_end(v)
            del self.clerks[ck]

    def connect_client_unlocked(self, ck):
        endname = self.clerks[ck]
        self.net.enable(endname, True)

    def connect_client(self, ck):
        with self.mu:
            self.connect_client_unlocked(ck)

    def start_server(self):
        self.kvserver = KVServer()
        kvsvc = Service(self.kvserver)
        srv = Server()
        srv.add_service(kvsvc)
        self.net.add_server(SERVERID, srv)

    def begin(self, description):
        print(f"{description} ...\n")
        self.t0 = time.time()
        self.rpcs0 = self.rpc_total()
        with self.mu:
            self.ops = 0

    def op(self):
        with self.mu:
            self.ops += 1

    def rpc_total(self):
        return self.net.get_total_count()

    def end(self):
        if self.t.defaultTestResult().wasSuccessful():
            t = time.time() - self.t0
            nrpc = self.rpc_total() - self.rpcs0
            with self.mu:
                ops = self.ops
            print("  ... Passed --")
            print(f" t {t} nrpc {nrpc} ops {ops}\n")

def make_config(t, unreliable):
    cfg = Config(t)
    cfg.clerks = {}
    cfg.next_client_id = SERVERID + 1
    cfg.start = time.time()
    cfg.start_server()
    cfg.net.reliable(not unreliable)
    return cfg

