import threading
import time
import unittest

from labrpc.labrpc import *

class JunkArgs:
    def __init__(self, x):
        self.x = x

class JunkReply:
    def __init__(self, x = ''):
        self.x = x

class JunkServer:
    def __init__(self):
        self.mu = threading.Lock()
        self.log1 = []
        self.log2 = []

    def handler1(self, args):
        reply = [""]
        with self.mu:
            self.log1.append(args)
            reply[0] = int(args)
        return reply

    def handler2(self, args):
        reply = [""]
        with self.mu:
            reply[0] = f"handler2-{args}"
        return reply

    def handler3(self, args):
        reply = [""]
        with self.mu:
            time.sleep(20)
            reply[0] = -args
        return reply

    def handler4(self, args):
        return JunkReply("pointer")

    def handler5(self, args):
        return JunkReply("no pointer")

    def handler6(self, args):
        reply = [""]
        with self.mu:
            reply[0] = len(args)
        return reply

    def handler7(self, args):
        reply = [""]
        with self.mu:
            reply[0] = "y" * args
        return reply

class TestBasic(unittest.TestCase):
    def test_basic(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        e = rn.make_end("end1-99")

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server("server99", rs)

        rn.connect("end1-99", "server99")
        rn.enable("end1-99", True)

        reply = e.call("JunkServer.handler2", 111)
        self.assertEqual(reply[0], "handler2-111", "wrong reply from handler2")

        reply = e.call("JunkServer.handler1", "9099")
        self.assertEqual(reply[0], 9099, "wrong reply from handler1")

class TestTypes(unittest.TestCase):
    def test_types(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        e = rn.make_end("end1-99")

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server("server99", rs)

        rn.connect("end1-99", "server99")
        rn.enable("end1-99", True)

        args = JunkArgs(0)
        reply = e.call("JunkServer.handler4", args)
        self.assertIsInstance(reply, JunkReply)
        self.assertEqual(reply.x, "pointer", "wrong reply from handler4")

        args = JunkArgs(0)
        reply = e.call("JunkServer.handler5", args)
        self.assertIsInstance(reply, JunkReply)
        self.assertEqual(reply.x, "no pointer", "wrong reply from handler5")

class TestDisconnect(unittest.TestCase):
    def test_disconnect(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        e = rn.make_end("end1-99")

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server("server99", rs)

        rn.connect("end1-99", "server99")

        reply = e.call("JunkServer.handler2", 111)
        self.assertEqual(reply, None)

        rn.enable("end1-99", True)

        reply = e.call("JunkServer.handler1", "9099")
        self.assertIsInstance(reply, list)
        self.assertEqual(reply[0], 9099, "wrong reply from handler1")

class TestCounts(unittest.TestCase):
    def test_counts(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        e = rn.make_end("end1-99")

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server(99, rs)

        rn.connect("end1-99", 99)
        rn.enable("end1-99", True)

        for i in range(17):
            reply = e.call("JunkServer.handler2", i)
            wanted = f"handler2-{i}"
            self.assertIsInstance(reply, list)
            self.assertEqual(reply[0], wanted, f"wrong reply {reply[0]} from handler2, expecting {wanted}")

        n = rn.get_count(99)
        self.assertEqual(n, 17, f"wrong get_count() {n}, expected 17")

class TestBytes(unittest.TestCase):
    def test_bytes(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        e = rn.make_end("end1-99")

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server(99, rs)

        rn.connect("end1-99", 99)
        rn.enable("end1-99", True)

        for _ in range(40):
            args = "x" * 128
            reply = e.call("JunkServer.handler6", args)
            wanted = len(args)
            self.assertIsInstance(reply, list)
            self.assertEqual(reply[0], wanted, f"wrong reply {reply[0]} from handler6, expecting {wanted}")

        n = rn.get_total_bytes()
        self.assertTrue(4828 <= n <= 6000, f"wrong get_total_bytes() {n}, expected about 5000")

        for _ in range(400):
            args = 107
            reply = e.call("JunkServer.handler7", args)
            wanted = args
            self.assertEqual(len(reply[0]), wanted, f"wrong reply len={len(reply[0])} from handler7, expecting {wanted}")

        nn = rn.get_total_bytes() - n
        self.assertTrue(1800 <= nn <= 2500, f"wrong get_total_bytes() {nn}, expected about 2000")

class TestConcurrentMany(unittest.TestCase):
    def test_concurrent_many(self):
        rn = Network()
        self.addCleanup(rn.cleanup)

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server(1000, rs)

        ch = []

        nclients = 20
        nrpcs = 10
        threads = []
        for ii in range(nclients):
            def client_thread(i):
                n = 0
                e = rn.make_end(i)
                rn.connect(i, 1000)
                rn.enable(i, True)

                for j in range(nrpcs):
                    arg = i * 100 + j
                    reply = e.call("JunkServer.handler2", arg)
                    wanted = f"handler2-{arg}"
                    self.assertIsInstance(reply, list)
                    self.assertEqual(reply[0], wanted, f"wrong reply {reply[0]} from handler2, expecting {wanted}")
                    n += 1
                ch.append(n)

            t = threading.Thread(target=client_thread, args=(ii,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        total = sum(ch)
        self.assertEqual(total, nclients * nrpcs, f"wrong number of RPCs completed, got {total}, expected {nclients * nrpcs}")

        n = rn.get_count(1000)
        self.assertEqual(n, total, f"wrong get_count() {n}, expected {total}")

class TestUnreliable(unittest.TestCase):
    def test_unreliable(self):
        rn = Network()
        self.addCleanup(rn.cleanup)
        rn.reliable(False)

        js = JunkServer()
        svc = Service(js)

        rs = Server()
        rs.add_service(svc)
        rn.add_server(1000, rs)

        ch = []

        nclients = 300
        threads = []
        for ii in range(nclients):
            def client_thread(i):
                n = 0
                e = rn.make_end(i)
                rn.connect(i, 1000)
                rn.enable(i, True)

                arg = i * 100
                reply = e.call("JunkServer.handler2", arg)
                if reply is not None:
                    wanted = f"handler2-{arg}"
                    self.assertIsInstance(reply, list)
                    self.assertEqual(reply[0], wanted, f"wrong reply {reply[0]} from handler2, expecting {wanted}")
                    n += 1
                ch.append(n)

            t = threading.Thread(target=client_thread, args=(ii,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        total = sum(ch)
        self.assertEqual(total, nclients, f"wrong number of RPCs completed, got {total}, expected {nclients}")

        n = rn.get_count(1000)
        self.assertEqual(n, total, f"wrong get_count() {n}, expected {total}")

