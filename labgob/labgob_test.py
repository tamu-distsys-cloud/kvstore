import unittest
import pickle
import io

from labgob.labgob import *

# Initialize global variables and helper functions
error_count = 0
checked = {}

class T1:
    def __init__(self, T1int0=0, T1int1=0, T1string0="", T1string1=""):
        self.T1int0 = T1int0
        self.T1int1 = T1int1
        self.T1string0 = T1string0
        self.T1string1 = T1string1

class T2:
    def __init__(self):
        self.T2slice = []
        self.T2map = {}
        self.T2t3 = None

class T3:
    def __init__(self, T3int999=0):
        self.T3int999 = T3int999

# Test that we didn't break GOB.
class TestGOB(unittest.TestCase):
    def test_gob(self):
        global error_count
        e0 = error_count

        w = io.BytesIO()

        x0 = 0
        x1 = 1
        t1 = T1()
        t1.T1int1 = 1
        t1.T1string1 = "6.5840"
        t2 = T2()
        t2.T2slice = [T1(), t1]
        t2.T2map[99] = T1(1, 2, "x", "y")
        t2.T2t3 = T3(999)

        e = LabEncoder(w)
        e.encode(x0)
        e.encode(x1)
        e.encode(t1)
        e.encode(t2)
        data = w.getvalue()

        x0 = None
        x1 = None
        t1 = None
        t2 = None

        r = io.BytesIO(data)
        d = LabDecoder(r)
        x0 = d.decode()
        x1 = d.decode()
        t1 = d.decode()
        t2 = d.decode()

        self.assertEqual(x0, 0, f"wrong x0 {x0}")
        self.assertEqual(x1, 1, f"wrong x1 {x1}")
        self.assertEqual(t1.T1int0, 0, f"wrong t1.T1int0 {t1.T1int0}")
        self.assertEqual(t1.T1int1, 1, f"wrong t1.T1int1 {t1.T1int1}")
        self.assertEqual(t1.T1string0, "", f"wrong t1.T1string0 {t1.T1string0}")
        self.assertEqual(t1.T1string1, "6.5840", f"wrong t1.T1string1 {t1.T1string1}")
        self.assertEqual(len(t2.T2slice), 2, f"wrong t2.T2slice len {len(t2.T2slice)}")
        self.assertEqual(t2.T2slice[1].T1int1, 1, "wrong slice value")
        self.assertEqual(len(t2.T2map), 1, f"wrong t2.T2map len {len(t2.T2map)}")
        self.assertEqual(t2.T2map[99].T1string1, "y", "wrong map value")
        self.assertEqual(t2.T2t3.T3int999, 999, "wrong t2.T2t3.T3int999")

        self.assertEqual(error_count, e0, "there were errors, but should not have been")

class T4:
    def __init__(self, Yes=0, no=0):
        self.Yes = Yes
        self.no = no

# Make sure we check capitalization
# labgob prints one warning during this test.
class TestCapital(unittest.TestCase):
    def test_capital(self):
        global error_count
        e0 = error_count

        v = [{}]

        w = io.BytesIO()
        e = LabEncoder(w)
        e.encode(v)
        data = w.getvalue()

        v1 = None
        r = io.BytesIO(data)
        d = LabDecoder(r)
        v1 = d.decode()

# Check that we warn when someone sends a default value over
# RPC but the target into which we're decoding holds a non-default
# value, which GOB seems not to overwrite as you'd expect.
#
# labgob does not print a warning.

class DD:
    def __init__(self, X=0):
        self.X = X

class TestDefault(unittest.TestCase):
    def test_default(self):
        global error_count
        e0 = error_count

        # Send a default value...
        dd1 = DD()

        w = io.BytesIO()
        e = LabEncoder(w)
        e.encode(dd1)
        data = w.getvalue()

        r = io.BytesIO(data)
        d = LabDecoder(r)
        reply = d.decode()
