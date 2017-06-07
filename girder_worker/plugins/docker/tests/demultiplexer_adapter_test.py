import unittest
import struct
from girder_worker.plugins.docker.demultiplexer_adapter import DemultiplexerAdapterPushAdapter

class CaptureAdapter(object):
    def __init__(self):
        self._captured = ''

    def write(self, data):
        self._captured += data

    def captured(self):
        return self._captured


class TestDemultiplexerPushAdapter(unittest.TestCase):
    def testSinglePayload(self):
        data = [
            '\x02\x00\x00\x00\x00\x00\x00\x14this is stderr data\n'
        ]

        capture = CaptureAdapter()
        adapter = DemultiplexerAdapterPushAdapter(capture)
        for d in data:
            adapter.write(d)

        self.assertEqual(capture.captured(), 'this is stderr data\n')

    def testAdapterBrokenUp(self):
        data = [
            '\x02\x00\x00\x00', '\x00\x00' '\x00\x14','this is stderr data\n'
        ]

        capture = CaptureAdapter()
        adapter = DemultiplexerAdapterPushAdapter(capture)
        for d in data:
            adapter.write(d)

        self.assertEqual(capture.captured(), 'this is stderr data\n')

    def testMultiplePayload(self):
        data = [
            '\x02\x00\x00\x00\x00\x00\x00\x14this is stderr data\n',
            '\x01\x00\x00\x00\x00\x00\x00\x14this is stdout data\n',
            '\x01\x00\x00\x00\x00\x00\x00\x0chello world!'
        ]

        capture = CaptureAdapter()
        adapter = DemultiplexerAdapterPushAdapter(capture)
        for d in data:
            adapter.write(d)


        self.assertEqual(capture.captured(), 'this is stderr data\nthis is stdout data\nhello world!')

