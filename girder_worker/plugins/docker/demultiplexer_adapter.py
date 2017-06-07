import struct

class DemultiplexerAdapterPushAdapter(object):

    def __init__(self, adapter):
        self._header_bytes_read = 0
        self._payload_bytes_read = 0
        self._payload_size = None
        self._header = ''
        self._payload = ''
        self._adapter = adapter

    def _read_header(self):
        bytes_to_read = min(8 - self._header_bytes_read, self._data_length-self._data_offset)
        self._header += self._data[self._data_offset:self._data_offset+bytes_to_read]
        self._data_offset += bytes_to_read
        self._header_bytes_read += bytes_to_read

        if self._header_bytes_read == 8:
            _, payload_size = struct.unpack('>BxxxL', self._header)

            return payload_size

    def _read_payload(self):
        bytes_to_read = min(self._payload_size - self._payload_bytes_read, self._data_length-self._data_offset)
        self._adapter.write(self._data[self._data_offset:self._data_offset+bytes_to_read])
        self._data_offset += bytes_to_read
        self._payload_bytes_read += bytes_to_read

    def write(self, data):
        self._data = data
        self._data_length = len(data)
        self._data_offset = 0

        while self._data_length - self._data_offset > 0:
            # We are reading the header
            if self._header_bytes_read < 8:
                self._payload_size = self._read_header()

            # We are reading the payload
            if self._payload_size and self._payload_bytes_read < self._payload_size:
                self._read_payload()

            # We are done with this payload
            if self._payload_size == self._payload_bytes_read:
                self._header = ''
                self._payload = ''
                self._header_bytes_read = 0
                self._payload_bytes_read = 0
                self._payload_size = 0


