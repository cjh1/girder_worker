import os
import sys
import errno
import stat
from girder_worker.app import app
from girder_worker.core import utils

class StreamConnector(object):
    def __init__(self, input, output):
        self._input = input
        self._output = output

    def open(self):
        pass

    def close(self):
        pass

class WriteStreamConnector(StreamConnector):

    def fileno(self):
        self._output.fileno()

    def path(self):
        return self._output.path()

    def write(self, n=65536):
        buf = self._input.read(n)

        if buf:
            self._output.write(buf)
        else:
            self.close()

    def open(self):
        self._output.open()


    def close(self):
        self._output.close()
        self._input.close()


class ReadStreamConnector(StreamConnector):

    def fileno(self):
        self._input.fileno()

    def path(self):
        return self._input.path()

    def read(self, n=65536):
        buf = self._input.read(n)

        if buf:
            return self._output.write(buf)
        else:
            self.close()

        return None

    def open(self):
        self._input.open()

    def close(self):
        self._input.close()
        self._output.close()

class FilenoReader(object):
    def __init__(self, fd=0):
        self._fd = fd

    def fileno(self):
        return self._fd

    def read(self, n):
        return os.read(self._fd, n)

    def close(self):
        os.close(self._fd)

class FilenoWriter(object):
    def __init__(self, fd=0):
        self._fd = fd

    def fileno(self):
        return self._fd

    def write(self, buf):
        return os.write(self._fd, buf)

    def close(self):
        os.close(self._fd)

class NamedPipe(object):
    def __init__(self, path):
        self.path = path
        self._fd = None
        os.mkfifo(self.path)

    def open(self):
        if self._fd is None:
            if not os.path.exists(self.path):
                raise Exception('Input pipe does not exist: %s' % self._path)
            if not stat.S_ISFIFO(os.stat(self.path).st_mode):
                raise Exception('Input pipe must be a fifo object: %s' % self._path)

            try:
                self._fd = os.open(self._path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as e:
                if e.errno != errno.ENXIO:
                    raise e

class NamedPipeReader(FilenoReader):
    def __init__(self, pipe):
        super(NamedPipeReader, self).__init__()
        self._pipe = pipe

    def open(self):
        if self._fd is None:
            self._fd = self._pipe.open()

class NamedPipeWriter(FilenoWriter):
    def __init__(self, pipe):
        super(NamedPipeWriter, self).__init__()
        self._pipe = pipe

    def open(self):
        if self._fd is None:
            self._fd = self._pipe.open()

class ProgressPipe(NamedPipe):

    def __init__(self, path):
        super(ProgressPipe, self).__init__(path)

    def transform(self, task):
        super(ProgressPipe, self).transform()
        job_mgr = task.job_manager
        fd = os.open(self._path, os.O_RDONLY | os.O_NONBLOCK)

        return ReadStreamConnector(FilenoReader(fd), utils.JobProgressAdapter(job_mgr))

