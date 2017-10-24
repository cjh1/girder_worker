import os
import sys
import errno
import stat
import abc

from girder_worker.app import app
from girder_worker.core import utils


class StreamConnector(object):
    """
    StreamConnector is an abstract base class use to connect a read(input) and write(output)
    stream.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, input, output):
        self.input = input
        self.output = output

    @abc.abstractmethod
    def open(self):
        """
        Open the stream connector, delegated to implementation.
        """

    @abc.abstractmethod
    def close(self):
        """
        Close the stream connector, delegated to implementation.
        """

    def fileno(self):
        """
        This method allows an instance of this class to used in the select call.

        :returns The file descriptor that should be used to determine if the connector
                 has data to process.
        """
        self.output.fileno()


class WriteStreamConnector(StreamConnector):
    """
    WriteStreamConnector can be used to connect a read and write stream. The write
    side of the connection will be used in the select loop to trigger write i.e
    the file description from the write stream will be used in the select call.
    """

    def fileno(self):
        """
        This method allows an instance of this class to used in the select call.

        :returns The file descriptor for write(output) side of the connection.
        """
        self._output.fileno()

    def __repr__(self):
        return repr(self._output)

    def write(self, n=65536):
        """
        Called when it is detected the output side of this connector is ready
        to write. Reads (potentially blocks) at most n bytes and writes them to
        the output ends of this connector. If no bytes could be read, the connector
        is closed.

        :param n The maximum number of bytes to write.
        :type n int
        :returns The actual number of bytes written.
        """
        buf = self.input.read(n)

        if buf:
            return self.output.write(buf)
        else:
            self.close()

        return 0

    def open(self):
        """
        Calls open on the output side of this connector.
        """
        self.output.open()


    def close(self):
        """
        Closes the output side of this connector, followed by the input side.
        """
        self.output.close()
        self.input.close()


class ReadStreamConnector(StreamConnector):
    """
    ReadStreamConnector can be used to connect a read and write stream. The read
    side of the connection will be used in the select loop to trigger write i.e
    the file description from the read stream will be used in the select call.
    """

    def fileno(self):
        """
        This method allows an instance of this class to used in the select call.

        :returns The file descriptor for read(input) side of the connection.
        """

        self.input.fileno()

    def __repr__(self):
        return repr(self.input)

    def read(self, n=65536):
        """
        Called when it is detected the input side of this connector is ready
        to read. Reads at most n bytes and writes them to the output ends of
        this connector. If no bytes could be read, the connector is closed.

        :param n The maximum number of bytes to read.
        :type n int
        :returns The actual number of bytes read.
        """
        buf = self.input.read(n)

        if buf:
            return self.output.write(buf)
        else:
            self.close()

        return 0

    def open(self):
        """
        Calls open on the input side of this connector.
        """
        self.input.open()

    def close(self):
        """
        Closes the output side of this connector, followed by the input side.
        """
        self.input.close()
        self.output.close()


class Reader(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def fileno(self):
        """
        This method allows an instance of this class to used in the select call.

        :returns The file descriptor that should be used to determine if there data
                 to read.
        """

    @abc.abstractmethod
    def read(self, n):
        """
        :param The maximum number if bytes to read.
        """

    @abc.abstractmethod
    def close(self):
        """
        Closes this reader.
        """

    def open(self):
        pass


class Writer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def fileno(self):
        """
        This method allows an instance of this class to used in the select call.

        :returns The file descriptor that should be used to determine if there data
                 to write.
        """

    @abc.abstractmethod
    def write(self, n):
        """
        :param The maximum number if bytes to write.
        """

    @abc.abstractmethod
    def close(self):
        """
        Closes this writer.
        """

    def open(self):
        pass

class FileDescriptorReader(Reader):
    def __init__(self, fd=0):
        self._fd = fd

    def fileno(self):
        return self._fd

    def read(self, n):
        return os.read(self._fd, n)

    def close(self):
        os.close(self._fd)

class FileDescriptorWriter(Writer):
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
                self._fd = os.open(self.path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as e:
                if e.errno != errno.ENXIO:
                    raise e

    def fileno(self):
        return self._fd

    def __repr__(self):
        return self.path



# TODO Do we need this???
class DockerContainerNamedPipe(NamedPipe):
    def __init__(self, outside_path, inside_path):
        super(DockerContainerNamedPipe, self).__init__(outside_path)
        self.container_path = inside_path

    def __repr__(self):
        return self.container_path


class NamedPipeReader(FileDescriptorReader):
    def __init__(self, pipe):
        super(NamedPipeReader, self).__init__()
        self._pipe = pipe

    def open(self):
        self._pipe.open()

    def __repr__(self):
        return repr(self._pipe)

    def fileno(self):
        return self._pipe.fileno()


class NamedPipeWriter(FileDescriptorWriter):
    def __init__(self, pipe):
        super(NamedPipeWriter, self).__init__()
        self._pipe = pipe

    def open(self):
        self._pipe.open()

    def __repr__(self):
        return repr(self._pipe)

    def fileno(self):
        return self._pipe.fileno()

