
# TODO This will move to girder_work_utils repo.
class BaseTransform(object):
    __state__ = {}

    @classmethod
    def __obj__(cls, state):
        return cls(*state.get("args", []),
                   **state.get("kwargs", {}))

    def __json__(self):
        return {"_class": self.__class__.__name__,
                "__state__": self.__state__}

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls)
        obj.__state__['args'] = args
        obj.__state__['kwargs'] = kwargs
        return obj


class StdOut(BaseTransform):

    def transform(self):
        return sys.stdout


class StdError(BaseTransform):

    def transform(self):
        return sys.stderr


class ContainerStdOut(BaseTransform):

    def transform(self):
        return sys.stdout


class ContainerStdError(BaseTransform):

    def transform(self):
        return sys.stderr


class NamedPipe(BaseTransform):
    def __init__(self, path):
        self.path = path
        self._fd = None

    def transform(self):
        os.mkfifo(self.path)

        return self

    def open(self):
        if self._fd is None
            if not os.path.exists(self.path):
                raise Exception('Input pipe does not exist: %s' % self._path)
            if not stat.S_ISFIFO(os.stat(self.path).st_mode):
                raise Exception('Input pipe must be a fifo object: %s' % self._path)

            try:
                self._fd = os.open(self._path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as e:
                if e.errno != errno.ENXIO:
                    raise e






class Connect(BaseTransform):
    def __init__(self, input, output):
        self._input = input
        self._output = output

    def transform(self):
        if self.isinput():
            return WriteStreamConnector(self._input.transform(), self._output.transform())
        else:
            return ReadStreamConnector(self._input.transform(), self._output.transform())

    def isinput(self):
        return isinstance(self._output, NamedPipe)




#
# For example:
# docker_run.delay(image, stream_connectors=[Connect(NamedPipe('my/input/pipe'), StdOut())]
# docker_run.delay(image, stream_connectors=[Connect(ContainerStdOut(), StdErr())]
# docker_run.delay(image, stream_connectors=[Connect(ContainerStdOut(), StdErr())]
# docker_run.delay(image, stream_connectors=[Connect(GirderFile(id), NamedPipe(my/girder/pipe))]
# docker_run.delay(image, stream_connectors=[ProgressPipe('write/your/progress/here')]
#                                           output                                                  input
# docker_run.delay(image, container_args=[Connect(NamedPipe('my/input/pipe'), StdOut()), Connect(GirderFileId(id), NamedPipe('my/girder/pipe'))])
#
# args passed to container: my/input/pipe, my/girder/pipe
#

