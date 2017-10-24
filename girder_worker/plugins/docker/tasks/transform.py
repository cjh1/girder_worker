import os
import sys

from girder_worker import utils

from .io import ReadStreamConnector, WriteStreamConnector, NamedPipe, FileDescriptorReader

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
        return self


class ContainerStdErr(BaseTransform):

    def transform(self):
        return self


class ProgressPipe(BaseTransform):

    def __init__(self, path):
        super(ProgressPipe, self).__init__()
        self._pipe = NamedPipe(path)

    def transform(self, task):
        super(ProgressPipe, self).transform()
        job_mgr = task.job_manager
        return ReadStreamConnector(FileDescriptorReader(self._pipe), utils.JobProgressAdapter(job_mgr))

class NamedPipe(BaseTransform):
    pass

class Connect(BaseTransform):
    def __init__(self, input, output):
        super(Connect, self).__init__()
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
# docker_run.delay(image, stream_connectors=[Connect(ContainerStdOut(), GirderFileId(id))]
#                                           output                                                  input
# docker_run.delay(image, container_args=[Connect(NamedPipe('my/input/pipe'), StdOut()), Connect(GirderFileId(id), NamedPipe('my/girder/pipe'))])
#
# args passed to container: my/input/pipe, my/girder/pipe
#

