import sys
import uuid
import os
import tempfile

from girder_worker_utils.transform import Transform


TEMP_VOLUME_MOUNT_PREFIX = '/mnt/girder_worker'


def _maybe_transform(obj, *args, **kwargs):
    if hasattr(obj, 'transform') and hasattr(obj.transform, '__call__'):
        return obj.transform(*args, **kwargs)

    return obj


class HostStdOut(Transform):
    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            StdStreamWriter
        )
        return StdStreamWriter(sys.stdout)


class HostStdErr(Transform):
    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            StdStreamWriter
        )
        return StdStreamWriter(sys.stderr)


class ContainerStdOut(Transform):

    def transform(self, **kwargs):
        return self

    def open(self):
        # noop
        pass


class ContainerStdErr(Transform):

    def transform(self, **kwargs):
        return self

    def open(self):
        # noop
        pass


class Volume(Transform):
    def __init__(self, host_path, container_path, mode='rw'):
        self._host_path = host_path
        self._container_path = container_path
        self.mode = mode

    def _repr_json_(self):
        return {
            self.host_path: {
                'bind': self.container_path,
                'mode': self.mode
            }
        }

    def transform(self, **kwargs):
        return self.container_path

    @property
    def container_path(self):
        return self._container_path

    @property
    def host_path(self):
        return self._host_path

class _DefaultTemporaryVolume(Volume):
    def __init__(self):
        self._dir = dir
        self._transformed = False
        super(_DefaultTemporaryVolume, self).__init__(
            tempfile.mkdtemp(),
            os.path.join(TEMP_VOLUME_MOUNT_PREFIX, uuid.uuid4().hex))

    def transform(self, temp_volume=None, **kwargs):
        self._transformed = True


class TemporaryVolume(Volume):
    """
    This is a class used to represent a temporary volume that will be mounted
    into a docker container. girder_worker will automatically create one and this
    utility class can be used to refer to it. The root directory on the host can
    be configured by pass a value into the constructor and then passing this instance,
    in the docker_run `volumes` param.
    """
    def __init__(self, host_dir=None):
        """
        :param host_dir: The root directory on the host to use when creating the
            the temporary host path.
        :type host_dir: str
        """
        super(TemporaryVolume, self).__init__(None, None)
        self.host_dir = host_dir
        self._instance = None
        self._transformed = False

    def transform(self, temp_volume=None, **kwargs):

        # If not host_dir was provided, i.e. We are the default temporary volume,
        # we save the runtime instances provide by the task, we delegate to this
        # instance.
        if temp_volume is not None and self.host_dir is None:
            self._instance = temp_volume
            self._transformed = True
            return self._instance.transform(**kwargs)
        else:
            if not self._transformed:
                self._transformed = True
                if self.host_dir is not None and not os.path.exists(self.host_dir):
                    os.makedirs(self.host_dir)
                self._host_path = tempfile.mkdtemp(dir=self.host_dir)
                self._container_path = os.path.join(TEMP_VOLUME_MOUNT_PREFIX, uuid.uuid4().hex)
            return super(TemporaryVolume, self).transform(**kwargs)

    @property
    def container_path(self):
        if self._instance is not None:
            return self._instance.container_path
        else:
            return super(TemporaryVolume, self).container_path

    @property
    def host_path(self):
        if self._instance is not None:
            return self._instance.host_path
        else:
            return super(TemporaryVolume, self).host_path


class NamedPipeBase(Transform):
    def __init__(self, name, container_path=None, host_path=None, volume=TemporaryVolume()):
        super(NamedPipeBase, self).__init__()
        self._container_path = None
        self._host_path = None
        self._volume = None

        if container_path is not None and host_path is not None:
            self._container_path = container_path
            self._host_path = host_path
        else:
            self._volume = volume

        self.name = name

    def transform(self, **kwargs):
        if self._volume is not None:
            self._volume.transform(**kwargs)

    @property
    def container_path(self):
        if self._container_path is not None:
            return os.path.join(self._container_path, self.name)
        else:
            return os.path.join(self._volume.container_path, self.name)

    @property
    def host_path(self):
        if self._host_path is not None:
            return os.path.join(self._host_path, self.name)
        else:
            return os.path.join(self._volume.host_path, self.name)

    def cleanup(self, **kwargs):
        os.remove(self.host_path)


class NamedInputPipe(NamedPipeBase):
    """
    A named pipe that read from within a docker container.
    i.e. To stream data out of a container.
    """
    def __init__(self, name,  container_path=None, host_path=None, volume=TemporaryVolume()):
        super(NamedInputPipe, self).__init__(name, container_path, host_path, volume)

    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            NamedPipe,
            NamedPipeWriter
        )
        super(NamedInputPipe, self).transform(**kwargs)

        pipe = NamedPipe(self.host_path)
        return NamedPipeWriter(pipe, self.container_path)


class NamedOutputPipe(NamedPipeBase):
    """
    A named pipe that written to from within a docker container.
    i.e. To stream data out of a container.
    """
    def __init__(self, name, container_path=None, host_path=None, volume=TemporaryVolume()):
        super(NamedOutputPipe, self).__init__(name, container_path, host_path, volume)

    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            NamedPipe,
            NamedPipeReader
        )
        super(NamedOutputPipe, self).transform(**kwargs)

        pipe = NamedPipe(self.host_path)
        return NamedPipeReader(pipe, self.container_path)


class FilePath(Transform):
    def __init__(self, filename, volume=TemporaryVolume()):
        self.filename = filename
        self._volume = volume

    def transform(self, *pargs, **kwargs):
        self._volume.transform(**kwargs)
        # If we are being called with arguments, then this is the execution of
        # girder_result_hooks, so return the host_path
        if len(pargs) > 0:
            return os.path.join(self._volume.host_path, self.filename)
        else:
            return os.path.join(self._volume.container_path, self.filename)


class Connect(Transform):
    def __init__(self, input, output):
        super(Connect, self).__init__()
        self._input = input
        self._output = output

    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            WriteStreamConnector,
            ReadStreamConnector,
        )
        input = _maybe_transform(self._input, **kwargs)
        output = _maybe_transform(self._output, **kwargs)
        if isinstance(self._output, NamedInputPipe):
            return WriteStreamConnector(input, output)
        else:
            return ReadStreamConnector(input, output)

    def _repr_model_(self):
        """
        The method is called before save the argument in the job model.
        """
        return str(self)


class ChunkedTransferEncodingStream(Transform):
    def __init__(self, url, headers={}, **kwargs):
        self.url = url
        self.headers = headers

    def transform(self, **kwargs):
        from girder_worker.docker.io import (
            ChunkedTransferEncodingStreamWriter
        )

        return ChunkedTransferEncodingStreamWriter(self.url, self.headers)
