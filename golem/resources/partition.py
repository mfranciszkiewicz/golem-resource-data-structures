import collections
import math
import merkle
import os

from golem.collections.bitarray import BitArray
from gevent.fileobject import FileObjectThread
from threading import RLock


DEFAULT_CHUNK_SIZE = 1024 * 16


class FileWrapper(object):

    @classmethod
    def open(cls, path, mode='r+b'):
        return open(path, mode)


class GeventFileWrapper(FileWrapper):

    @classmethod
    def open(cls, path, mode='r+b'):
        fd = super(GeventFileWrapper, cls).open(path, mode)

        from gevent.fileobject import FileObjectThread
        return FileObjectThread(fd, mode)


class Partition(object):
    """
        Stores a list of file paths and sizes.
        Files are partitioned by chunks, and can be accesed by chunk
        number, i.e. partition[12].
    """

    file_wrapper = GeventFileWrapper

    def __init__(self, paths, chunk_size=DEFAULT_CHUNK_SIZE):

        assert isinstance(paths, collections.Sequence)
        assert len(paths) > 0
        assert chunk_size > 0

        # Read file sizes
        sizes = [os.path.getsize(p) for p in paths]
        count = len(sizes)

        # Structure
        self._paths = paths
        self._sizes = sizes
        self._size = sum(sizes)
        self._chunk_size = chunk_size

        # Iteration
        self._iter_offset = None
        self._iter_idx = 0
        self._iter_file = None

        self._files = [None] * count
        self._locks = [RLock() for _ in range(count)]
        self._open = False

    @staticmethod
    def allocate(paths, sizes, chunk_size=DEFAULT_CHUNK_SIZE,
                 fill=b'0', data_size=1024 * 512):
        """ Allocates disk space for files.
            Creates a new Partition instance.  """
        for path, size in zip(paths, sizes):
            # Create directories
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Write file contents
            written = 0
            data = fill * data_size
            with open(path, 'wb') as out:
                while written < size:
                    length = min(data_size, size - written)
                    written += out.write(data[:length])

        return Partition(paths, chunk_size=chunk_size)

    def merkle(self):
        """ Creates a Merkle tree from chunks """
        tree = merkle.MerkleTree()
        was_open = self.is_open()

        if not was_open:
            self.open()

        try:
            for chunk in self:
                tree.add(chunk)
        finally:
            if not was_open:
                self.close()

        tree.build()
        return tree

    def size(self):
        """ Returns the number of chunks """
        return int(math.ceil(self._size / self._chunk_size))

    def offset(self, chunk_num):
        """ Returns the file index and offset of a chunk with given number """
        offset = chunk_num * self._chunk_size

        if not 0 <= offset <= self._size:
            raise IndexError('Index out of range')

        for idx, size in enumerate(self._sizes):
            if offset - size < 0:
                return idx, offset
            offset -= size

        return None, None

    def is_open(self):
        """ Returns whether partitioned files were opened """
        return self._open

    def open(self):
        """ Opens and stores handles to partitioned files """
        for idx, fd in enumerate(self._files):
            if fd:
                continue
            with self._locks[idx]:
                self._files[idx] = self.file_wrapper.open(
                    self._paths[idx])

        self._open = True

    def close(self):
        """ Closes any open file handles """
        for idx, fd in enumerate(self._files):
            with self._locks[idx]:
                fd and fd.close()

        self._open = False

    def __getitem__(self, chunk_num):
        idx, offset = self.offset(chunk_num)
        return self.__read(idx, offset )[0]

    def __setitem__(self, chunk_num, data):
        idx, offset = self.offset(chunk_num)
        return self.__write(idx, offset, data)[0]

    def __iter__(self):
        self._iter_offset = 0
        self._iter_idx = 0
        self._iter_file = self._files[0]

        return self

    def __next__(self):
        chunk, idx, offset = self.__read(self._iter_idx,
                                         self._iter_offset)

        if self._iter_idx == idx and self._iter_offset == offset:
            raise StopIteration()

        self._iter_idx = idx
        self._iter_offset = offset

        return chunk

    def __read(self, idx, offset):

        read = 0
        chunk = None
        source = self._files[idx]

        while read < self._chunk_size:

            with self._locks[idx]:
                if source.tell() != offset:
                    source.seek(offset)
                buf = source.read(self._chunk_size - read)

            chunk = chunk + buf if chunk else buf
            read += len(buf)
            offset += len(buf)

            is_enough = len(chunk) == self._chunk_size
            is_end = idx + 1 == len(self._sizes)
            if is_enough or is_end:
                break

            idx += 1
            offset = 0
            source = self._files[idx]

        return chunk, idx, offset

    def __write(self, idx, offset, data):

        written = 0
        chunk = data.__class__(data)
        source = self._files[idx]

        while written < len(data):

            with self._locks[idx]:
                if source.tell() != offset:
                    source.seek(offset)
                write_cnt = self._sizes[idx] - offset
                write_cnt = source.write(chunk[:write_cnt])

            chunk = chunk[write_cnt:]
            written += write_cnt
            offset += write_cnt

            is_enough = written == len(data)
            is_end = idx + 1 == len(self._sizes)
            if is_enough or is_end:
                break

            idx += 1
            offset = 0
            source = self._files[idx]

        return written, idx, offset

