import math
import threading

from golem.collections.bitarray import BitArray


def test_get_set_item():
    length = 101
    bitarray = BitArray(length)

    assert bitarray.length == length
    assert bitarray.byte_length == math.ceil(length / 8)

    for i in range(length):
        assert bitarray[i] == 0
        bitarray[i] = i % 2
        assert bitarray[i] == i % 2

    for i in range(length):
        assert bitarray.get(i) == i % 2
        bitarray.set(i, (i + 1) % 2)
        assert bitarray.get(i) == (i + 1) % 2


def test_empty_full():
    length = 1
    bitarray = BitArray(length)

    assert bitarray.empty()
    bitarray.set(0, 1)
    assert bitarray.full()

    length = 64
    bitarray = BitArray(length)

    assert bitarray.empty()
    for i in range(length):
        bitarray[-i] = 1
    assert bitarray.full()

    length = 167
    bitarray = BitArray(length)
    assert bitarray.empty()
    for i in range(length):
        bitarray.set(i, 1)
    assert bitarray.full()


def test_thread_safety():
    n_threads = 7
    length = n_threads * 128
    bitarray = BitArray(length)

    class Incrementer(object):
        def __init__(self, start_idx, val=1):
            self.idx = start_idx
            self.val = val

        def inc(self):
            while self.idx < length:
                bitarray[self.idx] = self.val
                self.idx += n_threads

    incrementers = [Incrementer(i) for i in range(n_threads)]
    threads = [threading.Thread(target=incrementers[i].inc)
               for i in range(n_threads)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert bitarray.full(), "{} != {}".format(bitarray.count(), length)

