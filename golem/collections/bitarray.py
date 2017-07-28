import array


class BitArray(object):

    BITMASK = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]

    def __init__(self, length=0, bitarray=None, fill=0):
        if bitarray:
            length = bitarray.length
            init = bitarray.array
        else:
            fill = 0 if not fill else 255
            init = [fill] * ((length + 7) // 8)

        self._length = length
        self._array = array.array('B', init)

        mod = self._length % 8
        ones = ~0 % 255

        # ending byte bitmask for current length
        self._emask = ~(ones << mod) % 255

    def __getitem__(self, pos):
        """ Retrieves the bit at a specified position """
        pos = self.__pos(pos)
        mod = pos % 8
        val = self._array[pos // 8] & self.BITMASK[mod]
        return val >> mod

    def __setitem__(self, pos, val):
        """ Sets the bit at a specified position """
        pos = self.__pos(pos)
        if val == 0:
            self._array[pos // 8] &= ~self.BITMASK[pos % 8]
        else:
            self._array[pos // 8] |= self.BITMASK[pos % 8]

    @property
    def length(self):
        """ Length property getter """
        return self._length

    @property
    def byte_length(self):
        """ Byte length property getter """
        return len(self._array)

    def get(self, pos):
        """ Retrieves the bit at the specified position.
            Proxy for __getitem__ """
        return self[pos]

    def set(self, pos, val):
        """ Sets the bit at the specified position.
            Proxy for __setitem__ """
        self[pos] = val

    def count(self, bit='1'):
        """ Returns the total number of 0 or 1 bits """
        return sum(bin(self._array[b]).count(bit)
                   for b in range(self.byte_length))

    def empty(self):
        """ Checks whether all bits are set to 0 """
        return all(self._array[b] == 0
                   for b in range(self.byte_length))

    def full(self):
        """ Checks whether all bits are set to 1 """
        if self._array[-1] & self._emask != self._emask:
            return False
        return all(self._array[b] == 255
                   for b in range(self.byte_length - 1))

    def __pos(self, pos):
        """ Calculates position if negative indexing was used """
        if pos < 0:
            return self._length + pos
        return pos

