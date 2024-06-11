import math
from typing import List

class BitSet:
    def __init__(self, bits: int):
        extra = 0
        if bits % 64 != 0:
            extra = 1
        chunks = bits // 64 + extra
        self.data = [0] * chunks

    def clone(self):
        data_copy = self.data.copy()
        return BitSet.from_data(data_copy)

    @classmethod
    def from_data(cls, data: List[int]):
        bitset = cls(0)
        bitset.data = data
        return bitset

    @staticmethod
    def bitset_index(pos: int):
        return pos // 64, pos % 64

    def set(self, pos: int):
        major, minor = self.bitset_index(pos)
        self.data[major] |= (1 << minor)
        return self

    def clear(self, pos: int):
        major, minor = self.bitset_index(pos)
        self.data[major] &= ~(1 << minor)
        return self

    def get(self, pos: int) -> bool:
        major, minor = self.bitset_index(pos)
        return (self.data[major] & (1 << minor)) != 0

    def popcnt(self) -> int:
        total = 0
        for v in self.data:
            total += bin(v).count('1')
        return total

    def hash(self) -> int:
        hash_value = self.popcnt()
        for v in self.data:
            hash_value ^= v
        return hash_value

    def equals(self, other) -> bool:
        if len(self.data) != len(other.data):
            return False
        for i in range(len(self.data)):
            if self.data[i] != other.data[i]:
                return False
        return True

