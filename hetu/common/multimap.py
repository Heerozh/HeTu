"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: MIT 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
from sortedcontainers import SortedList, SortedDict


class MultiMap:
    """
    可以有重复key的map，并能高效索引
    代价是add和slice速度慢一点，而len方法复杂度O(N)
    """

    def __init__(self):
        self._map = SortedDict()
        self.bisect_left = self._map.bisect_left
        self.bisect_right = self._map.bisect_right
        self.get = self._map.get

    def add(self, key, value) -> None:
        """添加一行数据"""
        pos = self.index(key)
        if pos.stop == pos.start:
            self._map[key] = value
        else:
            mapped = self._map.peekitem(pos.start)[1]
            if isinstance(mapped, SortedList):
                mapped.add(value)
            else:
                self._map[key] = SortedList([mapped, value])

    def __bool__(self) -> bool:
        return bool(self._map)

    def remove(self, key, value) -> None:
        """删除一行key和value都匹配的数据，如果有多条匹配的数据，只会删除第一条。复杂度O(2logN)"""
        mapped = self._map.get(key)
        if mapped is None:
            raise KeyError(f"Key {key} not found.")

        if isinstance(mapped, SortedList):
            mapped.remove(value)
            if len(mapped) == 0:
                self._map.pop(key)
        elif mapped == value:
            self._map.pop(key)
        else:
            raise KeyError(f"Key {key} found, but Value {value} not found.")

    def count(self, left=None, right=None) -> int:
        """查询key的数量"""
        if left is None:
            return sum(len(v) if isinstance(v, SortedList) else 1 for v in self._map.values())

        right = right or left

        start = self._map.bisect_left(left)
        if start == len(self._map):
            return 0
        elif self._map.peekitem(start)[0] < left:
            return 0

        stop = self._map.bisect_right(right)
        return sum(
            len(v) if isinstance(v, SortedList) else 1 for v in self._map.values()[start:stop])

    def iloc(self, index: slice) -> list:
        """传入index()返回的slice，获取map中的值"""
        maps = self._map.values()[index]
        rtn = []
        for mapped in maps:
            if type(mapped) is SortedList:  # do not use isinstance, it's slow
                rtn.extend(mapped[::index.step])
            else:
                rtn.append(mapped)
        return rtn

    def query(self, left=None, right=None) -> list:
        sel = self.index(left, right)
        return self.iloc(sel)

    def pop(self, left=None, right=None) -> list:
        sel = self.index(left, right)

        items = self._map.items()[sel]
        rtn = []
        for key, mapped in items:
            if type(mapped) is SortedList:  # 不要用 isinstance, it's slow
                rtn.extend(mapped)
            else:
                rtn.append(mapped)
        del self._map.items()[sel]
        return rtn

    def index(self, left=None, right=None) -> slice:
        """
        查询map，返回slice(start, stop)。如果没找到，返回start==stop的slice，值为插入位置。
        """
        _map = self._map
        lo = _map.bisect_left(left)

        if right is None:
            lo = self._map.bisect_left(left)
            if lo == len(self._map):
                return slice(lo, lo)
            elif self._map.peekitem(lo)[0] != left:
                return slice(lo, lo)

            return slice(lo, lo + 1)
        else:
            hi = _map.bisect_right(right)
            return slice(lo, hi)


