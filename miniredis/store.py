"""
The keyspace: values plus expiry deadlines.

Types stored:
    bytes            -> Redis string
    collections.deque -> Redis list
    dict             -> Redis hash
    set              -> Redis set

Expiry uses Redis's own two-part strategy:
  1. Lazy expiry: every key access checks its deadline first, so an expired
     key is never returned.
  2. Active expiry: a background cycle samples 20 random keys that have a TTL,
     deletes the expired ones, and repeats while more than 25% of the sample was
     expired. Memory is reclaimed even for keys nobody reads again, and the
     cycle never scans the whole keyspace. Keys with a TTL live in an
     ExpiryTable (array + position map), so drawing a random sample is O(k),
     not O(number of keys).
"""

import random
import time
from collections import deque
from typing import Callable, Optional

from .resp import Error

WRONGTYPE = Error("WRONGTYPE Operation against a key holding the wrong kind of value")

TYPE_NAMES = {bytes: "string", deque: "list", dict: "hash", set: "set"}


def now_ms() -> int:
    return int(time.time() * 1000)


class ExpiryTable:
    """Dict-like map of key -> deadline (ms) that also supports O(1) random
    sampling. Keys are kept in an array plus a key -> index map; deletion
    swaps the last element into the hole (swap-remove), so every operation is
    O(1). This is the classic "insert/delete/getRandom in O(1)" structure
    (LeetCode 380)."""

    def __init__(self):
        self._deadline: dict[bytes, int] = {}
        self._keys: list[bytes] = []
        self._pos: dict[bytes, int] = {}

    def __setitem__(self, key: bytes, deadline: int) -> None:
        if key not in self._deadline:
            self._pos[key] = len(self._keys)
            self._keys.append(key)
        self._deadline[key] = deadline

    def __getitem__(self, key: bytes) -> int:
        return self._deadline[key]

    def get(self, key: bytes, default=None):
        return self._deadline.get(key, default)

    def __contains__(self, key) -> bool:
        return key in self._deadline

    def __len__(self) -> int:
        return len(self._keys)

    def __iter__(self):
        return iter(list(self._keys))

    def pop(self, key: bytes, default=None):
        if key not in self._deadline:
            return default
        deadline = self._deadline.pop(key)
        i = self._pos.pop(key)
        last = self._keys.pop()
        if i < len(self._keys):          # move the last key into the freed slot
            self._keys[i] = last
            self._pos[last] = i
        return deadline

    def clear(self) -> None:
        self._deadline.clear()
        self._keys.clear()
        self._pos.clear()

    def sample(self, k: int, rng: random.Random) -> list[bytes]:
        """k distinct random keys in O(k). random.sample over a range object
        doesn't materialize the range."""
        n = len(self._keys)
        if n <= k:
            return list(self._keys)
        return [self._keys[i] for i in rng.sample(range(n), k)]


class Store:
    ACTIVE_EXPIRE_SAMPLE = 20
    ACTIVE_EXPIRE_REPEAT_THRESHOLD = 0.25

    def __init__(self, clock_ms: Callable[[], int] = now_ms, rng: Optional[random.Random] = None):
        self.data: dict[bytes, object] = {}
        self.expires = ExpiryTable()  # key -> absolute unix time in ms
        self.clock_ms = clock_ms
        self.rng = rng or random.Random()
        self.expired_keys = 0  # stat for INFO

    # ---- expiry ----

    def _expire_if_needed(self, key: bytes) -> bool:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= self.clock_ms():
            self.delete(key)
            self.expired_keys += 1
            return True
        return False

    def active_expire_cycle(self, max_rounds: int = 16) -> int:
        """Probabilistic sweep of keys with a TTL. Returns the number removed."""
        removed = 0
        for _ in range(max_rounds):
            if not self.expires:
                break
            sample = self.expires.sample(self.ACTIVE_EXPIRE_SAMPLE, self.rng)
            expired = sum(self._expire_if_needed(k) for k in sample)
            removed += expired
            if expired / len(sample) <= self.ACTIVE_EXPIRE_REPEAT_THRESHOLD:
                break
        return removed

    def set_expiry(self, key: bytes, deadline_ms: int) -> bool:
        if not self.exists(key):
            return False
        if deadline_ms <= self.clock_ms():
            self.delete(key)
        else:
            self.expires[key] = deadline_ms
        return True

    def ttl_ms(self, key: bytes) -> int:
        """-2 if the key doesn't exist, -1 if it has no TTL, else ms remaining."""
        if not self.exists(key):
            return -2
        deadline = self.expires.get(key)
        return -1 if deadline is None else max(0, deadline - self.clock_ms())

    def persist(self, key: bytes) -> bool:
        return self.exists(key) and self.expires.pop(key, None) is not None

    # ---- generic access ----

    def exists(self, key: bytes) -> bool:
        return key in self.data and not self._expire_if_needed(key)

    def get(self, key: bytes, expected_type: Optional[type] = None):
        if not self.exists(key):
            return None
        value = self.data[key]
        if expected_type is not None and type(value) is not expected_type:
            raise WRONGTYPE
        return value

    def get_or_create(self, key: bytes, factory: type):
        value = self.get(key, factory)
        if value is None:
            value = self.data[key] = factory()
        return value

    def set(self, key: bytes, value, keep_ttl: bool = False) -> None:
        self.data[key] = value
        if not keep_ttl:
            self.expires.pop(key, None)

    def delete(self, key: bytes) -> bool:
        self.expires.pop(key, None)
        return self.data.pop(key, None) is not None

    def delete_if_empty(self, key: bytes) -> None:
        """Redis deletes a list/hash/set automatically when it becomes empty."""
        value = self.data.get(key)
        if value is not None and not isinstance(value, bytes) and len(value) == 0:
            self.delete(key)

    def keys(self):
        return [k for k in list(self.data) if self.exists(k)]

    def type_name(self, key: bytes) -> str:
        value = self.get(key)
        return "none" if value is None else TYPE_NAMES[type(value)]

    def flush(self) -> None:
        self.data.clear()
        self.expires.clear()

    def __len__(self):
        return len(self.data)
