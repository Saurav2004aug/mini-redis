import random
import unittest

from helpers import FakeClock
from miniredis.resp import Error
from miniredis.store import ExpiryTable, Store


class TestExpiry(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock(1000)
        self.store = Store(clock_ms=self.clock, rng=random.Random(0))

    def test_lazy_expiry_on_access(self):
        self.store.set(b"k", b"v")
        self.store.set_expiry(b"k", 1500)
        self.clock.ms = 1499
        self.assertEqual(self.store.get(b"k"), b"v")
        self.clock.ms = 1500
        self.assertIsNone(self.store.get(b"k"))
        self.assertNotIn(b"k", self.store.data)       # memory actually freed
        self.assertEqual(self.store.expired_keys, 1)

    def test_ttl_semantics(self):
        self.assertEqual(self.store.ttl_ms(b"missing"), -2)
        self.store.set(b"k", b"v")
        self.assertEqual(self.store.ttl_ms(b"k"), -1)
        self.store.set_expiry(b"k", 3000)
        self.assertEqual(self.store.ttl_ms(b"k"), 2000)
        self.assertTrue(self.store.persist(b"k"))
        self.assertEqual(self.store.ttl_ms(b"k"), -1)

    def test_expiry_in_past_deletes_immediately(self):
        self.store.set(b"k", b"v")
        self.store.set_expiry(b"k", 0)
        self.assertNotIn(b"k", self.store.data)

    def test_set_clears_ttl_unless_keep_ttl(self):
        self.store.set(b"k", b"v")
        self.store.set_expiry(b"k", 5000)
        self.store.set(b"k", b"v2", keep_ttl=True)
        self.assertEqual(self.store.ttl_ms(b"k"), 4000)
        self.store.set(b"k", b"v3")
        self.assertEqual(self.store.ttl_ms(b"k"), -1)

    def test_active_expiry_reclaims_untouched_keys(self):
        """Keys nobody reads again must still be freed (no memory leak)."""
        for i in range(1000):
            self.store.set(b"tmp:%d" % i, b"x")
            self.store.set_expiry(b"tmp:%d" % i, 2000)
        for i in range(100):
            self.store.set(b"live:%d" % i, b"x")
        self.clock.ms = 5000
        for _ in range(20):  # 20 cycles = 2 seconds of server time
            self.store.active_expire_cycle()
        self.assertLess(len(self.store.expires), 50)
        self.assertEqual(sum(k.startswith(b"live") for k in self.store.data), 100)

    def test_active_expiry_does_not_scan_everything_when_few_expired(self):
        for i in range(10_000):
            self.store.set(b"k%d" % i, b"x")
            self.store.set_expiry(b"k%d" % i, 10**12)  # far future
        removed = self.store.active_expire_cycle()
        self.assertEqual(removed, 0)  # stops after one 20-key sample


class TestExpiryTable(unittest.TestCase):
    def test_matches_a_plain_dict_under_random_operations(self):
        rng = random.Random(3)
        table, ref = ExpiryTable(), {}
        for _ in range(20_000):
            key = b"k%d" % rng.randrange(300)
            op = rng.random()
            if op < 0.5:
                table[key] = ref[key] = rng.randrange(10**6)
            elif op < 0.9:
                self.assertEqual(table.pop(key, None), ref.pop(key, None))
            else:
                self.assertEqual(table.get(key), ref.get(key))
            self.assertEqual(len(table), len(ref))
        self.assertEqual(set(table), set(ref))
        # the internal array and position map must agree exactly
        for i, k in enumerate(table._keys):
            self.assertEqual(table._pos[k], i)

    def test_sample_is_distinct_and_bounded(self):
        table = ExpiryTable()
        for i in range(1000):
            table[b"%d" % i] = i
        s = table.sample(20, random.Random(0))
        self.assertEqual(len(s), 20)
        self.assertEqual(len(set(s)), 20)
        small = ExpiryTable()
        small[b"a"] = 1
        self.assertEqual(small.sample(20, random.Random(0)), [b"a"])


class TestTypes(unittest.TestCase):
    def test_wrongtype(self):
        s = Store()
        s.set(b"str", b"x")
        with self.assertRaises(Error) as ctx:
            s.get(b"str", dict)
        self.assertTrue(ctx.exception.message.startswith("WRONGTYPE"))

    def test_empty_collections_are_deleted(self):
        s = Store()
        h = s.get_or_create(b"h", dict)
        h[b"f"] = b"v"
        del h[b"f"]
        s.delete_if_empty(b"h")
        self.assertEqual(s.type_name(b"h"), "none")


if __name__ == "__main__":
    unittest.main()
