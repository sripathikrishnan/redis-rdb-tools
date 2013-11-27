from unittest import TestCase

from rdbtools import StatsAggregator


class TestStatsAggregator(TestCase):

    def setUp(self):
        self._stats = StatsAggregator()

    def test_add_aggregate(self):
        self._stats.add_aggregate('type_count', 'hash', 1)
        self._stats.add_aggregate('type_count', 'hash', 2)

        self.assertEqual(3, self._stats.aggregates['type_count']['hash'])

    def test_add_histogram(self):
        self._stats.add_histogram('hash_length', 12)
        self.assertEqual(1, self._stats.histograms['hash_length'][12])

        self._stats.add_histogram('hash_length', 12)
        self.assertEqual(2, self._stats.histograms['hash_length'][12])

    def test_add_scatter(self):
        self._stats.add_scatter('set_memory_by_length', 8, 32)
        self.assertEqual([[8, 32]],
                         self._stats.scatters['set_memory_by_length'])
