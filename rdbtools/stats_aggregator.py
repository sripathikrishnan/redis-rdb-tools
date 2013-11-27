import json

from collections import defaultdict


class StatsAggregator(object):

    _RECORD_TYPE_TO_HEADING = {
        'list': 'list_memory_by_length',
        'hash': 'hash_memory_by_length',
        'set': 'set_memory_by_length',
        'sortedset': 'sortedset_memory_by_length',
        'string': 'string_memory_by_length'
    }

    def __init__(self):
        self.aggregates = defaultdict(lambda: defaultdict(int))
        self.histograms = defaultdict(lambda: defaultdict(int))
        self.scatters = defaultdict(list)

    def next_record(self, record):
        self.add_aggregate('database_memory', record.database, record.bytes)
        self.add_aggregate('type_memory', record.type, record.bytes)
        self.add_aggregate('encoding_memory', record.encoding, record.bytes)

        self.add_aggregate('type_count', record.type, 1)
        self.add_aggregate('encoding_count', record.encoding, 1)

        self.add_histogram(record.type + '_length', record.size)
        self.add_histogram(record.type + '_memory', (record.bytes / 10) * 10)

        scatter_heading = self._RECORD_TYPE_TO_HEADING.get(record.type)
        if scatter_heading:
            self.add_scatter(scatter_heading, record.bytes, record.size)
        else:
            raise Exception('Invalid data type %s' % record.type)

    def add_aggregate(self, heading, subheading, metric):
        self.aggregates[heading][subheading] += metric

    def add_histogram(self, heading, metric):
        self.histograms[heading][metric] += 1

    def add_scatter(self, heading, x, y):
        self.scatters[heading].append([x, y])

    def get_json(self):
        return json.dumps({'aggregates': self.aggregates,
                           'scatters': self.scatters,
                           'histograms': self.histograms})
