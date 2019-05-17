class ImportMetric(object):
    def __init__(self, metricq_name, import_name, dataheap_name=None, sampling_rate=1, interval_factor=10,
                 interval_min=None, interval_max=None):
        self.metricq_name = metricq_name
        self.import_name = import_name
        self.dataheap_name = dataheap_name

        self.interval_factor = interval_factor
        self.interval_min = interval_min
        self.interval_max = interval_max
        self.sampling_rate = sampling_rate

        if self.interval_min is None:
            self.interval_min = sampling_rate * 40 * 1e9
        if self.interval_max is None:
            self.interval_max = self._default_interval_max()

    def _default_interval_max(self):
        i = self.interval_min
        assert i > 0
        while True:
            if i * self.interval_factor >= 2.592e15:
                return i
            i *= self.interval_factor

    @property
    def config(self):
        return {
            "mode": "RW",
            "interval_min": int(self.interval_min),
            "interval_max": int(self.interval_max),
            "interval_factor": self.interval_factor,
        }

    def __str__(self):
        return f'{self.import_name} => {self.metricq_name}, {self.interval_min:,}, {self.interval_max:,}, {self.interval_factor}'
