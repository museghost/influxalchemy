
class MultiResultSet(object):
    def __init__(self, entities):
        self._entities = entities
        self.raw = None
        self.measurement = ""

    def update(self, measurement, data):
        self.measurement = measurement
        self.raw = data.copy()
        for k, v in self.raw.items():
            if v:
                self.__setattr__(str(k), v)
