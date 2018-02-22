""" InfluxDB Query Object. """

import functools

from . import meta
from . import resultset


class InfluxDBQuery(object):
    """ InfluxDB Query object.

        entities    (tuple):          Query entities
        client      (InfluxAlchemy):  InfluxAlchemy instance
        expressions (tuple):          Query filters
        groupby     (str):            GROUP BY string
        limit       (int):            LIMIT int
    """
    def __init__(self, entities, client, expressions=None, groupby=None,
                 limit=None):
        # pylint: disable=too-many-arguments
        self._entities = entities
        self._client = client
        self._expressions = expressions or ()
        self._groupby = groupby
        self._limit = limit

        self._measurements = None

    def __str__(self):
        select = ", ".join(self._select)
        from_ = self._from
        where = " AND ".join(self._where)
        if any(where):
            iql = "SELECT %s FROM %s WHERE %s" % (select, from_, where)
        else:
            iql = "SELECT %s FROM %s" % (select, from_)
        if self._groupby is not None:
            iql += " GROUP BY %s" % self._groupby
        if self._limit is not None:
            iql += " LIMIT {0}".format(self._limit)
        return "%s;" % iql

    def __repr__(self):
        return str(self)

    def __iter__(self):
        rs = self._client.bind.query(str(self))
        if not rs:
            return None

        single_entity = len(self._measurements) == 1

        # a generator()
        try:
            self._client.reset_cursors()

            if single_entity:
                self._client._cursors.append(rs.get_points(measurement=self._from, tags=None))

                for raw_row in self._client._cursor[0]:
                    _cls = self._entities[0]
                    _cls = _cls.__new__(_cls)
                    _cls.__dict__.update(**raw_row)
                    yield _cls

            else:
                for ms in self._measurements:
                    self._client._cursors.append(rs.get_points(measurement=str(ms), tags=None))
                    self._client._cursors_name.append(str(ms))

                ent_idx = 0
                for cursor in self._client._cursors:
                    for raw_row in cursor:
                        try:
                            _cls = resultset.MultiResultSet(self._entities)
                            _cls.update(self._client._cursors_name[ent_idx], raw_row)
                        except StopIteration:
                            break

                        yield _cls

                    ent_idx += 1

        except StopIteration:
            return None
        except Exception as err:
            self._client.close()

        self._client.close()

        return None

    def all(self):
        return list(iter(self))

    def first(self):
        return next(iter(self), None)

    def execute(self):
        """ Execute query. """
        return self._client.bind.query(str(self))

    def filter(self, *expressions):
        """ Filter query. """
        expressions = self._expressions + expressions
        return InfluxDBQuery(self._entities, self._client,
                             expressions=expressions)

    def filter_by(self, **kwargs):
        """ Filter query by tag value. """
        expressions = self._expressions
        for key, val in sorted(kwargs.items()):
            expressions += (meta.TagExp.equals(key, val),)
        return InfluxDBQuery(self._entities, self._client,
                             expressions=expressions)

    def group_by(self, groupby):
        """ Group query. """
        return InfluxDBQuery(
            self._entities, self._client, self._expressions, groupby)

    def limit(self, limit):
        """ Limit query """
        assert isinstance(limit, int)
        return InfluxDBQuery(
            self._entities, self._client, self._expressions, self._groupby,
            limit)

    @property
    def measurement(self):
        """ Query measurement. """
        # remove the duplicated measurement
        #measurements = set(x.measurement for x in self._entities)
        self._measurements = set(x.measurement for x in self._entities)
        #print("self_.measurements")
        #print(self._measurements)
        return functools.reduce(lambda x, y: x | y, self._measurements)

    @property
    def _select(self):
        """ SELECT statement. """
        single_entity = len(self._entities) == 1
        selects = []
        for ent in self._entities:
            # Entity is a Tag
            if isinstance(ent, meta.Tag):
                selects.append(str(ent))
        return selects or ["*"]

    @property
    def _from(self):
        """ FROM statement. """
        return str(self.measurement)

    @property
    def _where(self):
        """ WHERE statement. """
        for exp in self._expressions:
            yield "(%s)" % exp
