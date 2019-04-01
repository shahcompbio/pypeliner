
import sqlite3


class SqliteDb(object):
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (key text unique, value text)")
        self.c = self.conn.cursor()

    def close(self):
        self.conn.commit()
        self.conn.close()

    def __len__(self):
        self.c.execute('SELECT COUNT(*) FROM kv')
        rows = self.c.fetchone()[0]
        return rows if rows is not None else 0

    def iterkeys(self):
        c1 = self.conn.cursor()
        for row in c1.execute('SELECT key FROM kv'):
            yield row[0]

    def itervalues(self):
        c2 = self.conn.cursor()
        for row in c2.execute('SELECT value FROM kv'):
            yield row[0]

    def iteritems(self):
        c3 = self.conn.cursor()
        for row in c3.execute('SELECT key, value FROM kv'):
            yield row[0], row[1]

    def keys(self):
        return list(self.keys())

    def values(self):
        return list(self.values())

    def items(self):
        return list(self.items())

    def __contains__(self, key):
        self.c.execute('SELECT 1 FROM kv WHERE key = ?', (key,))
        return self.c.fetchone() is not None

    def __getitem__(self, key):
        self.c.execute('SELECT value FROM kv WHERE key = ?', (key,))
        item = self.c.fetchone()
        if item is None:
            raise KeyError(key)
        return item[0]

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __setitem__(self, key, value):
        self.c.execute('REPLACE INTO kv (key, value) VALUES (?,?)', (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.c.execute('DELETE FROM kv WHERE key = ?', (key,))
        self.conn.commit()

    def __iter__(self):
        return iter(self.items())
