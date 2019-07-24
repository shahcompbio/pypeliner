import sqlite3


class SqliteDb(object):
    """
    Wrapper class for interacting with sqlite databases as a key value store
    :param filename: path at which the db file will be stored
    :type filename: any
    """

    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (key text unique, value text)")
        self.c = self.conn.cursor()

    def close(self):
        """
        close the database connections
        """
        self.conn.commit()
        self.conn.close()

    def __len__(self):
        self.c.execute('SELECT COUNT(*) FROM kv')
        rows = self.c.fetchone()[0]
        return rows if rows is not None else 0

    def iterkeys(self):
        """
        iterator over the keys in database
        :return: key value
        :rtype: any
        """
        c1 = self.conn.cursor()
        for row in c1.execute('SELECT key FROM kv'):
            yield row[0]

    def itervalues(self):
        """
        iterator over values in the database
        :return: value in database
        :rtype: any
        """
        c2 = self.conn.cursor()
        for row in c2.execute('SELECT value FROM kv'):
            yield row[0]

    def iteritems(self):
        """
        iterator over keys and values
        :return: tuple of key and its value
        :rtype: tuple
        """
        c3 = self.conn.cursor()
        for row in c3.execute('SELECT key, value FROM kv'):
            yield row[0], row[1]

    def keys(self):
        """
        returns all keys
        :return: list of keys
        :rtype: list
        """
        return [v for v in self.iterkeys()]

    def values(self):
        """
        returns all values
        :return: list of values
        :rtype: list
        """
        return [v for v in self.itervalues()]

    def items(self):
        """
        all keys and values
        :return: list of tuple of key and its value
        :rtype: list
        """
        return [v for v in self.iteritems()]

    def delete(self, key):
        c3 = self.conn.cursor()
        c3.execute('DELETE FROM kv WHERE key = ?', (key,))


    def __contains__(self, key):
        """
        check if a key exists in database
        """
        self.c.execute('SELECT 1 FROM kv WHERE key = ?', (key,))
        return self.c.fetchone() is not None

    def __getitem__(self, key):
        self.c.execute('SELECT value FROM kv WHERE key = ?', (key,))
        item = self.c.fetchone()
        if item is None:
            raise KeyError(key)
        return item[0]

    def get(self, key, default=None):
        """
        get the key if it exists, return default otherwise
        :param key: key to look for
        :type key: any
        :param default: value to return if key doesnt exist
        :type default: any
        :return: value for the key or default
        :rtype: any
        """
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
