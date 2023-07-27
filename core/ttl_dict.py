import sqlite3
from datetime import datetime, timedelta

import asyncio


class TTLDictionary:
    def __init__(self, db_path, default_ttl):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.default_ttl = default_ttl

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS expirable_dict 
                            (key TEXT PRIMARY KEY, 
                             value TEXT, 
                             expires_at TIMESTAMP)''')

    async def __setitem__(self, key, value, ttl=None):
        if ttl is None:
            ttl = self.default_ttl
        expires_at = datetime.now() + timedelta(seconds=ttl)
        self.cursor.execute("INSERT OR REPLACE INTO expirable_dict VALUES (?, ?, ?)", (key, value, expires_at))
        self.conn.commit()

    async def __getitem__(self, key):
        result = self.cursor.execute("SELECT value, expires_at FROM expirable_dict WHERE key=?", (key,)).fetchone()
        if result is None:
            raise KeyError(key)
        value, expires_at = result
        if datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S.%f') < datetime.now():
            self.cursor.execute("DELETE FROM expirable_dict WHERE key=?", (key,))
            self.conn.commit()
            raise KeyError(key)
        return value

    async def __contains__(self, key):
        result = self.cursor.execute("SELECT 1 FROM expirable_dict WHERE key=?", (key,)).fetchone()
        return result is not None

    async def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.cursor.execute("DELETE FROM expirable_dict WHERE key=?", (key,))
        self.conn.commit()

    async def keys(self):
        return [row[0] for row in self.cursor.execute("SELECT key FROM expirable_dict")]

    async def values(self):
        return [row[0] for row in self.cursor.execute("SELECT value FROM expirable_dict")]

    async def items(self):
        return [(row[0], row[1]) for row in self.cursor.execute("SELECT key, value FROM expirable_dict")]

    async def get(self, key, default=None):
        try:
            return await self.__getitem__(key)
        except KeyError:
            return default

    async def clear(self):
        self.cursor.execute("DELETE FROM expirable_dict")
        self.conn.commit()

    async def expiration_loop(self):
        while True:
            self.cursor.execute("DELETE FROM expirable_dict WHERE expires_at<?", (datetime.now(),))
            self.conn.commit()
            await asyncio.sleep(1)  # sleep for a while before next cleanup

    def start_expiration_loop(self):
        asyncio.run(self.expiration_loop())