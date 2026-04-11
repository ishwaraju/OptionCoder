from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from config import Config


class DBPool:
    _pool = None
    _initialized = False
    _enabled = False

    @classmethod
    def initialize(cls):
        if cls._initialized:
            return cls._enabled

        cls._initialized = True
        cls._enabled = Config.DB_ENABLED
        if not cls._enabled:
            return False

        try:
            cls._pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=Config.get_db_dsn(),
            )
            cls._enabled = True
            print("DB pool initialized successfully")
        except Exception as exc:
            cls._enabled = False
            cls._pool = None
            print("DB pool initialization failed:", exc)

        return cls._enabled

    @classmethod
    @contextmanager
    def connection(cls):
        if not cls.initialize() or cls._pool is None:
            yield None
            return

        conn = None
        try:
            conn = cls._pool.getconn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'Asia/Kolkata'")
            yield conn
        finally:
            if conn is not None and cls._pool is not None:
                cls._pool.putconn(conn)

    @classmethod
    def close_all(cls):
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None
        cls._initialized = False
