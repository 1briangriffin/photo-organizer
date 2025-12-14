import pytest
import sqlite3
from photo_organizer.database.schema import init_schema
from photo_organizer.database.ops import DBOperations

@pytest.fixture
def conn():
    """Returns an in-memory SQLite connection with the schema initialized."""
    c = sqlite3.connect(":memory:")
    init_schema(c)
    try:
        yield c
    finally:
        c.close()

@pytest.fixture
def db_ops(conn):
    """Returns a DBOperations instance attached to the in-memory DB."""
    return DBOperations(conn)