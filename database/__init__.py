"""
Database package for UFDR forensic analysis
SQL-based storage with fast indexed queries
"""

from database.schema import (
    DatabaseManager, Base,
    Case, Device, Contact, Message, Call, Media, Location,
    normalize_phone_to_digits, extract_phone_suffix
)

from database.jsonl_to_sql import JSONLToSQLIngester
from database.sql_validator import SQLValidator
from database.query_executor import QueryExecutor, QueryResult, ForensicQueries

__all__ = [
    'DatabaseManager',
    'Base',
    'Case',
    'Device',
    'Contact',
    'Message',
    'Call',
    'Media',
    'Location',
    'normalize_phone_to_digits',
    'extract_phone_suffix',
    'JSONLToSQLIngester',
    'SQLValidator',
    'QueryExecutor',
    'QueryResult',
    'ForensicQueries',
]
