"""
SQL Query Executor with Provenance Tracking
Executes validated SQL queries and tracks provenance for forensic audit trail
"""

import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import text

from database.schema import DatabaseManager
from database.sql_validator import SQLValidator

logger = logging.getLogger(__name__)


class QueryResult:
    """Container for query results with metadata"""
    
    def __init__(self,
                 rows: List[Dict],
                 columns: List[str],
                 row_count: int,
                 execution_time: float,
                 sql: str,
                 query_id: str,
                 timestamp: datetime):
        self.rows = rows
        self.columns = columns
        self.row_count = row_count
        self.execution_time = execution_time
        self.sql = sql
        self.query_id = query_id
        self.timestamp = timestamp
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert results to pandas DataFrame"""
        if not self.rows:
            return pd.DataFrame()
        return pd.DataFrame(self.rows)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'rows': self.rows,
            'columns': self.columns,
            'row_count': self.row_count,
            'execution_time': self.execution_time,
            'sql': self.sql,
            'query_id': self.query_id,
            'timestamp': self.timestamp.isoformat()
        }


class QueryExecutor:
    """
    Executes SQL queries with safety validation and audit logging
    """
    
    def __init__(self,
                 db_path: str = "data/forensics.db",
                 audit_log_path: Optional[str] = "data/query_audit.log"):
        """
        Initialize query executor
        
        Args:
            db_path: Path to SQLite database
            audit_log_path: Path to audit log file (None to disable)
        """
        self.db_manager = DatabaseManager(db_path)
        self.validator = SQLValidator()
        self.audit_log_path = audit_log_path
        
        # Setup audit logging
        if audit_log_path:
            self._setup_audit_log()
    
    def _setup_audit_log(self):
        """Setup audit log file"""
        log_file = Path(self.audit_log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create audit logger
        self.audit_logger = logging.getLogger('query_audit')
        self.audit_logger.setLevel(logging.INFO)
        
        # File handler for audit log
        handler = logging.FileHandler(self.audit_log_path, encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.audit_logger.addHandler(handler)
        self.audit_logger.propagate = False
    
    def execute(self,
                sql: str,
                user: str = "system",
                validate: bool = True,
                sanitize: bool = True) -> QueryResult:
        """
        Execute SQL query with validation and audit logging
        
        Args:
            sql: SQL query to execute
            user: User executing the query (for audit log)
            validate: Whether to validate query before execution
            sanitize: Whether to sanitize query (add LIMIT, etc.)
            
        Returns:
            QueryResult with data and metadata
            
        Raises:
            ValueError: If query fails validation
            Exception: If execution fails
        """
        query_id = self._generate_query_id()
        start_time = time.time()
        
        try:
            # Sanitize if requested
            if sanitize:
                sql = self.validator.sanitize(sql)
            
            # Validate if requested
            if validate:
                is_valid, error = self.validator.validate(sql)
                if not is_valid:
                    self._log_audit(query_id, user, sql, "REJECTED", error)
                    raise ValueError(f"Query validation failed: {error}")
            
            # Execute query
            session = self.db_manager.get_session()
            
            try:
                # Wrap SQL in text() for SQLAlchemy 2.x compatibility
                result_proxy = session.execute(text(sql))
                
                # Fetch results
                if result_proxy.returns_rows:
                    rows = []
                    columns = list(result_proxy.keys())
                    
                    for row in result_proxy:
                        row_dict = dict(zip(columns, row))
                        rows.append(row_dict)
                    
                    row_count = len(rows)
                else:
                    rows = []
                    columns = []
                    row_count = 0
                
                execution_time = time.time() - start_time
                
                # Create result object
                result = QueryResult(
                    rows=rows,
                    columns=columns,
                    row_count=row_count,
                    execution_time=execution_time,
                    sql=sql,
                    query_id=query_id,
                    timestamp=datetime.now()
                )
                
                # Log successful execution
                self._log_audit(
                    query_id, user, sql, "SUCCESS",
                    f"{row_count} rows, {execution_time:.3f}s"
                )
                
                # Use DEBUG level to avoid console spam on dashboard
                logger.debug(f"Query {query_id}: {row_count} rows in {execution_time:.3f}s")
                
                return result
                
            finally:
                session.close()
        
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            
            # Log failure
            self._log_audit(
                query_id, user, sql, "FAILED",
                f"Error: {error_msg}"
            )
            
            logger.error(f"Query {query_id} failed: {error_msg}")
            raise
    
    def execute_and_format(self,
                          sql: str,
                          format: str = "dict",
                          user: str = "system") -> Any:
        """
        Execute query and return in specified format
        
        Args:
            sql: SQL query
            format: Output format ('dict', 'dataframe', 'list', 'json')
            user: User executing query
            
        Returns:
            Results in requested format
        """
        result = self.execute(sql, user=user)
        
        if format == "dataframe" or format == "df":
            return result.to_dataframe()
        elif format == "list":
            return result.rows
        elif format == "json":
            return result.to_dict()
        else:  # dict
            return {
                'data': result.rows,
                'columns': result.columns,
                'row_count': result.row_count,
                'execution_time': result.execution_time
            }
    
    def _generate_query_id(self) -> str:
        """Generate unique query ID"""
        return f"Q{int(time.time() * 1000)}"
    
    def _log_audit(self,
                   query_id: str,
                   user: str,
                   sql: str,
                   status: str,
                   details: str = ""):
        """Log query execution to audit log"""
        if not hasattr(self, 'audit_logger'):
            return
        
        # Clean SQL for logging (single line)
        sql_clean = ' '.join(sql.split())
        if len(sql_clean) > 200:
            sql_clean = sql_clean[:200] + "..."
        
        log_entry = f"{query_id} | {user} | {status} | {sql_clean}"
        if details:
            log_entry += f" | {details}"
        
        self.audit_logger.info(log_entry)


# Helper functions for common queries
class ForensicQueries:
    """Pre-built forensic analysis queries"""
    
    @staticmethod
    def get_phone_suffix(case_id: str, suffix: str, limit: int = 100) -> str:
        """Get messages with phone numbers ending with suffix"""
        suffix_len = len(suffix)
        return f"""
        SELECT sender_raw AS phone, sender_digits, msg_id, text, timestamp, source_path 
        FROM messages 
        WHERE case_id='{case_id}' AND sender_suffix_{suffix_len}='{suffix}' 
        ORDER BY timestamp DESC 
        LIMIT {limit};
        """
    
    @staticmethod
    def get_phone_prefix(case_id: str, prefix: str, limit: int = 100) -> str:
        """Get messages with phone numbers starting with prefix"""
        return f"""
        SELECT sender_raw AS phone, sender_digits, msg_id, timestamp 
        FROM messages 
        WHERE case_id='{case_id}' AND sender_digits LIKE '{prefix}%' 
        ORDER BY timestamp DESC 
        LIMIT {limit};
        """
    
    @staticmethod
    def get_text_search(case_id: str, search_term: str, limit: int = 100) -> str:
        """Search messages by text content"""
        return f"""
        SELECT msg_id, sender_raw, receiver_raw, text, timestamp, source_path 
        FROM messages 
        WHERE case_id='{case_id}' AND text LIKE '%{search_term}%' 
        ORDER BY timestamp DESC 
        LIMIT {limit};
        """
    
    @staticmethod
    def get_call_summary(case_id: str) -> str:
        """Get call summary statistics"""
        return f"""
        SELECT 
            COUNT(*) as total_calls,
            SUM(duration_seconds) as total_duration,
            AVG(duration_seconds) as avg_duration,
            MAX(duration_seconds) as max_duration
        FROM calls 
        WHERE case_id='{case_id}'
        LIMIT 1;
        """


# Test function
def test_executor():
    """Test query executor"""
    print("=" * 80)
    print(" Query Executor Test")
    print("=" * 80)
    print()
    
    executor = QueryExecutor("data/forensics_test.db")
    
    # Test 1: Valid query
    print("[1/3] Testing valid query...")
    try:
        sql = "SELECT sender_raw, msg_id FROM messages WHERE sender_suffix_2='20' LIMIT 5;"
        result = executor.execute(sql, user="test_user")
        print(f"✅ SUCCESS: Got {result.row_count} rows in {result.execution_time:.3f}s")
        print(f"  Query ID: {result.query_id}")
        if result.rows:
            print(f"  Sample: {result.rows[0]}")
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print()
    
    # Test 2: Invalid query (should be rejected)
    print("[2/3] Testing invalid query (should reject)...")
    try:
        sql = "DROP TABLE messages;"
        result = executor.execute(sql, user="test_user")
        print("❌ FAIL: Invalid query was not rejected!")
    except ValueError as e:
        print(f"✅ SUCCESS: Query rejected as expected")
        print(f"  Reason: {e}")
    print()
    
    # Test 3: Query with sanitization
    print("[3/3] Testing query sanitization...")
    try:
        sql = "SELECT * FROM messages"  # Missing LIMIT
        result = executor.execute(sql, user="test_user", sanitize=True)
        print(f"✅ SUCCESS: Query auto-sanitized and executed")
        print(f"  Rows: {result.row_count}")
        print(f"  Sanitized SQL: {result.sql[:100]}...")
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print()
    
    print("=" * 80)
    print("Check data/query_audit.log for audit trail")


if __name__ == "__main__":
    test_executor()