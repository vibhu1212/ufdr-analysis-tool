"""
SQL Query Validator
Ensures queries are safe, read-only, and conform to security policies
"""

import re
import sqlparse
from typing import Tuple, List, Optional
import logging

logger = logging.getLogger(__name__)


class SQLValidator:
    """
    Validates SQL queries for safety before execution
    Enforces read-only access and prevents SQL injection
    """
    
    # Allowed SQL keywords (read-only operations)
    ALLOWED_KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT',  
        'ORDER', 'BY', 'LIMIT', 'OFFSET', 'AS',
        'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
        'GROUP', 'HAVING', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
        'INNER', 'LEFT', 'RIGHT', 'OUTER', 'JOIN', 'ON',
        'UNION', 'INTERSECT', 'EXCEPT',
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'CAST', 'SUBSTR', 'LENGTH', 'UPPER', 'LOWER', 'TRIM',
        'DATE', 'DATETIME', 'TIMESTAMP', 'NOW', 'CURRENT_TIMESTAMP'
    }
    
    # Forbidden keywords (write operations)
    FORBIDDEN_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
        'TRUNCATE', 'REPLACE', 'GRANT', 'REVOKE',
        'ATTACH', 'DETACH', 'PRAGMA', 'VACUUM',
        'EXEC', 'EXECUTE', 'PROCEDURE'  # Note: 'CALL' removed to allow 'calls' table
    }
    
    # Allowed tables from our schema
    ALLOWED_TABLES = {
        'cases', 'devices', 'contacts', 'messages',
        'calls', 'media', 'locations'
    }
    
    # Maximum allowed rows
    MAX_LIMIT = 1000
    DEFAULT_LIMIT = 100
    
    def __init__(self, strict_mode: bool = True):
        """
        Initialize validator
        
        Args:
            strict_mode: If True, enforce stricter validation rules
        """
        self.strict_mode = strict_mode
    
    def validate(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL query for safety
        
        Args:
            sql: SQL query string to validate
            
        Returns:
            Tuple of (is_valid, error_message)
            If valid, error_message is None
            If invalid, error_message explains why
        """
        # Basic checks
        if not sql or not sql.strip():
            return False, "Empty query"
        
        sql = sql.strip()
        
        # Check 1: No multiple statements (prevent SQL injection)
        if self._has_multiple_statements(sql):
            return False, "Multiple statements not allowed (use single SELECT only)"
        
        # Check 2: Must start with SELECT
        if not sql.upper().startswith('SELECT'):
            return False, "Only SELECT queries allowed"
        
        # Check 3: No forbidden keywords
        forbidden_found = self._check_forbidden_keywords(sql)
        if forbidden_found:
            return False, f"Forbidden keyword detected: {forbidden_found}"
        
        # Check 4: Validate table names
        invalid_tables = self._check_table_names(sql)
        if invalid_tables:
            return False, f"Invalid table(s): {', '.join(invalid_tables)}"
        
        # Check 5: Enforce LIMIT clause
        if not self._has_limit_clause(sql):
            return False, f"LIMIT clause required (max {self.MAX_LIMIT} rows)"
        
        # Check 6: Validate LIMIT value
        limit_value = self._extract_limit_value(sql)
        if limit_value and limit_value > self.MAX_LIMIT:
            return False, f"LIMIT too high (max {self.MAX_LIMIT}, got {limit_value})"
        
        # Check 7: No comments (can hide malicious code)
        if self._has_comments(sql):
            return False, "SQL comments not allowed"
        
        # Check 8: Basic syntax check
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return False, "Invalid SQL syntax"
        except Exception as e:
            return False, f"SQL parsing error: {str(e)}"
        
        # All checks passed
        return True, None
    
    def sanitize(self, sql: str) -> str:
        """
        Sanitize SQL query by adding/fixing safety constraints
        
        Args:
            sql: Input SQL query
            
        Returns:
            Sanitized SQL query
        """
        sql = sql.strip()
        
        # Remove trailing semicolon
        sql = sql.rstrip(';')
        
        # Add LIMIT if missing
        if not self._has_limit_clause(sql):
            sql = f"{sql} LIMIT {self.DEFAULT_LIMIT}"
        
        # Fix LIMIT if too high
        limit_value = self._extract_limit_value(sql)
        if limit_value and limit_value > self.MAX_LIMIT:
            sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {self.MAX_LIMIT}', sql, flags=re.IGNORECASE)
        
        # Add semicolon at end
        sql = f"{sql};"
        
        return sql
    
    def _has_multiple_statements(self, sql: str) -> bool:
        """Check if query contains multiple statements"""
        # Remove string literals to avoid false positives
        sql_no_strings = re.sub(r"'[^']*'", '', sql)
        sql_no_strings = re.sub(r'"[^"]*"', '', sql_no_strings)
        
        # Count semicolons
        semicolons = sql_no_strings.count(';')
        
        # One trailing semicolon is OK
        if semicolons == 1 and sql_no_strings.rstrip().endswith(';'):
            return False
        
        return semicolons > 1
    
    def _check_forbidden_keywords(self, sql: str) -> Optional[str]:
        """Check for forbidden SQL keywords"""
        sql_upper = sql.upper()
        
        for keyword in self.FORBIDDEN_KEYWORDS:
            # Use word boundaries to avoid false positives
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, sql_upper):
                return keyword
        
        return None
    
    def _check_table_names(self, sql: str) -> List[str]:
        """Extract and validate table names"""
        # Simple regex to find table names after FROM or JOIN
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        
        invalid = []
        for table in matches:
            if table.lower() not in self.ALLOWED_TABLES:
                invalid.append(table)
        
        return invalid
    
    def _has_limit_clause(self, sql: str) -> bool:
        """Check if query has LIMIT clause"""
        return bool(re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE))
    
    def _extract_limit_value(self, sql: str) -> Optional[int]:
        """Extract LIMIT value from query"""
        match = re.search(r'\bLIMIT\s+(\d+)', sql, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
    
    def _has_comments(self, sql: str) -> bool:
        """Check for SQL comments"""
        # Check for -- comments
        if '--' in sql:
            return True
        # Check for /* */ comments
        if '/*' in sql or '*/' in sql:
            return True
        return False


# Test function
def test_validator():
    """Test SQL validator"""
    validator = SQLValidator()
    
    test_cases = [
        # Valid queries
        ("SELECT * FROM messages WHERE case_id='TEST' LIMIT 10;", True),
        ("SELECT sender_raw, text FROM messages LIMIT 50;", True),
        ("SELECT COUNT(*) FROM calls WHERE timestamp > '2024-01-01' LIMIT 1;", True),
        
        # Invalid queries
        ("INSERT INTO messages VALUES ('test');", False),
        ("SELECT * FROM messages; DROP TABLE messages;", False),
        ("SELECT * FROM messages", False),  # No LIMIT
        ("SELECT * FROM messages LIMIT 5000;", False),  # LIMIT too high
        ("SELECT * FROM secret_table LIMIT 10;", False),  # Invalid table
        ("UPDATE messages SET text='hacked' WHERE id=1;", False),
        ("SELECT * FROM messages -- comment\nLIMIT 10;", False),  # Has comment
    ]
    
    print("=" * 80)
    print(" SQL Validator Test")
    print("=" * 80)
    print()
    
    passed = 0
    failed = 0
    
    for sql, expected_valid in test_cases:
        is_valid, error = validator.validate(sql)
        status = "✅ PASS" if is_valid == expected_valid else "❌ FAIL"
        
        if is_valid == expected_valid:
            passed += 1
        else:
            failed += 1
        
        print(f"{status}")
        print(f"  SQL: {sql[:60]}...")
        print(f"  Expected: {'Valid' if expected_valid else 'Invalid'}")
        print(f"  Got: {'Valid' if is_valid else 'Invalid'}")
        if error:
            print(f"  Error: {error}")
        print()
    
    print(f"Results: {passed} passed, {failed} failed")


if __name__ == "__main__":
    test_validator()