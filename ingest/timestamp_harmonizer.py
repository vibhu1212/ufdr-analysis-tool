"""
Step 9/10: Timestamp Harmonization
Normalize timestamps across different formats to ISO 8601 standard
"""

import re
from datetime import datetime, timezone, timedelta
from typing import Union, Optional
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TimestampFormat(Enum):
    """Common timestamp formats"""
    ISO_8601 = "iso_8601"  # 2021-01-01T00:00:00Z
    UNIX_SECONDS = "unix_seconds"  # 1609459200
    UNIX_MILLISECONDS = "unix_milliseconds"  # 1609459200000
    HUMAN_READABLE = "human_readable"  # Jan 1, 2021 12:00 AM
    DATE_ONLY = "date_only"  # 2021-01-01
    RELATIVE = "relative"  # 2 days ago
    UNKNOWN = "unknown"


@dataclass
class TimestampResult:
    """Result of timestamp normalization"""
    original: str
    normalized: str  # ISO 8601 format
    format_detected: TimestampFormat
    timezone: str
    confidence: float
    error: Optional[str] = None


class TimestampHarmonizer:
    """
    Normalize timestamps to ISO 8601 format with timezone handling
    """
    
    # Common timezone abbreviations
    TIMEZONE_MAP = {
        'IST': '+05:30',  # India Standard Time
        'UTC': '+00:00',
        'GMT': '+00:00',
        'EST': '-05:00',  # Eastern Standard Time
        'EDT': '-04:00',  # Eastern Daylight Time
        'PST': '-08:00',  # Pacific Standard Time
        'PDT': '-07:00',  # Pacific Daylight Time
        'CST': '-06:00',  # Central Standard Time
        'CDT': '-05:00',  # Central Daylight Time
    }
    
    def __init__(self, default_timezone: str = 'UTC'):
        """
        Initialize timestamp harmonizer
        
        Args:
            default_timezone: Default timezone for ambiguous timestamps
        """
        self.default_timezone = default_timezone
        
        # Compile regex patterns for efficiency
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for timestamp detection"""
        # Unix timestamp (10 digits for seconds, 13 for milliseconds)
        self.unix_pattern = re.compile(r'^\d{10,13}$')
        
        # ISO 8601: 2021-01-01T00:00:00Z or 2021-01-01T00:00:00+05:30
        self.iso_pattern = re.compile(
            r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$'
        )
        
        # Date only: 2021-01-01 or 2021/01/01 or 01-01-2021
        self.date_only_pattern = re.compile(
            r'^(\d{4})[-/](\d{2})[-/](\d{2})$|^(\d{2})[-/](\d{2})[-/](\d{4})$'
        )
        
        # Relative: "2 days ago", "3 hours ago", "yesterday"
        self.relative_pattern = re.compile(
            r'^(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago$|^(yesterday|today)$',
            re.IGNORECASE
        )
    
    def normalize(self, timestamp: Union[str, int, float, datetime],
                  reference_time: Optional[datetime] = None) -> TimestampResult:
        """
        Normalize timestamp to ISO 8601 format
        
        Args:
            timestamp: Input timestamp in various formats
            reference_time: Reference time for relative timestamps
            
        Returns:
            TimestampResult with normalized timestamp
        """
        original_str = str(timestamp)
        
        try:
            # Handle different input types
            if isinstance(timestamp, datetime):
                return self._normalize_datetime(timestamp, original_str)
            elif isinstance(timestamp, (int, float)):
                return self._normalize_numeric(timestamp, original_str)
            elif isinstance(timestamp, str):
                return self._normalize_string(timestamp, reference_time or datetime.now(timezone.utc))
            else:
                return TimestampResult(
                    original=original_str,
                    normalized='',
                    format_detected=TimestampFormat.UNKNOWN,
                    timezone=self.default_timezone,
                    confidence=0.0,
                    error=f"Unsupported type: {type(timestamp)}"
                )
        
        except Exception as e:
            logger.error(f"Timestamp normalization failed for '{original_str}': {e}")
            return TimestampResult(
                original=original_str,
                normalized='',
                format_detected=TimestampFormat.UNKNOWN,
                timezone=self.default_timezone,
                confidence=0.0,
                error=str(e)
            )
    
    def _normalize_datetime(self, dt: datetime, original: str) -> TimestampResult:
        """Normalize datetime object"""
        # Ensure timezone aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to ISO 8601
        normalized = dt.isoformat()
        
        return TimestampResult(
            original=original,
            normalized=normalized,
            format_detected=TimestampFormat.ISO_8601,
            timezone=str(dt.tzinfo),
            confidence=1.0
        )
    
    def _normalize_numeric(self, timestamp: Union[int, float], original: str) -> TimestampResult:
        """Normalize Unix timestamp (seconds or milliseconds)"""
        # Determine if seconds or milliseconds
        if timestamp > 1e12:  # Likely milliseconds
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            format_detected = TimestampFormat.UNIX_MILLISECONDS
        else:  # Likely seconds
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            format_detected = TimestampFormat.UNIX_SECONDS
        
        normalized = dt.isoformat()
        
        return TimestampResult(
            original=original,
            normalized=normalized,
            format_detected=format_detected,
            timezone='UTC',
            confidence=1.0
        )
    
    def _normalize_string(self, timestamp_str: str, reference_time: datetime) -> TimestampResult:
        """Normalize string timestamp"""
        timestamp_str = timestamp_str.strip()
        
        # Try Unix timestamp
        if self.unix_pattern.match(timestamp_str):
            return self._normalize_numeric(int(timestamp_str), timestamp_str)
        
        # Try ISO 8601
        if self.iso_pattern.match(timestamp_str):
            return self._parse_iso8601(timestamp_str)
        
        # Try date only
        if self.date_only_pattern.match(timestamp_str):
            return self._parse_date_only(timestamp_str)
        
        # Try relative
        if self.relative_pattern.match(timestamp_str):
            return self._parse_relative(timestamp_str, reference_time)
        
        # Try common human-readable formats
        return self._parse_human_readable(timestamp_str)
    
    def _parse_iso8601(self, timestamp_str: str) -> TimestampResult:
        """Parse ISO 8601 timestamp"""
        try:
            # Handle different ISO 8601 variants
            if timestamp_str.endswith('Z'):
                dt = datetime.fromisoformat(timestamp_str[:-1] + '+00:00')
            elif '+' in timestamp_str or timestamp_str.count('-') > 2:
                dt = datetime.fromisoformat(timestamp_str)
            else:
                # No timezone, assume UTC
                dt = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
            
            normalized = dt.isoformat()
            
            return TimestampResult(
                original=timestamp_str,
                normalized=normalized,
                format_detected=TimestampFormat.ISO_8601,
                timezone=str(dt.tzinfo),
                confidence=1.0
            )
        except Exception as e:
            logger.warning(f"ISO 8601 parsing failed: {e}")
            return TimestampResult(
                original=timestamp_str,
                normalized='',
                format_detected=TimestampFormat.UNKNOWN,
                timezone=self.default_timezone,
                confidence=0.0,
                error=str(e)
            )
    
    def _parse_date_only(self, timestamp_str: str) -> TimestampResult:
        """Parse date-only timestamp (assume midnight UTC)"""
        try:
            # Handle different date formats
            if re.match(r'^\d{4}[-/]\d{2}[-/]\d{2}$', timestamp_str):
                # YYYY-MM-DD or YYYY/MM/DD
                dt = datetime.strptime(timestamp_str.replace('/', '-'), '%Y-%m-%d')
            elif re.match(r'^\d{2}[-/]\d{2}[-/]\d{4}$', timestamp_str):
                # DD-MM-YYYY or DD/MM/YYYY
                dt = datetime.strptime(timestamp_str.replace('/', '-'), '%d-%m-%Y')
            else:
                raise ValueError(f"Unrecognized date format: {timestamp_str}")
            
            # Assume midnight UTC
            dt = dt.replace(tzinfo=timezone.utc)
            normalized = dt.isoformat()
            
            return TimestampResult(
                original=timestamp_str,
                normalized=normalized,
                format_detected=TimestampFormat.DATE_ONLY,
                timezone='UTC',
                confidence=0.8  # Lower confidence due to missing time
            )
        except Exception as e:
            logger.warning(f"Date parsing failed: {e}")
            return TimestampResult(
                original=timestamp_str,
                normalized='',
                format_detected=TimestampFormat.UNKNOWN,
                timezone=self.default_timezone,
                confidence=0.0,
                error=str(e)
            )
    
    def _parse_relative(self, timestamp_str: str, reference_time: datetime) -> TimestampResult:
        """Parse relative timestamp (e.g., '2 days ago')"""
        try:
            timestamp_lower = timestamp_str.lower().strip()
            
            if timestamp_lower == 'today':
                dt = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
            elif timestamp_lower == 'yesterday':
                dt = reference_time - timedelta(days=1)
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                # Parse "N units ago"
                match = re.match(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', timestamp_lower)
                if not match:
                    raise ValueError(f"Invalid relative format: {timestamp_str}")
                
                amount = int(match.group(1))
                unit = match.group(2)
                
                if unit == 'second':
                    dt = reference_time - timedelta(seconds=amount)
                elif unit == 'minute':
                    dt = reference_time - timedelta(minutes=amount)
                elif unit == 'hour':
                    dt = reference_time - timedelta(hours=amount)
                elif unit == 'day':
                    dt = reference_time - timedelta(days=amount)
                elif unit == 'week':
                    dt = reference_time - timedelta(weeks=amount)
                elif unit == 'month':
                    dt = reference_time - timedelta(days=amount * 30)  # Approximate
                elif unit == 'year':
                    dt = reference_time - timedelta(days=amount * 365)  # Approximate
                else:
                    raise ValueError(f"Unknown unit: {unit}")
            
            normalized = dt.isoformat()
            
            return TimestampResult(
                original=timestamp_str,
                normalized=normalized,
                format_detected=TimestampFormat.RELATIVE,
                timezone=str(dt.tzinfo),
                confidence=0.7  # Lower confidence for relative timestamps
            )
        except Exception as e:
            logger.warning(f"Relative timestamp parsing failed: {e}")
            return TimestampResult(
                original=timestamp_str,
                normalized='',
                format_detected=TimestampFormat.UNKNOWN,
                timezone=self.default_timezone,
                confidence=0.0,
                error=str(e)
            )
    
    def _parse_human_readable(self, timestamp_str: str) -> TimestampResult:
        """Parse human-readable timestamp (e.g., 'Jan 1, 2021 12:00 AM')"""
        try:
            # Try common formats
            formats = [
                '%b %d, %Y %I:%M %p',  # Jan 1, 2021 12:00 AM
                '%B %d, %Y %I:%M %p',  # January 1, 2021 12:00 AM
                '%m/%d/%Y %I:%M %p',  # 01/01/2021 12:00 AM
                '%d/%m/%Y %H:%M:%S',  # 01/01/2021 00:00:00
                '%Y-%m-%d %H:%M:%S',  # 2021-01-01 00:00:00
            ]
            
            dt = None
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                raise ValueError(f"No matching format found for: {timestamp_str}")
            
            # Assume UTC if no timezone
            dt = dt.replace(tzinfo=timezone.utc)
            normalized = dt.isoformat()
            
            return TimestampResult(
                original=timestamp_str,
                normalized=normalized,
                format_detected=TimestampFormat.HUMAN_READABLE,
                timezone='UTC',
                confidence=0.9
            )
        except Exception as e:
            logger.warning(f"Human-readable timestamp parsing failed: {e}")
            return TimestampResult(
                original=timestamp_str,
                normalized='',
                format_detected=TimestampFormat.UNKNOWN,
                timezone=self.default_timezone,
                confidence=0.0,
                error=str(e)
            )
    
    def convert_timezone(self, timestamp_str: str, target_timezone: str) -> Optional[str]:
        """
        Convert timestamp from one timezone to another
        
        Args:
            timestamp_str: ISO 8601 timestamp
            target_timezone: Target timezone (e.g., '+05:30', 'IST')
            
        Returns:
            Converted timestamp in ISO 8601 format
        """
        try:
            # Parse timestamp
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            # Parse target timezone
            if target_timezone in self.TIMEZONE_MAP:
                target_offset_str = self.TIMEZONE_MAP[target_timezone]
            else:
                target_offset_str = target_timezone
            
            # Parse offset
            match = re.match(r'([+-])(\d{2}):(\d{2})', target_offset_str)
            if not match:
                raise ValueError(f"Invalid timezone format: {target_timezone}")
            
            sign = 1 if match.group(1) == '+' else -1
            hours = int(match.group(2))
            minutes = int(match.group(3))
            
            target_tz = timezone(timedelta(hours=sign * hours, minutes=sign * minutes))
            
            # Convert
            dt_converted = dt.astimezone(target_tz)
            
            return dt_converted.isoformat()
        
        except Exception as e:
            logger.error(f"Timezone conversion failed: {e}")
            return None


# Singleton instance
_harmonizer = None

def get_timestamp_harmonizer(default_timezone: str = 'UTC') -> TimestampHarmonizer:
    """Get singleton timestamp harmonizer instance"""
    global _harmonizer
    if _harmonizer is None:
        _harmonizer = TimestampHarmonizer(default_timezone=default_timezone)
    return _harmonizer


# ============================================================
# TESTING CODE
# ============================================================

def test_timestamp_harmonization():
    """Test timestamp harmonization"""
    print("\n" + "="*70)
    print("STEP 9/10: TIMESTAMP HARMONIZATION TEST")
    print("="*70)
    
    harmonizer = get_timestamp_harmonizer()
    
    test_cases = [
        # Unix timestamps
        (1609459200, "2021-01-01T00:00:00+00:00"),
        (1609459200000, "2021-01-01T00:00:00+00:00"),
        
        # ISO 8601
        ("2021-01-01T00:00:00Z", "2021-01-01T00:00:00+00:00"),
        ("2021-01-01T05:30:00+05:30", "2021-01-01T05:30:00+05:30"),
        
        # Date only
        ("2021-01-01", "2021-01-01T00:00:00+00:00"),
        ("01-01-2021", "2021-01-01T00:00:00+00:00"),
        
        # Human readable
        ("Jan 1, 2021 12:00 AM", "2021-01-01T00:00:00+00:00"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for input_ts, expected in test_cases:
        result = harmonizer.normalize(input_ts)
        
        is_correct = result.normalized == expected
        passed += 1 if is_correct else 0
        status = "\u2705" if is_correct else "\u274c"
        
        print(f"{status} {str(input_ts):35} \u2192 {result.normalized:35} "
              f"(confidence: {result.confidence:.2f})")
        if not is_correct:
            print(f"   Expected: {expected}")
    
    print(f"\nResult: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
    return passed == total


if __name__ == "__main__":
    test_timestamp_harmonization()
