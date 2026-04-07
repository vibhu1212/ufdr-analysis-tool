"""
Step 6: Normalization & Storage
Stores records in database with normalized columns + raw_blob for provenance
"""

import sqlite3
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import re

from record_segmenter import RecordType, SegmentedRecord
from schema_inference import EntitySchema, FieldType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WriteStatistics:
    """Statistics for database write operations"""
    total_records: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "total_records": self.total_records,
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
            "success_rate": f"{(self.inserted + self.updated) / max(self.total_records, 1):.2%}"
        }


class RecordNormalizer:
    """
    Normalizes record fields for storage
    """
    
    @staticmethod
    def normalize_phone(phone: str) -> str:
        """Normalize phone to E.164 format"""
        if not phone:
            return phone
        
        # Remove all non-digits except leading +
        phone = re.sub(r'[\s\-\(\)]', '', phone)
        
        # Ensure + prefix
        if not phone.startswith('+'):
            if phone.startswith('91') and len(phone) == 12:
                phone = '+' + phone
            elif len(phone) == 10:
                phone = '+91' + phone  # Assume Indian
        
        return phone
    
    @staticmethod
    def normalize_email(email: str) -> str:
        """Normalize email address"""
        if not email:
            return email
        return email.lower().strip()
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text for search"""
        if not text:
            return text
        # Lowercase, strip whitespace
        return text.strip().lower()
    
    @staticmethod
    def generate_name_tokens(name: str) -> str:
        """Generate search tokens from name for fuzzy matching"""
        if not name:
            return ""
        
        # Remove punctuation and split
        tokens = re.split(r'[\s,\.\-]+', name.lower())
        tokens = [t.strip() for t in tokens if t.strip()]
        
        # Return space-separated tokens for FTS
        return ' '.join(tokens)
    
    @staticmethod
    def parse_datetime(dt: Any) -> Optional[str]:
        """Parse datetime to ISO 8601 format"""
        if not dt:
            return None
        
        if isinstance(dt, datetime):
            return dt.isoformat()
        
        # Already a string, return as-is
        return str(dt)


class DatabaseSchema:
    """
    Manages database schema creation and updates
    """
    
    @staticmethod
    def get_sqlite_type(field_type: FieldType) -> str:
        """Map FieldType to SQLite type"""
        type_map = {
            FieldType.UUID: "TEXT",
            FieldType.TEXT: "TEXT",
            FieldType.PHONE: "TEXT",
            FieldType.EMAIL: "TEXT",
            FieldType.DATETIME: "TEXT",
            FieldType.INTEGER: "INTEGER",
            FieldType.FLOAT: "REAL",
            FieldType.BOOLEAN: "INTEGER",
            FieldType.JSON: "TEXT",
            FieldType.UNKNOWN: "TEXT",
        }
        return type_map.get(field_type, "TEXT")
    
    @staticmethod
    def create_table_sql(entity_type: RecordType, schema: EntitySchema) -> str:
        """Generate CREATE TABLE SQL from schema"""
        table_name = f"{entity_type.value}s"
        
        columns = []
        columns.append("id INTEGER PRIMARY KEY AUTOINCREMENT")
        
        for field_name, field_schema in schema.fields.items():
            sql_type = DatabaseSchema.get_sqlite_type(field_schema.field_type)
            constraints = []
            
            if field_schema.required and not field_schema.nullable:
                constraints.append("NOT NULL")
            
            if field_name == 'record_uuid':
                constraints.append("UNIQUE")
            
            column_def = f"{field_name} {sql_type}"
            if constraints:
                column_def += " " + " ".join(constraints)
            
            columns.append(column_def)
        
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n  " + ",\n  ".join(columns) + "\n)"
    
    @staticmethod
    def create_indexes_sql(entity_type: RecordType, schema: EntitySchema) -> List[str]:
        """Generate CREATE INDEX SQL statements"""
        table_name = f"{entity_type.value}s"
        statements = []
        
        for index_field in schema.indexes:
            if index_field in schema.fields:
                index_name = f"idx_{table_name}_{index_field}"
                statements.append(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({index_field})"
                )
        
        return statements


class BatchWriter:
    """
    Writes records to database in batches with checkpointing
    """
    
    def __init__(self, 
                 db_path: str,
                 batch_size: int = 1000,
                 checkpoint_interval: int = 5000):
        """
        Initialize batch writer
        
        Args:
            db_path: Path to SQLite database
            batch_size: Number of records per batch
            checkpoint_interval: Save checkpoint every N records
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.checkpoint_interval = checkpoint_interval
        self.normalizer = RecordNormalizer()
        self.stats = WriteStatistics()
        
        # Ensure parent directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized BatchWriter: {db_path}")
    
    def initialize_database(self, schemas: Dict[RecordType, EntitySchema]):
        """
        Initialize database with schemas
        
        Args:
            schemas: Dictionary of entity schemas
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for entity_type, schema in schemas.items():
                if entity_type != RecordType.UNKNOWN:
                    # Create table
                    create_table_sql = DatabaseSchema.create_table_sql(entity_type, schema)
                    logger.info(f"Creating table for {entity_type.value}...")
                    cursor.execute(create_table_sql)
                    
                    # Create indexes
                    for index_sql in DatabaseSchema.create_indexes_sql(entity_type, schema):
                        cursor.execute(index_sql)
            
            conn.commit()
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _normalize_record(self, record: SegmentedRecord, case_id: str) -> Dict[str, Any]:
        """
        Normalize a record for database storage
        
        Args:
            record: Segmented record
            case_id: Case identifier
            
        Returns:
            Dictionary of normalized fields
        """
        normalized = {
            'record_uuid': str(uuid.uuid4()),
            'case_id': case_id,
        }
        
        # Copy and normalize fields
        for key, value in record.fields.items():
            if key == 'phone':
                normalized[key] = self.normalizer.normalize_phone(value)
            elif key in ['sender', 'receiver', 'caller', 'callee'] and value:
                normalized[key] = self.normalizer.normalize_phone(str(value))
            elif key == 'email':
                normalized[key] = self.normalizer.normalize_email(value)
            elif key == 'name':
                normalized[key] = value
                normalized['name_tokens'] = self.normalizer.generate_name_tokens(value)
            elif key in ['timestamp', 'created_at', 'updated_at']:
                normalized[key] = self.normalizer.parse_datetime(value)
            else:
                normalized[key] = value
        
        # Add detection metadata
        normalized['detection_confidence'] = record.confidence
        normalized['detection_reasons'] = json.dumps(record.extraction_reasons)
        
        # Add provenance
        if record.provenance:
            normalized['src_file'] = record.provenance.get('src_file')
            normalized['src_offset'] = record.provenance.get('src_offset')
        
        normalized['schema_version'] = "1.0.0"
        
        # Store raw blob for audit
        normalized['raw_blob'] = json.dumps({
            'fields': record.fields,
            'raw_text': record.raw_text,
            'type_label': record.type_label.value,
            'metadata': record.metadata
        }, ensure_ascii=False)
        
        # Timestamps
        now = datetime.utcnow().isoformat() + 'Z'
        normalized['created_at'] = now
        normalized['updated_at'] = now
        
        return normalized
    
    def write_records(self,
                     records: List[SegmentedRecord],
                     case_id: str,
                     progress_callback: Optional[Callable[[int, int], None]] = None) -> WriteStatistics:
        """
        Write records to database in batches
        
        Args:
            records: List of segmented records
            case_id: Case identifier
            progress_callback: Optional callback(current, total) for progress updates
            
        Returns:
            WriteStatistics
        """
        self.stats = WriteStatistics(total_records=len(records))
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Group records by type
            grouped = {}
            for record in records:
                if record.type_label not in grouped:
                    grouped[record.type_label] = []
                grouped[record.type_label].append(record)
            
            # Process each type
            for record_type, type_records in grouped.items():
                if record_type == RecordType.UNKNOWN:
                    self.stats.skipped += len(type_records)
                    continue
                
                table_name = f"{record_type.value}s"
                
                # Process in batches
                for i in range(0, len(type_records), self.batch_size):
                    batch = type_records[i:i + self.batch_size]

                    # ⚡ Bolt: Fix N+1 Query bottleneck by batch fetching existing records
                    # First pass: normalize all records and collect search keys
                    normalized_batch = []
                    keys_to_fetch = set()
                    for record in batch:
                        try:
                            normalized = self._normalize_record(record, case_id)
                            normalized_batch.append((record, normalized))
                            src_file = normalized.get("src_file")
                            src_offset = normalized.get("src_offset")
                            if src_file is not None and src_offset is not None:
                                keys_to_fetch.add((src_file, src_offset))
                        except Exception as e:
                            logger.error(f"Error normalizing record: {e}")
                            self.stats.errors += 1
                            normalized_batch.append((record, None))

                    existing_records = set()
                    if keys_to_fetch:
                        # SQLite max variable limit is 999. We use 2 variables per pair.
                        # Chunking at 400 pairs (800 vars + 1 for case_id) is safe.
                        keys_list = list(keys_to_fetch)
                        for chunk_idx in range(0, len(keys_list), 400):
                            chunk = keys_list[chunk_idx:chunk_idx + 400]

                            or_conditions = []
                            query_params = [case_id]
                            for (src_file, src_offset) in chunk:
                                or_conditions.append("(src_file = ? AND src_offset = ?)")
                                query_params.extend([src_file, src_offset])

                            where_clause = " OR ".join(or_conditions)
                            query = f"SELECT src_file, src_offset FROM {table_name} WHERE case_id = ? AND ({where_clause})"

                            cursor.execute(query, query_params)
                            for row in cursor.fetchall():
                                existing_records.add((row[0], row[1]))

                    for record, normalized in normalized_batch:
                        if normalized is None:
                            continue
                        try:
                            # Check if exists in memory
                            record_key = (normalized.get("src_file"), normalized.get("src_offset"))
                            if record_key in existing_records:
                                self.stats.skipped += 1
                                continue

                            # Add to existing_records to prevent intra-batch duplicates
                            existing_records.add(record_key)

                            # Insert
                            columns = list(normalized.keys())
                            placeholders = ",".join(["?" for _ in columns])
                            values = [normalized[col] for col in columns]

                            cursor.execute(
                                f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                                values
                            )
                            self.stats.inserted += 1

                        except Exception as e:
                            logger.error(f"Error writing record: {e}")
                            self.stats.errors += 1

                    # Commit batch
                    conn.commit()
                    
                    # Checkpoint
                    if (i + self.batch_size) % self.checkpoint_interval == 0:
                        logger.info(f"Checkpoint: {self.stats.inserted} inserted, {self.stats.skipped} skipped, {self.stats.errors} errors")
                    
                    # Progress callback
                    if progress_callback:
                        current = self.stats.inserted + self.stats.skipped + self.stats.errors
                        progress_callback(current, self.stats.total_records)
            
            conn.commit()
            logger.info(f"✅ Write complete: {self.stats.to_dict()}")
            
        except Exception as e:
            logger.error(f"❌ Batch write failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
        
        return self.stats


class ProvenanceTracker:
    """
    Tracks and validates data provenance
    """
    
    @staticmethod
    def validate_provenance(record: Dict[str, Any]) -> bool:
        """Validate that record has complete provenance"""
        required_fields = ['record_uuid', 'case_id', 'raw_blob', 'created_at']
        return all(field in record and record[field] is not None for field in required_fields)
    
    @staticmethod
    def get_provenance_summary(db_path: str, case_id: str) -> Dict[str, Any]:
        """Get provenance summary for a case"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        summary = {}
        
        for table in ['contacts', 'messages', 'call_logs']:
            try:
                cursor.execute(
                    f"SELECT COUNT(*), AVG(detection_confidence) FROM {table} WHERE case_id = ?",
                    (case_id,)
                )
                count, avg_conf = cursor.fetchone()
                summary[table] = {
                    "count": count or 0,
                    "avg_confidence": avg_conf or 0.0
                }
            except sqlite3.OperationalError:
                # Table doesn't exist yet
                summary[table] = {"count": 0, "avg_confidence": 0.0}
        
        conn.close()
        return summary


if __name__ == "__main__":
    # Test database writer
    from record_segmenter import RecordSegmentationEngine
    from schema_inference import SchemaInferenceEngine
    
    # Create sample records
    engine = RecordSegmentationEngine()
    records = []
    
    # Sample contacts
    for i in range(10):
        record = engine.segment_record("", {
            "name": f"Test Contact {i}",
            "phone": f"+9198765432{i:02d}",
            "email": f"test{i}@example.com"
        }, provenance={"src_file": "test.csv", "src_offset": i * 100})
        records.append(record)
    
    # Sample messages
    for i in range(10):
        record = engine.segment_record(
            f"From: +9198765432{i:02d}\nTo: +9187654321{i:02d}\nText: Test message {i}\nTimestamp: 2025-01-02T16:30:{i:02d}",
            provenance={"src_file": "messages.json", "src_offset": i * 200}
        )
        records.append(record)
    
    # Infer schemas
    inference_engine = SchemaInferenceEngine()
    schemas = inference_engine.infer_schemas(records, version="1.0.0")
    
    # Debug: Print schemas
    for record_type, schema in schemas.items():
        print(f"\n{'='*60}")
        print(f"Schema for {record_type.value}:")
        print(f"{'='*60}")
        print(f"Fields: {list(schema.fields.keys())}")
        print(f"\nCREATE TABLE SQL:")
        sql = DatabaseSchema.create_table_sql(record_type, schema)
        print(sql)
    
    # Write to database
    db_path = "data/test_mvp.db"
    writer = BatchWriter(db_path, batch_size=5)
    
    # Initialize database
    writer.initialize_database(schemas)
    
    # Write records
    def progress(current, total):
        print(f"Progress: {current}/{total} ({current/total:.1%})")
    
    stats = writer.write_records(records, case_id="test_case_001", progress_callback=progress)
    
    print("\n" + "="*60)
    print("Database Write Results")
    print("="*60)
    print(json.dumps(stats.to_dict(), indent=2))
    
    # Get provenance summary
    summary = ProvenanceTracker.get_provenance_summary(db_path, "test_case_001")
    print("\n" + "="*60)
    print("Provenance Summary")
    print("="*60)
    print(json.dumps(summary, indent=2))
    
    print(f"\n✅ Database created at: {db_path}")
