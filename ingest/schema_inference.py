"""
Step 5: Dynamic Schema Inference
Automatically infers normalized schema from sample records
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import Counter, defaultdict
import logging

from record_segmenter import RecordType, SegmentedRecord

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FieldType(Enum):
    """Inferred field types"""
    UUID = "uuid"
    PHONE = "phone"
    EMAIL = "email"
    DATETIME = "datetime"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TEXT = "text"
    JSON = "json"
    UNKNOWN = "unknown"


@dataclass
class FieldSchema:
    """Schema for a single field"""
    name: str
    field_type: FieldType
    required: bool = False
    nullable: bool = True
    default_value: Optional[Any] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    examples: List[Any] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "type": self.field_type.value,
            "required": self.required,
            "nullable": self.nullable,
            "default": self.default_value,
            "constraints": self.constraints,
            "examples": self.examples[:5]  # Keep max 5 examples
        }


@dataclass
class EntitySchema:
    """Schema for an entity type (contact, message, call_log)"""
    entity_type: RecordType
    version: str
    fields: Dict[str, FieldSchema]
    indexes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "entity_type": self.entity_type.value,
            "version": self.version,
            "fields": {name: field.to_dict() for name, field in self.fields.items()},
            "indexes": self.indexes,
            "metadata": self.metadata,
            "created_at": self.created_at
        }
    
    def save(self, output_path: Path):
        """Save schema to JSON file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info(f"✅ Schema saved to {output_path}")


@dataclass
class MappingRule:
    """Mapping rule from original field to normalized field"""
    original_key: str
    normalized_key: str
    transformation: Optional[str] = None  # e.g., "normalize_phone", "parse_datetime"
    validation: Optional[str] = None  # e.g., "is_valid_phone", "is_valid_email"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "original_key": self.original_key,
            "normalized_key": self.normalized_key,
            "transformation": self.transformation,
            "validation": self.validation
        }


class FieldTypeDetector:
    """
    Detects field types from sample values
    """
    
    # Regex patterns for type detection
    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    PHONE_PATTERN = re.compile(r'^\+?\d{7,15}$')
    EMAIL_PATTERN = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
    DATETIME_PATTERNS = [
        re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'),  # ISO 8601
        re.compile(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}'),  # YYYY-MM-DD HH:MM:SS
        re.compile(r'^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}'),  # DD/MM/YYYY HH:MM:SS
    ]
    
    @classmethod
    def detect_type(cls, value: Any) -> FieldType:
        """
        Detect field type from a single value
        """
        if value is None:
            return FieldType.UNKNOWN
        
        # Check Python type first
        if isinstance(value, bool):
            return FieldType.BOOLEAN
        
        if isinstance(value, int):
            return FieldType.INTEGER
        
        if isinstance(value, float):
            return FieldType.FLOAT
        
        if isinstance(value, (dict, list)):
            return FieldType.JSON
        
        # String value - check patterns
        if not isinstance(value, str):
            value = str(value)
        
        value = value.strip()
        
        # UUID
        if cls.UUID_PATTERN.match(value):
            return FieldType.UUID
        
        # Phone
        if cls.PHONE_PATTERN.match(value):
            return FieldType.PHONE
        
        # Email
        if cls.EMAIL_PATTERN.match(value):
            return FieldType.EMAIL
        
        # Datetime
        for pattern in cls.DATETIME_PATTERNS:
            if pattern.match(value):
                return FieldType.DATETIME
        
        # Default to text
        return FieldType.TEXT
    
    @classmethod
    def infer_type_from_samples(cls, samples: List[Any]) -> Tuple[FieldType, float]:
        """
        Infer type from multiple samples
        
        Returns:
            (FieldType, confidence)
        """
        if not samples:
            return FieldType.UNKNOWN, 0.0
        
        # Filter out None values
        non_null_samples = [s for s in samples if s is not None]
        
        if not non_null_samples:
            return FieldType.UNKNOWN, 0.0
        
        # Count type occurrences
        type_counts = Counter(cls.detect_type(sample) for sample in non_null_samples)
        
        # Most common type
        most_common_type, count = type_counts.most_common(1)[0]
        confidence = count / len(non_null_samples)
        
        return most_common_type, confidence


class SchemaInferenceEngine:
    """
    Infers schema from sample records
    """
    
    def __init__(self, sample_size: int = 1000, min_confidence: float = 0.7):
        """
        Initialize schema inference engine
        
        Args:
            sample_size: Number of records to sample per entity type
            min_confidence: Minimum confidence for type inference
        """
        self.sample_size = sample_size
        self.min_confidence = min_confidence
        self.detector = FieldTypeDetector()
    
    def analyze_records(self, records: List[SegmentedRecord]) -> Dict[RecordType, List[Dict]]:
        """
        Group records by type and extract field samples
        
        Returns:
            Dictionary mapping RecordType to list of field dictionaries
        """
        grouped = defaultdict(list)
        
        for record in records:
            if record.fields:
                grouped[record.type_label].append(record.fields)
        
        # Sample if too many records
        for record_type, field_list in grouped.items():
            if len(field_list) > self.sample_size:
                import random
                grouped[record_type] = random.sample(field_list, self.sample_size)
        
        return dict(grouped)
    
    def infer_field_schema(self, field_name: str, samples: List[Any]) -> FieldSchema:
        """
        Infer schema for a single field
        
        Args:
            field_name: Name of the field
            samples: List of sample values
            
        Returns:
            FieldSchema
        """
        # Detect type
        field_type, confidence = self.detector.infer_type_from_samples(samples)
        
        # Check if required (present in most records)
        non_null_count = sum(1 for s in samples if s is not None)
        presence_ratio = non_null_count / len(samples) if samples else 0
        required = presence_ratio >= 0.9  # Required if present in 90%+ of records
        nullable = presence_ratio < 1.0
        
        # Gather examples
        examples = [s for s in samples if s is not None][:10]
        
        # Build constraints
        constraints = {}
        if field_type == FieldType.TEXT:
            lengths = [len(str(s)) for s in examples if s is not None]
            if lengths:
                constraints["min_length"] = min(lengths)
                constraints["max_length"] = max(lengths)
        
        elif field_type in [FieldType.INTEGER, FieldType.FLOAT]:
            numeric_values = [float(s) for s in examples if s is not None]
            if numeric_values:
                constraints["min_value"] = min(numeric_values)
                constraints["max_value"] = max(numeric_values)
        
        return FieldSchema(
            name=field_name,
            field_type=field_type,
            required=required,
            nullable=nullable,
            constraints=constraints,
            examples=examples[:5]
        )
    
    def infer_entity_schema(self, 
                           entity_type: RecordType,
                           field_samples: List[Dict],
                           version: str = "1.0.0") -> EntitySchema:
        """
        Infer schema for an entity type
        
        Args:
            entity_type: Type of entity (CONTACT, MESSAGE, CALL_LOG)
            field_samples: List of field dictionaries from records
            version: Schema version
            
        Returns:
            EntitySchema
        """
        # Collect all field names
        all_field_names = set()
        for sample in field_samples:
            all_field_names.update(sample.keys())
        
        # Infer schema for each field
        fields = {}
        for field_name in sorted(all_field_names):
            # Gather samples for this field
            samples = [sample.get(field_name) for sample in field_samples]
            fields[field_name] = self.infer_field_schema(field_name, samples)
        
        # Add standard fields
        if 'record_uuid' not in fields:
            fields['record_uuid'] = FieldSchema(
                name='record_uuid',
                field_type=FieldType.UUID,
                required=True,
                nullable=False
            )
        
        if 'case_id' not in fields:
            fields['case_id'] = FieldSchema(
                name='case_id',
                field_type=FieldType.TEXT,
                required=True,
                nullable=False
            )
        
        # Add detection metadata fields
        for meta_field, meta_type in [
            ('detected_script', FieldType.TEXT),
            ('detected_language', FieldType.TEXT),
            ('detection_confidence', FieldType.FLOAT),
            ('detection_reasons', FieldType.JSON),
        ]:
            if meta_field not in fields:
                fields[meta_field] = FieldSchema(
                    name=meta_field,
                    field_type=meta_type,
                    required=False,
                    nullable=True
                )
        
        # Add provenance fields
        for prov_field, prov_type in [
            ('src_file', FieldType.TEXT),
            ('src_offset', FieldType.INTEGER),
            ('schema_version', FieldType.TEXT),
            ('raw_blob', FieldType.JSON),
            ('created_at', FieldType.DATETIME),
            ('updated_at', FieldType.DATETIME),
        ]:
            if prov_field not in fields:
                fields[prov_field] = FieldSchema(
                    name=prov_field,
                    field_type=prov_type,
                    required=False,
                    nullable=True
                )
        
        # Determine indexes
        indexes = ['record_uuid', 'case_id']
        
        # Add type-specific indexes
        if entity_type == RecordType.CONTACT:
            if 'name' in fields:
                indexes.append('name')
            if 'phone' in fields:
                indexes.append('phone')
        elif entity_type == RecordType.MESSAGE:
            if 'timestamp' in fields:
                indexes.append('timestamp')
            if 'sender' in fields:
                indexes.append('sender')
        elif entity_type == RecordType.CALL_LOG:
            if 'timestamp' in fields:
                indexes.append('timestamp')
            if 'caller' in fields:
                indexes.append('caller')
        
        # Always index detection_confidence
        indexes.append('detection_confidence')
        
        return EntitySchema(
            entity_type=entity_type,
            version=version,
            fields=fields,
            indexes=indexes,
            metadata={
                "sample_size": len(field_samples),
                "inferred_at": datetime.utcnow().isoformat() + 'Z'
            }
        )
    
    def infer_schemas(self, records: List[SegmentedRecord], version: str = "1.0.0") -> Dict[RecordType, EntitySchema]:
        """
        Infer schemas for all entity types in records
        
        Args:
            records: List of segmented records
            version: Schema version
            
        Returns:
            Dictionary mapping RecordType to EntitySchema
        """
        logger.info(f"Inferring schemas from {len(records)} records...")
        
        # Analyze and group records
        grouped = self.analyze_records(records)
        
        # Infer schema for each type
        schemas = {}
        for record_type, field_samples in grouped.items():
            if record_type != RecordType.UNKNOWN:
                logger.info(f"Inferring schema for {record_type.value} ({len(field_samples)} samples)")
                schemas[record_type] = self.infer_entity_schema(record_type, field_samples, version)
        
        return schemas


class MappingRuleGenerator:
    """
    Generates mapping rules from original keys to normalized keys
    """
    
    # Common key aliases
    KEY_ALIASES = {
        'name': ['name', 'contact_name', 'display_name', 'full_name', 'displayName'],
        'phone': ['phone', 'phone_number', 'phoneNumber', 'mobile', 'cell'],
        'email': ['email', 'email_address', 'emailAddress', 'e_mail'],
        'sender': ['sender', 'from', 'from_user', 'fromUser'],
        'receiver': ['receiver', 'to', 'to_user', 'toUser', 'recipient'],
        'timestamp': ['timestamp', 'date', 'datetime', 'time', 'created_at', 'createdAt'],
        'message_text': ['message', 'text', 'body', 'content', 'message_text'],
        'caller': ['caller', 'from', 'calling_number'],
        'callee': ['callee', 'to', 'called_number', 'receiver'],
        'duration': ['duration', 'duration_seconds', 'call_duration'],
    }
    
    @classmethod
    def generate_rules(cls, schema: EntitySchema, original_keys: Set[str]) -> List[MappingRule]:
        """
        Generate mapping rules for a schema
        
        Args:
            schema: Entity schema
            original_keys: Set of original field names from data
            
        Returns:
            List of MappingRule objects
        """
        rules = []
        
        for normalized_key, field_schema in schema.fields.items():
            # Find matching original keys
            if normalized_key in original_keys:
                # Exact match
                transformation = cls._get_transformation(field_schema.field_type)
                validation = cls._get_validation(field_schema.field_type)
                
                rules.append(MappingRule(
                    original_key=normalized_key,
                    normalized_key=normalized_key,
                    transformation=transformation,
                    validation=validation
                ))
            else:
                # Check aliases
                aliases = cls.KEY_ALIASES.get(normalized_key, [])
                for original_key in original_keys:
                    if original_key.lower() in [a.lower() for a in aliases]:
                        transformation = cls._get_transformation(field_schema.field_type)
                        validation = cls._get_validation(field_schema.field_type)
                        
                        rules.append(MappingRule(
                            original_key=original_key,
                            normalized_key=normalized_key,
                            transformation=transformation,
                            validation=validation
                        ))
                        break
        
        return rules
    
    @classmethod
    def _get_transformation(cls, field_type: FieldType) -> Optional[str]:
        """Get transformation function name for field type"""
        transformations = {
            FieldType.PHONE: "normalize_phone",
            FieldType.EMAIL: "normalize_email",
            FieldType.DATETIME: "parse_datetime",
            FieldType.TEXT: "normalize_text",
        }
        return transformations.get(field_type)
    
    @classmethod
    def _get_validation(cls, field_type: FieldType) -> Optional[str]:
        """Get validation function name for field type"""
        validations = {
            FieldType.PHONE: "is_valid_phone",
            FieldType.EMAIL: "is_valid_email",
            FieldType.DATETIME: "is_valid_datetime",
        }
        return validations.get(field_type)
    
    @classmethod
    def save_rules(cls, rules: List[MappingRule], output_path: Path):
        """Save mapping rules to JSON file"""
        rules_dict = [rule.to_dict() for rule in rules]
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(rules_dict, f, indent=2, ensure_ascii=False)
        logger.info(f"✅ Mapping rules saved to {output_path}")


if __name__ == "__main__":
    # Test schema inference
    from record_segmenter import RecordSegmentationEngine
    
    # Create some sample records
    engine = RecordSegmentationEngine()
    
    records = []
    
    # Sample contacts
    for i in range(5):
        record = engine.segment_record("", {
            "name": f"Person {i}",
            "phone": f"+9198765432{i:02d}",
            "email": f"person{i}@example.com"
        })
        records.append(record)
    
    # Sample messages
    for i in range(5):
        record = engine.segment_record(
            f"From: +9198765432{i:02d}\nTo: +9187654321{i:02d}\nText: Hello {i}\nTimestamp: 2025-01-02T16:30:{i:02d}"
        )
        records.append(record)
    
    # Sample calls
    for i in range(5):
        record = engine.segment_record(
            f"Caller: +9198765432{i:02d}\nCallee: +9187654321{i:02d}\nDuration: {60+i*10} seconds\nTimestamp: 2025-01-02 16:30:{i:02d}"
        )
        records.append(record)
    
    # Infer schemas
    inference_engine = SchemaInferenceEngine(sample_size=10)
    schemas = inference_engine.infer_schemas(records, version="1.0.0")
    
    # Display results
    print("\n" + "="*60)
    print("Schema Inference Results")
    print("="*60)
    
    for record_type, schema in schemas.items():
        print(f"\n{record_type.value.upper()} Schema (v{schema.version}):")
        print(f"  Sample size: {schema.metadata['sample_size']}")
        print(f"  Fields: {len(schema.fields)}")
        print(f"  Indexes: {', '.join(schema.indexes)}")
        
        print(f"\n  Key Fields:")
        for field_name, field in list(schema.fields.items())[:5]:
            req_str = "required" if field.required else "optional"
            print(f"    - {field_name}: {field.field_type.value} ({req_str})")
            if field.examples:
                print(f"      Example: {field.examples[0]}")
    
    # Save schemas
    output_dir = Path("data/schemas")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for record_type, schema in schemas.items():
        output_file = output_dir / f"{record_type.value}_v{schema.version}.json"
        schema.save(output_file)
    
    print(f"\n✅ Schemas saved to {output_dir}")
