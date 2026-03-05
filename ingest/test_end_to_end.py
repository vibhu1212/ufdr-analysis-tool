"""
End-to-End Test for UFDR Ingestion Pipeline (Steps 0-6)
Tests the complete pipeline from file ingestion to database storage
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest.config import ProcessingConfig, ProcessingMode
from ingest.file_ingestor import FileIngestor
from ingest.file_type_detector import FileTypeDetector
from ingest.text_extractors import TextExtractionEngine
from ingest.record_segmenter import RecordSegmentationEngine
from ingest.schema_inference import SchemaInferenceEngine
from ingest.database_writer import BatchWriter, ProvenanceTracker


def create_test_data():
    """Create sample test data files"""
    print("Creating test data...")
    
    # Create test data directory
    test_dir = Path("data/test_e2e")
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Create sample contacts CSV
    contacts_csv = test_dir / "contacts.csv"
    with open(contacts_csv, 'w', encoding='utf-8') as f:
        f.write("Name,Phone,Email\n")
        f.write("राज कुमार,+919876543210,raj.kumar@example.com\n")
        f.write("అనిల్ రెడ్డి,+919123456789,anil@example.com\n")
        f.write("রাজেশ বোস,+919988776655,rajesh@example.com\n")
        f.write("Priya Sharma,+918765432109,priya@example.com\n")
    
    # Create sample messages JSON
    messages_json = test_dir / "messages.json"
    messages_data = [
        {
            "sender": "+919876543210",
            "receiver": "+918765432109",
            "text": "Hello, how are you?",
            "timestamp": "2025-01-02T10:30:00"
        },
        {
            "sender": "+919123456789",
            "receiver": "+919988776655",
            "text": "నేను బాగున్నాను",
            "timestamp": "2025-01-02T11:45:00"
        },
        {
            "sender": "+919988776655",
            "receiver": "+919876543210",
            "text": "আমি ভালো আছি",
            "timestamp": "2025-01-02T14:20:00"
        }
    ]
    with open(messages_json, 'w', encoding='utf-8') as f:
        json.dump(messages_data, f, indent=2, ensure_ascii=False)
    
    # Create sample call logs text
    calls_txt = test_dir / "call_logs.txt"
    with open(calls_txt, 'w', encoding='utf-8') as f:
        f.write("Caller: +919876543210\nCallee: +918765432109\nDuration: 180 seconds\nType: Outgoing\nTimestamp: 2025-01-02 09:15:00\n\n")
        f.write("Caller: +919123456789\nCallee: +919988776655\nDuration: 420 seconds\nType: Incoming\nTimestamp: 2025-01-02 12:30:00\n\n")
        f.write("Caller: +919988776655\nCallee: +919876543210\nDuration: 90 seconds\nType: Missed\nTimestamp: 2025-01-02 15:45:00\n\n")
    
    print(f"Test data created in {test_dir}")
    return test_dir


def test_step_0_config():
    """Test Step 0: Configuration"""
    print("\n" + "="*60)
    print("STEP 0: Configuration Framework")
    print("="*60)
    
    # Test MVP mode
    config_mvp = ProcessingConfig(mode=ProcessingMode.MVP)
    print(f"MVP Config: {config_mvp.mode.value}")
    print(f"   - Batch size: {config_mvp.batch_size}")
    print(f"   - Database: {config_mvp.database_type}")
    
    # Test STANDARD mode
    config_std = ProcessingConfig(mode=ProcessingMode.STANDARD)
    print(f"STANDARD Config: {config_std.mode.value}")
    
    # Test FORENSIC mode
    config_forensic = ProcessingConfig(mode=ProcessingMode.FORENSIC)
    print(f"FORENSIC Config: {config_forensic.mode.value}")
    print(f"   - Max workers: {config_forensic.max_workers}")
    
    return config_mvp


def test_step_1_ingestion(test_dir):
    """Test Step 1: File Ingestion"""
    print("\n" + "="*60)
    print("STEP 1: Format-Agnostic Ingestion")
    print("="*60)
    
    ingestor = FileIngestor()
    
    # Ingest test directory
    manifest = ingestor.ingest(test_dir)
    
    print(f"✅ Ingested {manifest.total_files} files")
    print(f"✅ Total size: {manifest.total_size_bytes:,} bytes")
    print(f"✅ Workspace: {manifest.workspace_path}")
    print(f"\nFiles:")
    for file_entry in manifest.files:
        print(f"   - {file_entry.filename}: {file_entry.mime_type}")
        print(f"     SHA256: {file_entry.sha256_hash[:16]}...")
    
    return manifest


def test_step_2_detection(manifest):
    """Test Step 2: File Type & Script Detection"""
    print("\n" + "="*60)
    print("STEP 2: File Type & Script Detection")
    print("="*60)
    
    detector = FileTypeDetector()
    detected_files = []
    
    for file_entry in manifest.files:
        file_path = Path(file_entry.extracted_path)
        if file_path.suffix in ['.csv', '.json', '.txt']:
            info = detector.detect(file_path, file_entry.mime_type)
            detected_files.append((file_entry, info))
            
            print(f"\n✅ {file_entry.filename}:")
            print(f"   Category: {info.category.value}")
            print(f"   Encoding: {info.encoding}")
            if info.scripts:
                print(f"   Scripts: {', '.join(s.value for s in info.scripts[:3])}")
            print(f"   Confidence: {info.confidence:.1%}")
    
    return detected_files


def test_step_3_extraction(detected_files):
    """Test Step 3: Text Extraction"""
    print("\n" + "="*60)
    print("STEP 3: Multi-Path Text Extraction")
    print("="*60)
    
    extractor = TextExtractionEngine()
    extracted_data = []
    
    for file_entry, file_info in detected_files:
        file_path = Path(file_entry.extracted_path)
        
        result = extractor.extract(file_path, file_info)
        extracted_data.append((file_entry, result))
        
        print(f"\n{file_entry.filename}:")
        print(f"   Extraction method: {result.extraction_method}")
        print(f"   Text length: {len(result.text) if result.text else 0} chars")
        print(f"   Records extracted: {len(result.records) if result.records else 0}")
        print(f"   Confidence: {result.confidence:.1%}")
        
        if result.records and len(result.records) > 0:
            print(f"   Sample record: {list(result.records[0].keys())[:5]}")
    
    return extracted_data


def test_step_4_segmentation(extracted_data):
    """Test Step 4: Record Segmentation"""
    print("\n" + "="*60)
    print("STEP 4: Record Segmentation & Labeling")
    print("="*60)
    
    segmenter = RecordSegmentationEngine()
    all_records = []
    
    for file_entry, extraction in extracted_data:
        print(f"\n✅ Processing {file_entry.filename}...")
        
        if extraction.records:
            # Segment structured records
            for i, record in enumerate(extraction.records):
                # Convert record to text for segmentation
                text = "\n".join(f"{k}: {v}" for k, v in record.items())
                
                provenance = {
                    "src_file": file_entry.filename,
                    "src_offset": i,
                    "sha256": file_entry.sha256_hash,
                    "extracted_from": extraction.extraction_method
                }
                
                segmented = segmenter.segment_record(text, record, provenance)
                all_records.append(segmented)
        else:
            # Segment plain text
            # Split by double newlines for separate records
            text_blocks = extraction.text.split('\n\n')
            for i, block in enumerate(text_blocks):
                if block.strip():
                    provenance = {
                        "src_file": file_entry.filename,
                        "src_offset": i,
                        "sha256": file_entry.sha256_hash,
                        "extracted_from": extraction.extraction_method
                    }
                    
                    segmented = segmenter.segment_record(block, None, provenance)
                    all_records.append(segmented)
        
        print(f"   Records segmented: {len([r for r in all_records if r.provenance.get('src_file') == file_entry.filename])}")
    
    # Summary by type
    print(f"\n📊 Segmentation Summary:")
    type_counts = {}
    for record in all_records:
        type_counts[record.type_label] = type_counts.get(record.type_label, 0) + 1
    
    for record_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        avg_conf = sum(r.confidence for r in all_records if r.type_label == record_type) / count
        print(f"   - {record_type.value}: {count} records (avg confidence: {avg_conf:.1%})")
    
    return all_records


def test_step_5_schema_inference(all_records):
    """Test Step 5: Dynamic Schema Inference"""
    print("\n" + "="*60)
    print("STEP 5: Dynamic Schema Inference")
    print("="*60)
    
    inference_engine = SchemaInferenceEngine(sample_size=1000, min_confidence=0.7)
    schemas = inference_engine.infer_schemas(all_records, version="1.0.0")
    
    print(f"\n✅ Schemas inferred for {len(schemas)} entity types")
    
    for record_type, schema in schemas.items():
        print(f"\n{record_type.value.upper()} Schema:")
        print(f"   Version: {schema.version}")
        print(f"   Fields: {len(schema.fields)}")
        print(f"   Indexes: {len(schema.indexes)}")
        print(f"   Sample size: {schema.metadata['sample_size']}")
        
        # Show key fields
        print(f"   Key fields:")
        for field_name in list(schema.fields.keys())[:5]:
            field = schema.fields[field_name]
            req = "required" if field.required else "optional"
            print(f"      - {field_name}: {field.field_type.value} ({req})")
    
    # Save schemas to file
    schema_dir = Path("data/schemas")
    schema_dir.mkdir(parents=True, exist_ok=True)
    
    for record_type, schema in schemas.items():
        schema_file = schema_dir / f"{record_type.value}_v{schema.version}.json"
        schema.save(schema_file)
    
    print(f"\n✅ Schemas saved to {schema_dir}")
    
    return schemas


def test_step_6_storage(all_records, schemas):
    """Test Step 6: Normalization & Storage"""
    print("\n" + "="*60)
    print("STEP 6: Normalization & Storage")
    print("="*60)
    
    # Create database
    db_path = "data/test_e2e_mvp.db"
    writer = BatchWriter(db_path, batch_size=10, checkpoint_interval=50)
    
    # Initialize database with schemas
    print(f"\n📊 Initializing database...")
    writer.initialize_database(schemas)
    print(f"✅ Database initialized: {db_path}")
    
    # Write records
    print(f"\n💾 Writing {len(all_records)} records...")
    
    def progress_callback(current, total):
        if current % 5 == 0 or current == total:
            print(f"   Progress: {current}/{total} ({current/total:.1%})")
    
    stats = writer.write_records(
        all_records, 
        case_id="TEST_CASE_E2E_001",
        progress_callback=progress_callback
    )
    
    print(f"\n✅ Write Statistics:")
    print(json.dumps(stats.to_dict(), indent=2))
    
    # Get provenance summary
    summary = ProvenanceTracker.get_provenance_summary(db_path, "TEST_CASE_E2E_001")
    print(f"\n📋 Provenance Summary:")
    print(json.dumps(summary, indent=2))
    
    return db_path, stats


def main():
    """Run end-to-end test"""
    print("\n" + "="*60)
    print("UFDR INGESTION PIPELINE - END-TO-END TEST")
    print("="*60)
    print("Testing Steps 0-6 (MVP Foundation)")
    print("="*60)
    
    try:
        # Step 0: Configuration
        test_step_0_config()
        
        # Create test data
        test_dir = create_test_data()
        
        # Step 1: Ingestion
        manifest = test_step_1_ingestion(test_dir)
        
        # Step 2: Detection
        detected_files = test_step_2_detection(manifest)
        
        # Step 3: Extraction
        extracted_data = test_step_3_extraction(detected_files)
        
        # Step 4: Segmentation
        all_records = test_step_4_segmentation(extracted_data)
        
        # Step 5: Schema Inference
        schemas = test_step_5_schema_inference(all_records)
        
        # Step 6: Storage
        db_path, stats = test_step_6_storage(all_records, schemas)
        
        # Final Summary
        print("\n" + "="*60)
        print("END-TO-END TEST COMPLETE!")
        print("="*60)
        print(f"\nAll 6 steps executed successfully!")
        print(f"\nFinal Results:")
        print(f"   - Test data: {test_dir}")
        print(f"   - Files ingested: {manifest.total_files}")
        print(f"   - Records processed: {len(all_records)}")
        print(f"   - Schemas generated: {len(schemas)}")
        print(f"   - Records stored: {stats.inserted}")
        print(f"   - Database: {db_path}")
        print(f"   - Success rate: {stats.to_dict()['success_rate']}")
        
        print(f"\nMVP Foundation (Steps 0-6) is COMPLETE and WORKING!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error:")
        print(f"   {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
