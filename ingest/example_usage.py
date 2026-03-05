"""
Example Usage of UFDR Ingestion Pipeline (Steps 0-2)

Demonstrates:
1. Configuration setup
2. Format-agnostic file ingestion
3. File type detection with script analysis
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest.config import (
    get_config,
    ProcessingMode,
    validate_config,
)

from ingest.file_ingestor import (
    FileIngestor,
    load_manifest,
)

from ingest.file_type_detector import (
    FileTypeDetector,
    get_language_from_script,
)


def example_1_configuration():
    """Example 1: Working with different configuration modes"""
    
    print("\n" + "="*70)
    print("Example 1: Configuration Setup")
    print("="*70)
    
    # Get different configuration modes
    for mode in ProcessingMode:
        config = get_config(mode)
        print(f"\n📋 {mode.value.upper()} Mode Configuration:")
        print(f"   Batch size: {config.batch_size:,}")
        print(f"   Cloud LLM: {config.use_cloud_llm}")
        print(f"   Multi-signal detection: {config.enable_multi_signal_detection}")
        print(f"   Deduplication: {config.enable_deduplication}")
        
        # Validate configuration
        warnings = validate_config(config)
        if warnings:
            print(f"   ⚠️  Warnings:")
            for warning in warnings:
                print(f"      {warning}")


def example_2_ingest_single_file():
    """Example 2: Ingest a single file"""
    
    print("\n" + "="*70)
    print("Example 2: Ingest Single File")
    print("="*70)
    
    # Create a test file
    test_file = Path("data/test_file.json")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write('''{
    "contacts": [
        {
            "name": "राज कुमार",
            "phone": "+919876543210",
            "email": "raj.kumar@example.com"
        },
        {
            "name": "Priya Sharma",
            "phone": "+918765432109",
            "email": "priya.s@example.com"
        }
    ]
}''')
    
    # Ingest the file
    ingestor = FileIngestor()
    manifest = ingestor.ingest(test_file)
    
    print(f"\n✅ Ingestion Complete!")
    print(f"   Ingestion ID: {manifest.ingestion_id}")
    print(f"   Source: {manifest.source_path}")
    print(f"   Type: {manifest.source_type}")
    print(f"   Files: {manifest.total_files}")
    print(f"   Size: {manifest.total_size_bytes:,} bytes")
    print(f"   Workspace: {manifest.workspace_path}")
    
    # Show file details
    for entry in manifest.files:
        print(f"\n📄 File Details:")
        print(f"   Filename: {entry.filename}")
        print(f"   MIME Type: {entry.mime_type}")
        print(f"   SHA256: {entry.sha256_hash[:16]}...")
        print(f"   Timestamp: {entry.timestamp}")
    
    return manifest


def example_3_ingest_folder():
    """Example 3: Ingest a folder with multiple files"""
    
    print("\n" + "="*70)
    print("Example 3: Ingest Folder")
    print("="*70)
    
    # Create test folder with multiple files
    test_folder = Path("data/test_folder")
    test_folder.mkdir(parents=True, exist_ok=True)
    
    # Create contacts file
    (test_folder / "contacts.csv").write_text(
        "name,phone,email\n"
        "राज कुमार,+919876543210,raj@example.com\n"
        "అనిల్ రెడ్డి,+919123456789,anil@example.com\n"
        "রাজেশ বোস,+918912345678,rajesh@example.com\n",
        encoding='utf-8'
    )
    
    # Create messages file
    (test_folder / "messages.txt").write_text(
        "Message 1: हैलो, कैसे हो?\n"
        "Message 2: నేను బాగున్నాను, ధన్యవాదాలు!\n"
        "Message 3: আমি ভালো আছি\n",
        encoding='utf-8'
    )
    
    # Create notes file
    (test_folder / "notes.txt").write_text(
        "Investigation notes for Case #12345\n"
        "Date: 2025-01-02\n"
        "Evidence collected from Device A\n",
        encoding='utf-8'
    )
    
    # Ingest folder
    ingestor = FileIngestor()
    manifest = ingestor.ingest(test_folder)
    
    print(f"\n✅ Folder Ingestion Complete!")
    print(f"   Total files: {manifest.total_files}")
    print(f"   Total size: {manifest.total_size_bytes:,} bytes")
    
    print(f"\n📁 Files:")
    for i, entry in enumerate(manifest.files, 1):
        print(f"   {i}. {entry.filename} ({entry.file_size:,} bytes) - {entry.mime_type}")
    
    return manifest


def example_4_file_type_detection():
    """Example 4: Detect file types with script analysis"""
    
    print("\n" + "="*70)
    print("Example 4: File Type Detection with Script Analysis")
    print("="*70)
    
    # Create test files with different scripts
    test_files = {
        "hindi.txt": "यह एक हिंदी पाठ है। राज कुमार +919876543210",
        "telugu.txt": "ఇది తెలుగు వచనం. అనిల్ రెడ్డి +919123456789",
        "bengali.txt": "এটি বাংলা পাঠ্য। রাজেশ বোস +918912345678",
        "mixed.txt": "Mixed text: राज Kumar (Hindi+English) +919876543210",
        "english.txt": "This is English text. John Doe +919876543210",
    }
    
    test_dir = Path("data/test_scripts")
    test_dir.mkdir(parents=True, exist_ok=True)
    
    detector = FileTypeDetector()
    
    for filename, content in test_files.items():
        filepath = test_dir / filename
        filepath.write_text(content, encoding='utf-8')
        
        # Detect file type
        info = detector.detect(filepath, "text/plain")
        
        print(f"\n📝 {filename}:")
        print(f"   Category: {info.category.value}")
        print(f"   Encoding: {info.encoding}")
        print(f"   Confidence: {info.confidence:.2%}")
        
        if info.scripts:
            script_names = [s.value for s in info.scripts]
            print(f"   Detected Scripts: {', '.join(script_names)}")
        
        if info.dominant_script:
            print(f"   Dominant Script: {info.dominant_script.value}")
            languages = get_language_from_script(info.dominant_script)
            print(f"   Likely Languages: {', '.join(languages)}")


def example_5_comprehensive_pipeline():
    """Example 5: Complete pipeline (ingest + detect)"""
    
    print("\n" + "="*70)
    print("Example 5: Complete Ingestion + Detection Pipeline")
    print("="*70)
    
    # Get configuration
    config = get_config(ProcessingMode.STANDARD)
    print(f"\n🔧 Using {config.mode.value.upper()} mode")
    
    # Ingest test folder
    test_folder = Path("data/test_folder")
    ingestor = FileIngestor()
    
    if test_folder.exists():
        manifest = ingestor.ingest(test_folder)
        
        print(f"\n✅ Ingestion: {manifest.total_files} files ingested")
        
        # Detect file types
        detector = FileTypeDetector()
        
        print(f"\n🔍 File Type Detection:")
        print(f"{'Filename':<20} {'Category':<18} {'Scripts':<30} {'Languages':<15}")
        print("-" * 85)
        
        for entry in manifest.files:
            file_path = Path(entry.extracted_path)
            info = detector.detect(file_path, entry.mime_type)
            
            # Format scripts
            if info.scripts:
                scripts_str = ', '.join([s.value for s in info.scripts[:2]])
                if len(info.scripts) > 2:
                    scripts_str += f", +{len(info.scripts)-2}"
            else:
                scripts_str = "None"
            
            # Format languages
            if info.dominant_script:
                langs = get_language_from_script(info.dominant_script)
                langs_str = ', '.join(langs[:3])
            else:
                langs_str = "en"
            
            print(f"{entry.filename:<20} {info.category.value:<18} {scripts_str:<30} {langs_str:<15}")
        
        # Summary statistics
        print(f"\n📊 Summary:")
        categories = {}
        scripts = {}
        
        for entry in manifest.files:
            file_path = Path(entry.extracted_path)
            info = detector.detect(file_path, entry.mime_type)
            
            # Count categories
            categories[info.category] = categories.get(info.category, 0) + 1
            
            # Count scripts
            for script in info.scripts:
                scripts[script] = scripts.get(script, 0) + 1
        
        print(f"\n   File Categories:")
        for category, count in categories.items():
            print(f"      {category.value}: {count}")
        
        if scripts:
            print(f"\n   Detected Scripts:")
            for script, count in sorted(scripts.items(), key=lambda x: x[1], reverse=True):
                print(f"      {script.value}: {count}")


def example_6_load_manifest():
    """Example 6: Load and inspect a saved manifest"""
    
    print("\n" + "="*70)
    print("Example 6: Load Saved Manifest")
    print("="*70)
    
    # Find most recent manifest
    workspace_root = Path("data/ingestion_workspace")
    
    if workspace_root.exists():
        manifests = list(workspace_root.glob("*/manifest.json"))
        
        if manifests:
            latest_manifest = max(manifests, key=lambda p: p.stat().st_mtime)
            
            print(f"\n📂 Loading manifest: {latest_manifest}")
            
            # Load manifest
            manifest = load_manifest(latest_manifest)
            
            print(f"\n✅ Manifest Loaded:")
            print(f"   Ingestion ID: {manifest.ingestion_id}")
            print(f"   Source: {manifest.source_path}")
            print(f"   Type: {manifest.source_type}")
            print(f"   Created: {manifest.created_at}")
            print(f"   Files: {manifest.total_files}")
            print(f"   Total Size: {manifest.total_size_bytes:,} bytes")
            
            # File provenance
            if manifest.files:
                print(f"\n🔍 First File Provenance:")
                entry = manifest.files[0]
                print(f"   File ID: {entry.file_id}")
                print(f"   Original Path: {entry.original_path}")
                print(f"   Extracted Path: {entry.extracted_path}")
                print(f"   SHA256: {entry.sha256_hash}")
                print(f"   MIME Type: {entry.mime_type}")
                print(f"   Timestamp: {entry.timestamp}")
        else:
            print("\n⚠️  No manifests found. Run examples 2 or 3 first.")
    else:
        print("\n⚠️  Workspace not found. Run examples 2 or 3 first.")


def main():
    """Run all examples"""
    
    print("\n" + "🚀 "*30)
    print("UFDR Ingestion Pipeline - Example Usage (Steps 0-2)")
    print("🚀 "*30)
    
    try:
        # Run examples
        example_1_configuration()
        example_2_ingest_single_file()
        example_3_ingest_folder()
        example_4_file_type_detection()
        example_5_comprehensive_pipeline()
        example_6_load_manifest()
        
        print("\n" + "="*70)
        print("✅ All Examples Completed Successfully!")
        print("="*70)
        print("\nNext Steps:")
        print("  1. Install dependencies: pip install -r requirements.txt")
        print("  2. Run this script: python example_usage.py")
        print("  3. Check output in: data/ingestion_workspace/")
        print("  4. Review manifest.json files for full provenance")
        print("\nUpcoming (Steps 3-16):")
        print("  - Multi-path text extraction (OCR, STT)")
        print("  - Dynamic schema inference")
        print("  - Multi-signal name detection")
        print("  - Human-in-the-loop review")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
