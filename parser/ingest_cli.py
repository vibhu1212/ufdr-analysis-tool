"""
UFDR Ingest CLI
Unified command-line interface for ingesting UFDR files
Combines extraction and parsing in a single workflow
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

from ufdr_unzip import UFDRExtractor
from ufdr_parser import UFDRStreamParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UFDRIngestor:
    """Orchestrates the complete UFDR ingestion pipeline"""
    
    def __init__(self, 
                 raw_dir: str = "data/raw",
                 parsed_dir: str = "data/parsed"):
        self.raw_dir = Path(raw_dir)
        self.parsed_dir = Path(parsed_dir)
        self.extractor = UFDRExtractor(raw_dir)
        
    def ingest(self, 
               ufdr_path: str,
               case_id: Optional[str] = None,
               operator: Optional[str] = None) -> Dict:
        """
        Complete ingestion pipeline for UFDR file
        
        Args:
            ufdr_path: Path to UFDR file
            case_id: Unique case identifier
            operator: Name of operator
            
        Returns:
            Complete ingestion manifest
        """
        ufdr_file = Path(ufdr_path)
        
        if not ufdr_file.exists():
            raise FileNotFoundError(f"UFDR file not found: {ufdr_path}")
        
        # Generate case ID if not provided
        if not case_id:
            case_id = f"case_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting ingestion for case: {case_id}")
        
        # Step 1: Extract UFDR
        logger.info("Step 1/3: Extracting UFDR file...")
        extraction_manifest = self.extractor.extract(
            ufdr_path,
            case_id=case_id,
            operator=operator,
            preserve_original=True
        )
        
        # Step 2: Find and parse XML files
        logger.info("Step 2/3: Parsing extracted data...")
        extraction_path = Path(extraction_manifest['extraction_path'])
        
        # Look for XML files in extraction
        xml_files = list(extraction_path.rglob("*.xml"))
        
        if not xml_files:
            logger.warning("No XML files found in extraction - looking for other formats")
            # Could add handlers for JSON, CSV, etc. here
        
        parsing_manifests = []
        parser = UFDRStreamParser(case_id, self.parsed_dir)
        
        for xml_file in xml_files:
            logger.info(f"Parsing: {xml_file.name}")
            try:
                parse_manifest = parser.parse_ufdr_xml(str(xml_file))
                parsing_manifests.append(parse_manifest)
            except Exception as e:
                logger.error(f"Failed to parse {xml_file}: {e}")
        
        # Step 3: Create unified ingestion manifest
        logger.info("Step 3/3: Creating ingestion manifest...")
        
        # Aggregate statistics
        total_stats = {
            "messages": 0,
            "calls": 0,
            "contacts": 0,
            "media": 0,
            "locations": 0,
            "devices": 0
        }
        
        for manifest in parsing_manifests:
            for key in total_stats:
                total_stats[key] += manifest.get('statistics', {}).get(key, 0)
        
        # Create complete manifest
        ingestion_manifest = {
            "case_id": case_id,
            "ingestion_time": datetime.utcnow().isoformat() + "Z",
            "operator": operator,
            "source_file": {
                "path": extraction_manifest['original_file'],
                "sha256": extraction_manifest['sha256_hash'],
                "size": extraction_manifest['file_size']
            },
            "extraction": {
                "path": extraction_manifest['extraction_path'],
                "files_count": extraction_manifest['extracted_files_count'],
                "time": extraction_manifest['extraction_time']
            },
            "parsing": {
                "xml_files_processed": len(xml_files),
                "statistics": total_stats,
                "output_path": str(self.parsed_dir / case_id)
            },
            "status": "completed" if parsing_manifests else "extracted_only"
        }
        
        # Save complete manifest
        manifest_path = self.parsed_dir / case_id / "ingestion_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(manifest_path, 'w') as f:
            json.dump(ingestion_manifest, f, indent=2)
        
        logger.info(f"Ingestion manifest saved to: {manifest_path}")
        
        # Also save to audit log
        self._append_to_audit_log(ingestion_manifest)
        
        return ingestion_manifest
    
    def _append_to_audit_log(self, manifest: Dict):
        """Append ingestion event to audit log"""
        audit_dir = Path("data/audit_logs")
        audit_dir.mkdir(parents=True, exist_ok=True)
        
        audit_file = audit_dir / f"ingestion_log_{datetime.now().strftime('%Y%m')}.jsonl"
        
        # Create audit entry
        audit_entry = {
            "timestamp": manifest['ingestion_time'],
            "event": "ingestion",
            "case_id": manifest['case_id'],
            "operator": manifest['operator'],
            "sha256": manifest['source_file']['sha256'],
            "status": manifest['status']
        }
        
        # Append to log file (one JSON object per line)
        with open(audit_file, 'a') as f:
            f.write(json.dumps(audit_entry) + '\n')


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="Ingest UFDR forensic files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s case1.ufdr --case-id CASE001 --operator "John Doe"
  %(prog)s evidence.zip --operator "Jane Smith"
        """
    )
    
    parser.add_argument(
        "ufdr_file",
        help="Path to UFDR file (.ufdr or .zip)"
    )
    
    parser.add_argument(
        "--case-id",
        help="Unique case identifier (auto-generated if not provided)"
    )
    
    parser.add_argument(
        "--operator",
        help="Name of operator performing ingestion"
    )
    
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Directory for raw extracted files (default: data/raw)"
    )
    
    parser.add_argument(
        "--parsed-dir",
        default="data/parsed",
        help="Directory for parsed data (default: data/parsed)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create ingestor
    ingestor = UFDRIngestor(args.raw_dir, args.parsed_dir)
    
    try:
        # Run ingestion
        manifest = ingestor.ingest(
            args.ufdr_file,
            case_id=args.case_id,
            operator=args.operator
        )
        
        # Print summary
        print("\n" + "="*60)
        print("✓ UFDR Ingestion Successful!")
        print("="*60)
        print(f"  Case ID:     {manifest['case_id']}")
        print(f"  SHA256:      {manifest['source_file']['sha256']}")
        print(f"  Status:      {manifest['status']}")
        print(f"\n  Statistics:")
        for key, count in manifest['parsing']['statistics'].items():
            if count > 0:
                print(f"    - {key.capitalize()}: {count:,}")
        print(f"\n  Output Path: {manifest['parsing']['output_path']}")
        print("="*60)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Ingestion failed: {str(e)}", file=sys.stderr)
        logger.exception("Ingestion error")
        return 1


if __name__ == "__main__":
    sys.exit(main())