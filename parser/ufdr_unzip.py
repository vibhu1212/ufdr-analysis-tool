"""
UFDR Unzip Utility
Handles extraction and validation of UFDR forensic report files
Maintains chain of custody through SHA256 hashing
"""

import zipfile
import hashlib
import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
import logging
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExtractionManifest:
    """Manifest for tracking extraction details"""
    case_id: str
    original_file: str
    sha256_hash: str
    extraction_time: str
    extraction_path: str
    file_size: int
    extracted_files_count: int
    operator: Optional[str] = None
    notes: Optional[str] = None


class UFDRExtractor:
    """Handles secure extraction of UFDR files with integrity checking"""
    
    def __init__(self, base_path: str = "data/raw"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    def calculate_sha256(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file for integrity verification"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def validate_ufdr(self, file_path: Path) -> Tuple[bool, str]:
        """Validate UFDR file format and structure"""
        if not file_path.exists():
            return False, "File does not exist"
        
        if not file_path.suffix.lower() in ['.ufdr', '.zip']:
            return False, "Invalid file extension (expected .ufdr or .zip)"
        
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                # Check for expected UFDR structure
                namelist = zf.namelist()
                
                # Look for common UFDR components
                has_report = any('report' in name.lower() or '.xml' in name.lower() 
                                for name in namelist)
                
                if not has_report:
                    logger.warning("No report.xml found in UFDR - may be non-standard format")
                
                # Test integrity
                corrupt_files = zf.testzip()
                if corrupt_files:
                    return False, f"Corrupt files detected: {corrupt_files}"
                    
            return True, "Valid UFDR file"
            
        except zipfile.BadZipFile:
            return False, "Invalid or corrupt zip file"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def extract(self, 
                ufdr_path: str, 
                case_id: Optional[str] = None,
                operator: Optional[str] = None,
                preserve_original: bool = True) -> Dict:
        """
        Extract UFDR file with full audit trail
        
        Args:
            ufdr_path: Path to UFDR file
            case_id: Unique case identifier
            operator: Name of operator performing extraction
            preserve_original: Keep copy of original file
            
        Returns:
            Extraction manifest as dictionary
        """
        ufdr_file = Path(ufdr_path)
        
        # Validate input
        is_valid, message = self.validate_ufdr(ufdr_file)
        if not is_valid:
            raise ValueError(f"Invalid UFDR file: {message}")
        
        # Generate case ID if not provided
        if not case_id:
            case_id = f"case_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create extraction directory
        extraction_path = self.base_path / case_id
        if extraction_path.exists():
            logger.warning(f"Extraction path exists, backing up to {extraction_path}_backup")
            shutil.move(str(extraction_path), str(extraction_path) + "_backup")
        extraction_path.mkdir(parents=True, exist_ok=True)
        
        # Calculate hash BEFORE extraction
        logger.info("Calculating SHA256 hash...")
        sha256_hash = self.calculate_sha256(ufdr_file)
        
        # Preserve original if requested
        if preserve_original:
            original_copy = extraction_path / f"original_{ufdr_file.name}"
            shutil.copy2(ufdr_file, original_copy)
            logger.info(f"Original file preserved at {original_copy}")
        
        # Extract files
        logger.info(f"Extracting UFDR to {extraction_path}")
        extracted_count = 0
        
        try:
            with zipfile.ZipFile(ufdr_file, 'r') as zf:
                # Extract with path preservation
                for member in zf.namelist():
                    # Security: prevent path traversal
                    if os.path.isabs(member) or ".." in member:
                        logger.warning(f"Skipping potentially unsafe path: {member}")
                        continue
                    
                    zf.extract(member, extraction_path)
                    extracted_count += 1
                    
                    if extracted_count % 100 == 0:
                        logger.info(f"Extracted {extracted_count} files...")
        
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            # Cleanup on failure
            if extraction_path.exists():
                shutil.rmtree(extraction_path)
            raise
        
        # Create extraction manifest
        manifest = ExtractionManifest(
            case_id=case_id,
            original_file=str(ufdr_file.absolute()),
            sha256_hash=sha256_hash,
            extraction_time=datetime.utcnow().isoformat() + "Z",
            extraction_path=str(extraction_path.absolute()),
            file_size=ufdr_file.stat().st_size,
            extracted_files_count=extracted_count,
            operator=operator,
            notes=f"Extracted using UFDRExtractor v1.0"
        )
        
        # Save manifest
        manifest_path = extraction_path / "extraction_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(asdict(manifest), f, indent=2)
        
        logger.info(f"Extraction complete: {extracted_count} files extracted")
        logger.info(f"SHA256: {sha256_hash}")
        logger.info(f"Manifest saved to {manifest_path}")
        
        return asdict(manifest)
    
    def verify_extraction(self, case_id: str) -> Tuple[bool, str]:
        """Verify extraction integrity using manifest"""
        extraction_path = self.base_path / case_id
        manifest_path = extraction_path / "extraction_manifest.json"
        
        if not manifest_path.exists():
            return False, "Manifest not found"
        
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            # Verify original file if preserved
            original_files = list(extraction_path.glob("original_*"))
            if original_files:
                original_hash = self.calculate_sha256(original_files[0])
                if original_hash != manifest['sha256_hash']:
                    return False, "Hash mismatch - possible tampering detected"
            
            return True, "Extraction verified successfully"
            
        except Exception as e:
            return False, f"Verification failed: {str(e)}"


def main():
    """CLI interface for UFDR extraction"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract UFDR forensic files")
    parser.add_argument("ufdr_file", help="Path to UFDR file")
    parser.add_argument("--case-id", help="Case identifier")
    parser.add_argument("--operator", help="Operator name")
    parser.add_argument("--output-dir", default="data/raw", help="Output directory")
    parser.add_argument("--no-preserve", action="store_true", 
                       help="Don't preserve original file")
    
    args = parser.parse_args()
    
    extractor = UFDRExtractor(args.output_dir)
    
    try:
        manifest = extractor.extract(
            args.ufdr_file,
            case_id=args.case_id,
            operator=args.operator,
            preserve_original=not args.no_preserve
        )
        
        print(f"\n✓ Extraction successful!")
        print(f"  Case ID: {manifest['case_id']}")
        print(f"  SHA256: {manifest['sha256_hash']}")
        print(f"  Files: {manifest['extracted_files_count']}")
        print(f"  Path: {manifest['extraction_path']}")
        
    except Exception as e:
        print(f"\n✗ Extraction failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())