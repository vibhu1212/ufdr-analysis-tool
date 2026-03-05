"""
Step 3: Multi-Path Text Extraction
Extracts text from various sources: JSON/XML/CSV, plain text, images (OCR), audio/video (STT)
"""

import json
import csv
import xml.etree.ElementTree as ET
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging

try:
    import xmltodict
    HAS_XMLTODICT = True
except ImportError:
    HAS_XMLTODICT = False

import sys
from pathlib import Path as PathLib
sys.path.insert(0, str(PathLib(__file__).parent))

from file_type_detector import FileCategory, FileTypeInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExtractedText:
    """Container for extracted text with metadata"""
    text: str
    source_file: str
    extraction_method: str  # json, xml, csv, plain_text, ocr, stt
    confidence: float  # 0.0-1.0
    metadata: Dict[str, Any]
    records: Optional[List[Dict]] = None  # For structured data
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "text": self.text,
            "source_file": self.source_file,
            "extraction_method": self.extraction_method,
            "confidence": self.confidence,
            "metadata": self.metadata,
            "records": self.records
        }


class StructuredTextExtractor:
    """
    Extracts and normalizes text from structured formats (JSON, XML, CSV)
    """
    
    def __init__(self):
        self.supported_formats = ['.json', '.jsonl', '.xml', '.csv', '.tsv']
    
    def extract_json(self, file_path: Path) -> ExtractedText:
        """
        Extract data from JSON file
        
        Returns:
            ExtractedText with records list
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Flatten JSON to text
            text = json.dumps(data, ensure_ascii=False, indent=2)
            
            # Try to extract records if it's an array or has array fields
            records = []
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Look for array fields
                for key, value in data.items():
                    if isinstance(value, list) and value:
                        records.extend(value)
                        break
            
            return ExtractedText(
                text=text,
                source_file=str(file_path),
                extraction_method="json",
                confidence=1.0,
                metadata={
                    "format": "json",
                    "record_count": len(records) if records else 0
                },
                records=records if records else None
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed for {file_path}: {e}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="json",
                confidence=0.0,
                metadata={"error": str(e)},
                records=None
            )
    
    def extract_jsonl(self, file_path: Path) -> ExtractedText:
        """
        Extract data from JSONL (JSON Lines) file
        """
        try:
            records = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            
            # Combine all records to text
            text = '\n'.join(json.dumps(record, ensure_ascii=False) for record in records)
            
            return ExtractedText(
                text=text,
                source_file=str(file_path),
                extraction_method="jsonl",
                confidence=1.0,
                metadata={
                    "format": "jsonl",
                    "record_count": len(records)
                },
                records=records
            )
            
        except Exception as e:
            logger.error(f"JSONL parsing failed for {file_path}: {e}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="jsonl",
                confidence=0.0,
                metadata={"error": str(e)},
                records=None
            )
    
    def extract_xml(self, file_path: Path) -> ExtractedText:
        """
        Extract data from XML file
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Convert XML to text (element tags + text content)
            text_parts = []
            
            def traverse(element, depth=0):
                indent = "  " * depth
                if element.text and element.text.strip():
                    text_parts.append(f"{indent}{element.tag}: {element.text.strip()}")
                for child in element:
                    traverse(child, depth + 1)
            
            traverse(root)
            text = '\n'.join(text_parts)
            
            # Try to use xmltodict for better structure
            records = None
            if HAS_XMLTODICT:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        xml_dict = xmltodict.parse(f.read())
                    # Try to find array-like structures
                    for key, value in xml_dict.items():
                        if isinstance(value, dict):
                            for subkey, subvalue in value.items():
                                if isinstance(subvalue, list):
                                    records = subvalue
                                    break
                except Exception:
                    pass
            
            return ExtractedText(
                text=text,
                source_file=str(file_path),
                extraction_method="xml",
                confidence=0.9,
                metadata={
                    "format": "xml",
                    "root_tag": root.tag,
                    "record_count": len(records) if records else 0
                },
                records=records
            )
            
        except ET.ParseError as e:
            logger.error(f"XML parsing failed for {file_path}: {e}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="xml",
                confidence=0.0,
                metadata={"error": str(e)},
                records=None
            )
    
    def extract_csv(self, file_path: Path, delimiter: str = ',') -> ExtractedText:
        """
        Extract data from CSV/TSV file
        """
        try:
            records = []
            with open(file_path, 'r', encoding='utf-8') as f:
                # Detect delimiter
                sample = f.read(1024)
                f.seek(0)
                
                if '\t' in sample:
                    delimiter = '\t'
                
                reader = csv.DictReader(f, delimiter=delimiter)
                headers = reader.fieldnames
                
                for row in reader:
                    records.append(row)
            
            # Convert to text (table format)
            if headers and records:
                text_lines = [','.join(headers)]
                for record in records:
                    text_lines.append(','.join(str(record.get(h, '')) for h in headers))
                text = '\n'.join(text_lines)
            else:
                text = ""
            
            return ExtractedText(
                text=text,
                source_file=str(file_path),
                extraction_method="csv",
                confidence=1.0,
                metadata={
                    "format": "csv" if delimiter == ',' else "tsv",
                    "headers": headers,
                    "record_count": len(records)
                },
                records=records
            )
            
        except Exception as e:
            logger.error(f"CSV parsing failed for {file_path}: {e}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="csv",
                confidence=0.0,
                metadata={"error": str(e)},
                records=None
            )
    
    def extract(self, file_path: Path, file_info: FileTypeInfo) -> ExtractedText:
        """
        Universal extraction for structured formats
        
        Args:
            file_path: Path to file
            file_info: File type information from FileTypeDetector
            
        Returns:
            ExtractedText object
        """
        extension = file_path.suffix.lower()
        
        if extension == '.json':
            return self.extract_json(file_path)
        elif extension == '.jsonl':
            return self.extract_jsonl(file_path)
        elif extension == '.xml':
            return self.extract_xml(file_path)
        elif extension in ['.csv', '.tsv']:
            return self.extract_csv(file_path)
        else:
            logger.warning(f"Unsupported structured format: {extension}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="unsupported",
                confidence=0.0,
                metadata={"error": f"Unsupported format: {extension}"},
                records=None
            )


class PlainTextExtractor:
    """
    Extracts text from plain text files with basic structure detection
    """
    
    def __init__(self):
        self.key_value_pattern = re.compile(r'^([A-Za-z_]+):\s*(.+)$', re.MULTILINE)
    
    def extract_key_value_pairs(self, text: str) -> List[Dict[str, str]]:
        """
        Extract key-value pairs from text (e.g., "Name: John Doe")
        """
        pairs = []
        matches = self.key_value_pattern.findall(text)
        
        if matches:
            record = {}
            for key, value in matches:
                record[key.lower()] = value.strip()
            pairs.append(record)
        
        return pairs
    
    def detect_tables(self, text: str) -> Optional[List[Dict]]:
        """
        Detect simple text tables (pipe or tab separated)
        """
        lines = text.split('\n')
        
        # Check if it looks like a table
        if len(lines) < 2:
            return None
        
        # Look for pipe-separated values
        if '|' in lines[0]:
            headers = [h.strip() for h in lines[0].split('|') if h.strip()]
            records = []
            
            for line in lines[1:]:
                if '|' in line and not line.strip().startswith('|---'):
                    values = [v.strip() for v in line.split('|') if v.strip()]
                    if len(values) == len(headers):
                        record = dict(zip(headers, values))
                        records.append(record)
            
            return records if records else None
        
        return None
    
    def extract(self, file_path: Path, file_info: FileTypeInfo) -> ExtractedText:
        """
        Extract text from plain text file
        
        Args:
            file_path: Path to file
            file_info: File type information
            
        Returns:
            ExtractedText object
        """
        try:
            encoding = file_info.encoding or 'utf-8'
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                text = f.read()
            
            # Try to detect structure
            key_value_records = self.extract_key_value_pairs(text)
            table_records = self.detect_tables(text)
            
            records = key_value_records or table_records
            
            return ExtractedText(
                text=text,
                source_file=str(file_path),
                extraction_method="plain_text",
                confidence=1.0,
                metadata={
                    "format": "plain_text",
                    "encoding": encoding,
                    "has_structure": records is not None,
                    "line_count": len(text.split('\n'))
                },
                records=records
            )
            
        except Exception as e:
            logger.error(f"Plain text extraction failed for {file_path}: {e}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="plain_text",
                confidence=0.0,
                metadata={"error": str(e)},
                records=None
            )


class TextExtractionEngine:
    """
    Main text extraction engine that routes to appropriate extractors
    """
    
    def __init__(self):
        self.structured_extractor = StructuredTextExtractor()
        self.plain_text_extractor = PlainTextExtractor()
    
    def extract(self, file_path: Path, file_info: FileTypeInfo) -> ExtractedText:
        """
        Extract text from file based on its type
        
        Args:
            file_path: Path to file
            file_info: File type information from FileTypeDetector
            
        Returns:
            ExtractedText object
        """
        category = file_info.category
        logger.debug(f"Extracting from category: {category} ({category.value})")
        
        if category == FileCategory.STRUCTURED_TEXT:
            logger.info(f"Extracting structured text from {file_path.name}")
            return self.structured_extractor.extract(file_path, file_info)
        
        elif category == FileCategory.PLAIN_TEXT:
            logger.info(f"Extracting plain text from {file_path.name}")
            return self.plain_text_extractor.extract(file_path, file_info)
        
        elif category == FileCategory.IMAGE or category == FileCategory.PDF:
            logger.warning(f"OCR not yet implemented for {file_path.name}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="ocr_pending",
                confidence=0.0,
                metadata={
                    "status": "OCR not implemented yet",
                    "category": category.value
                },
                records=None
            )
        
        elif category in [FileCategory.AUDIO, FileCategory.VIDEO]:
            logger.warning(f"STT not yet implemented for {file_path.name}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="stt_pending",
                confidence=0.0,
                metadata={
                    "status": "STT not implemented yet",
                    "category": category.value
                },
                records=None
            )
        
        else:
            logger.warning(f"Unsupported category for text extraction: {category.value}")
            return ExtractedText(
                text="",
                source_file=str(file_path),
                extraction_method="unsupported",
                confidence=0.0,
                metadata={
                    "status": "Unsupported file category",
                    "category": category.value
                },
                records=None
            )


if __name__ == "__main__":
    # Test text extraction
    from file_type_detector import FileTypeDetector
    
    if len(sys.argv) < 2:
        print("Usage: python text_extractors.py <file_path>")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)
    
    # Detect file type first
    detector = FileTypeDetector()
    file_info = detector.detect(file_path, "text/plain")
    
    # Extract text
    engine = TextExtractionEngine()
    result = engine.extract(file_path, file_info)
    
    print(f"\n{'='*60}")
    print(f"Text Extraction: {file_path.name}")
    print(f"{'='*60}")
    print(f"Method: {result.extraction_method}")
    print(f"Confidence: {result.confidence:.2%}")
    print(f"Metadata: {result.metadata}")
    
    if result.records:
        print(f"\nExtracted Records: {len(result.records)}")
        for i, record in enumerate(result.records[:3], 1):
            print(f"  {i}. {record}")
    
    if result.text:
        print(f"\nExtracted Text (first 500 chars):")
        print(result.text[:500])
        print("..." if len(result.text) > 500 else "")
