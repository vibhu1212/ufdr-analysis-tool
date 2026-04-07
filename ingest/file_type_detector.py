"""
File Type Detection and Normalization
Step 2: Detect MIME, Unicode script, encoding, and tag files for processing
"""

import chardet
import unicodedata
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

from ingest.config import FileTypeConfig, DEFAULT_FILE_TYPE_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileCategory(Enum):
    """High-level file categories for processing"""
    STRUCTURED_TEXT = "structured_text"  # JSON, XML, CSV
    PLAIN_TEXT = "plain_text"  # TXT, LOG, MD
    IMAGE = "image"  # JPG, PNG, etc.
    PDF = "pdf"
    AUDIO = "audio"
    VIDEO = "video"
    ARCHIVE = "archive"
    BINARY = "binary"
    UNKNOWN = "unknown"


class UnicodeScript(Enum):
    """Common Unicode scripts for forensic analysis"""
    LATIN = "Latin"
    DEVANAGARI = "Devanagari"  # Hindi, Marathi
    BENGALI = "Bengali"
    TELUGU = "Telugu"
    TAMIL = "Tamil"
    GUJARATI = "Gujarati"
    KANNADA = "Kannada"
    MALAYALAM = "Malayalam"
    GURMUKHI = "Gurmukhi"  # Punjabi
    ORIYA = "Oriya"
    ARABIC = "Arabic"
    CYRILLIC = "Cyrillic"
    HAN = "Han"  # Chinese
    HIRAGANA = "Hiragana"  # Japanese
    KATAKANA = "Katakana"  # Japanese
    HANGUL = "Hangul"  # Korean
    THAI = "Thai"
    HEBREW = "Hebrew"
    GREEK = "Greek"
    COMMON = "Common"  # Numbers, punctuation
    UNKNOWN = "Unknown"


@dataclass
class FileTypeInfo:
    """Complete file type information"""
    category: FileCategory
    mime_type: str
    extension: str
    encoding: Optional[str] = None  # For text files
    scripts: List[UnicodeScript] = None  # Detected Unicode scripts
    dominant_script: Optional[UnicodeScript] = None
    is_binary: bool = False
    is_encrypted: bool = False
    confidence: float = 1.0  # Detection confidence
    metadata: Dict = None
    
    def __post_init__(self):
        if self.scripts is None:
            self.scripts = []
        if self.metadata is None:
            self.metadata = {}


class FileTypeDetector:
    """
    Comprehensive file type detection with MIME, script, and encoding analysis
    """
    
    def __init__(self, config: FileTypeConfig = None):
        """
        Initialize file type detector
        
        Args:
            config: File type configuration
        """
        self.config = config or DEFAULT_FILE_TYPE_CONFIG
        
        # Build category mappings
        self._build_category_maps()
    
    def _build_category_maps(self):
        """Build extension to category mappings"""
        self.extension_to_category = {}
        
        for ext in self.config.supported_structured:
            self.extension_to_category[ext] = FileCategory.STRUCTURED_TEXT
        
        for ext in self.config.supported_text:
            self.extension_to_category[ext] = FileCategory.PLAIN_TEXT
        
        for ext in self.config.supported_images:
            self.extension_to_category[ext] = FileCategory.IMAGE
        
        for ext in self.config.supported_pdf:
            self.extension_to_category[ext] = FileCategory.PDF
        
        for ext in self.config.supported_audio:
            self.extension_to_category[ext] = FileCategory.AUDIO
        
        for ext in self.config.supported_video:
            self.extension_to_category[ext] = FileCategory.VIDEO
        
        for ext in self.config.supported_archives:
            self.extension_to_category[ext] = FileCategory.ARCHIVE
        
        for ext in self.config.supported_binary:
            self.extension_to_category[ext] = FileCategory.BINARY
    
    def _detect_encoding(self, file_path: Path) -> Tuple[Optional[str], float]:
        """
        Detect character encoding using chardet
        
        Returns:
            Tuple of (encoding, confidence)
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB
                result = chardet.detect(raw_data)
                return result['encoding'], result['confidence']
        except Exception as e:
            logger.debug(f"Encoding detection failed: {e}")
            return None, 0.0
    
    def _normalize_encoding(self, text: str) -> str:
        """
        Normalize text encoding to canonical form
        - Convert to NFC (Canonical Decomposition, followed by Canonical Composition)
        - Normalize newlines to \n
        """
        # Unicode normalization
        text = unicodedata.normalize('NFC', text)
        
        # Normalize newlines
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        return text
    
    def _detect_script(self, char: str) -> UnicodeScript:
        """
        Detect Unicode script for a single character
        """
        try:
            script_name = unicodedata.name(char).split()[0]
            
            # Map Unicode script names to our enum
            script_mapping = {
                'LATIN': UnicodeScript.LATIN,
                'DEVANAGARI': UnicodeScript.DEVANAGARI,
                'BENGALI': UnicodeScript.BENGALI,
                'TELUGU': UnicodeScript.TELUGU,
                'TAMIL': UnicodeScript.TAMIL,
                'GUJARATI': UnicodeScript.GUJARATI,
                'KANNADA': UnicodeScript.KANNADA,
                'MALAYALAM': UnicodeScript.MALAYALAM,
                'GURMUKHI': UnicodeScript.GURMUKHI,
                'ORIYA': UnicodeScript.ORIYA,
                'ARABIC': UnicodeScript.ARABIC,
                'CYRILLIC': UnicodeScript.CYRILLIC,
                'CJK': UnicodeScript.HAN,
                'HIRAGANA': UnicodeScript.HIRAGANA,
                'KATAKANA': UnicodeScript.KATAKANA,
                'HANGUL': UnicodeScript.HANGUL,
                'THAI': UnicodeScript.THAI,
                'HEBREW': UnicodeScript.HEBREW,
                'GREEK': UnicodeScript.GREEK,
            }
            
            for key, script in script_mapping.items():
                if key in script_name:
                    return script
            
            # Check if it's a digit or punctuation
            if char.isdigit() or char in '.,!?;:\'"()-[]{}':
                return UnicodeScript.COMMON
                
        except (ValueError, AttributeError):
            pass
        
        return UnicodeScript.UNKNOWN
    
    def _analyze_scripts(self, text: str, sample_size: int = 1000) -> Tuple[List[UnicodeScript], UnicodeScript]:
        """
        Analyze Unicode scripts in text
        
        Returns:
            Tuple of (list of detected scripts, dominant script)
        """
        # Sample text if too long
        if len(text) > sample_size:
            # Sample from beginning, middle, end
            sample = text[:sample_size//3] + text[len(text)//2:len(text)//2 + sample_size//3] + text[-sample_size//3:]
        else:
            sample = text
        
        # Count scripts
        script_counts = {}
        total_chars = 0
        
        for char in sample:
            if char.isspace():
                continue
            
            script = self._detect_script(char)
            if script != UnicodeScript.COMMON and script != UnicodeScript.UNKNOWN:
                script_counts[script] = script_counts.get(script, 0) + 1
                total_chars += 1
        
        if not script_counts:
            return [UnicodeScript.LATIN], UnicodeScript.LATIN  # Default
        
        # Get unique scripts
        detected_scripts = list(script_counts.keys())
        
        # Find dominant script
        dominant_script = max(script_counts.items(), key=lambda x: x[1])[0]
        
        return detected_scripts, dominant_script
    
    def _is_binary_file(self, file_path: Path, sample_size: int = 8192) -> bool:
        """
        Check if file is binary by looking for null bytes
        """
        try:
            with open(file_path, 'rb') as f:
                chunk = f.read(sample_size)
                # If file contains null bytes, it's likely binary
                if b'\x00' in chunk:
                    return True
                # Check for high ratio of non-printable characters
                non_printable = sum(1 for byte in chunk if byte < 32 and byte not in (9, 10, 13))
                if non_printable / len(chunk) > 0.3:
                    return True
        except Exception:
            return True
        
        return False
    
    def _detect_encryption(self, file_path: Path) -> bool:
        """
        Detect if file might be encrypted
        Simple heuristic: high entropy in first few KB
        """
        try:
            import math
            from collections import Counter
            
            with open(file_path, 'rb') as f:
                data = f.read(8192)
            
            if len(data) < 100:
                return False
            
            # Calculate Shannon entropy
            counter = Counter(data)
            entropy = -sum((count / len(data)) * math.log2(count / len(data)) 
                          for count in counter.values())
            
            # High entropy suggests encryption or compression
            # Threshold of 7.5 bits per byte
            return entropy > 7.5
            
        except Exception:
            return False
    
    def detect(self, file_path: Path, mime_type: str) -> FileTypeInfo:
        """
        Comprehensive file type detection
        
        Args:
            file_path: Path to file
            mime_type: Pre-detected MIME type from FileIngestor
            
        Returns:
            FileTypeInfo with complete analysis
        """
        extension = file_path.suffix.lower()
        
        # Determine category from extension
        category = self.extension_to_category.get(extension, FileCategory.UNKNOWN)
        
        # Check if binary
        is_binary = self._is_binary_file(file_path)
        
        # Check encryption
        is_encrypted = self._detect_encryption(file_path)
        
        info = FileTypeInfo(
            category=category,
            mime_type=mime_type,
            extension=extension,
            is_binary=is_binary,
            is_encrypted=is_encrypted,
            confidence=0.9
        )
        
        # For text files, detect encoding and scripts
        if category in [FileCategory.PLAIN_TEXT, FileCategory.STRUCTURED_TEXT] and not is_binary:
            encoding, enc_confidence = self._detect_encoding(file_path)
            info.encoding = encoding
            info.confidence = enc_confidence
            
            if encoding:
                try:
                    # Read and analyze text
                    with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                        text_sample = f.read(5000)  # Read first 5KB
                    
                    # Normalize encoding
                    text_sample = self._normalize_encoding(text_sample)
                    
                    # Detect scripts
                    scripts, dominant_script = self._analyze_scripts(text_sample)
                    info.scripts = scripts
                    info.dominant_script = dominant_script
                    
                    info.metadata['text_sample'] = text_sample[:500]  # Store small sample
                    
                except Exception as e:
                    logger.debug(f"Script analysis failed: {e}")
        
        # Add category-specific metadata
        if category == FileCategory.IMAGE:
            info.metadata['requires_ocr'] = True
        elif category == FileCategory.PDF:
            info.metadata['requires_ocr'] = True
        elif category in [FileCategory.AUDIO, FileCategory.VIDEO]:
            info.metadata['requires_stt'] = True
        
        logger.debug(f"Detected: {file_path.name} → {category.value} ({mime_type})")
        
        return info
    
    def batch_detect(self, file_paths: List[Path], mime_types: List[str]) -> List[FileTypeInfo]:
        """
        Batch detection for multiple files
        
        Args:
            file_paths: List of file paths
            mime_types: Corresponding MIME types
            
        Returns:
            List of FileTypeInfo objects
        """
        results = []
        for file_path, mime_type in zip(file_paths, mime_types):
            info = self.detect(file_path, mime_type)
            results.append(info)
        return results


class TextNormalizer:
    """
    Text normalization utilities for consistent processing
    """
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Comprehensive text normalization
        """
        # Unicode normalization (NFC)
        text = unicodedata.normalize('NFC', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Normalize quotes
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        
        # Normalize dashes
        text = text.replace('—', '-').replace('–', '-')
        
        return text.strip()
    
    @staticmethod
    def normalize_line_endings(text: str) -> str:
        """Normalize line endings to \n"""
        return text.replace('\r\n', '\n').replace('\r', '\n')
    
    @staticmethod
    def remove_zero_width_chars(text: str) -> str:
        """Remove zero-width characters that can hide data"""
        zero_width_chars = [
            '\u200b',  # Zero width space
            '\u200c',  # Zero width non-joiner
            '\u200d',  # Zero width joiner
            '\ufeff',  # Zero width no-break space
        ]
        for char in zero_width_chars:
            text = text.replace(char, '')
        return text


def get_language_from_script(script: UnicodeScript) -> List[str]:
    """
    Map Unicode script to likely languages
    Returns list of ISO 639-1 language codes
    """
    script_to_languages = {
        UnicodeScript.DEVANAGARI: ['hi', 'mr', 'ne'],  # Hindi, Marathi, Nepali
        UnicodeScript.BENGALI: ['bn'],  # Bengali
        UnicodeScript.TELUGU: ['te'],  # Telugu
        UnicodeScript.TAMIL: ['ta'],  # Tamil
        UnicodeScript.GUJARATI: ['gu'],  # Gujarati
        UnicodeScript.KANNADA: ['kn'],  # Kannada
        UnicodeScript.MALAYALAM: ['ml'],  # Malayalam
        UnicodeScript.GURMUKHI: ['pa'],  # Punjabi
        UnicodeScript.ORIYA: ['or'],  # Odia
        UnicodeScript.ARABIC: ['ar', 'ur', 'fa'],  # Arabic, Urdu, Persian
        UnicodeScript.CYRILLIC: ['ru', 'uk', 'bg'],  # Russian, Ukrainian, Bulgarian
        UnicodeScript.HAN: ['zh', 'ja'],  # Chinese, Japanese
        UnicodeScript.HIRAGANA: ['ja'],  # Japanese
        UnicodeScript.KATAKANA: ['ja'],  # Japanese
        UnicodeScript.HANGUL: ['ko'],  # Korean
        UnicodeScript.THAI: ['th'],  # Thai
        UnicodeScript.HEBREW: ['he'],  # Hebrew
        UnicodeScript.GREEK: ['el'],  # Greek
        UnicodeScript.LATIN: ['en'],  # Default to English for Latin
    }
    
    return script_to_languages.get(script, ['en'])


if __name__ == "__main__":
    # Test file type detection
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python file_type_detector.py <file_path>")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)
    
    detector = FileTypeDetector()
    
    # Quick MIME detection (simplified for testing)
    mime_type = "text/plain"  # Would come from FileIngestor
    
    info = detector.detect(file_path, mime_type)
    
    print(f"\n{'='*60}")
    print(f"File Type Analysis: {file_path.name}")
    print(f"{'='*60}")
    print(f"Category: {info.category.value}")
    print(f"MIME Type: {info.mime_type}")
    print(f"Extension: {info.extension}")
    print(f"Is Binary: {info.is_binary}")
    print(f"Is Encrypted: {info.is_encrypted}")
    print(f"Confidence: {info.confidence:.2%}")
    
    if info.encoding:
        print(f"\nText Analysis:")
        print(f"  Encoding: {info.encoding}")
        print(f"  Detected Scripts: {[s.value for s in info.scripts]}")
        print(f"  Dominant Script: {info.dominant_script.value if info.dominant_script else 'None'}")
        
        if info.dominant_script:
            languages = get_language_from_script(info.dominant_script)
            print(f"  Likely Languages: {', '.join(languages)}")
    
    if info.metadata:
        print(f"\nMetadata:")
        for key, value in info.metadata.items():
            if key != 'text_sample':  # Don't print full sample
                print(f"  {key}: {value}")
