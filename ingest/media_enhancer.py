"""
Step 12: Media Enhancement Module
Integrates OCR (Optical Character Recognition) and STT (Speech-to-Text)
into the UFDR ingestion pipeline with automatic entity extraction

Features:
- OCR for images (Tesseract/TrOCR)
- STT for audio (Whisper)
- Entity extraction from transcripts
- Integration with canonical ingestion pipeline
- Progress tracking and reporting
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass, asdict
import re
import sqlite3

# Import media workers
try:
    from media.ocr_worker import OCRWorker
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("Warning: OCR worker not available")

try:
    from media.asr_worker import ASRWorker
    ASR_AVAILABLE = True
except ImportError:
    ASR_AVAILABLE = False
    print("Warning: ASR worker not available")

logger = logging.getLogger(__name__)


@dataclass
class MediaEnhancementResult:
    """Result from media enhancement processing"""
    case_id: str
    media_type: str  # 'image' or 'audio'
    file_path: str
    extracted_text: str
    confidence: float
    language: str
    entities: Dict[str, List[str]]  # Extracted entities
    processing_time: float
    sha256_hash: str
    metadata: Dict
    success: bool
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


class EntityExtractor:
    """Extract entities from text (names, phones, emails, etc.)"""
    
    def __init__(self):
        """Initialize entity extraction patterns"""
        # Regex patterns for entity extraction
        self.patterns = {
            'phone': [
                r'\+?\d{1,4}[\s-]?\(?\d{1,4}\)?[\s-]?\d{1,4}[\s-]?\d{1,9}',  # International
                r'\d{10}',  # 10 digit phone
                r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',  # US format
                r'\+91[\s-]?\d{10}',  # India format
            ],
            'email': [
                r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            ],
            'url': [
                r'https?://(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&/=]*)'
            ],
            'crypto_wallet': [
                r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b',  # Bitcoin
                r'\b0x[a-fA-F0-9]{40}\b',  # Ethereum
            ],
            'date': [
                r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # MM/DD/YYYY
                r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',  # YYYY-MM-DD
            ],
            'time': [
                r'\d{1,2}:\d{2}(?::\d{2})?(?:\s?[AP]M)?',  # HH:MM or HH:MM:SS AM/PM
            ],
            'money': [
                r'[$₹€£]\s?\d+(?:,\d{3})*(?:\.\d{2})?',  # Currency symbols
                r'\d+(?:,\d{3})*(?:\.\d{2})?\s?(?:USD|INR|EUR|GBP)',  # Currency codes
            ],
            'location': [
                r'\d+\s+[A-Za-z\s,]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Circle|Cir|Way)',
            ],
            'coordinates': [
                r'[-+]?\d{1,3}\.\d+[,\s]+[-+]?\d{1,3}\.\d+',  # Lat,Lon decimal
            ]
        }
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract entities from text
        
        Args:
            text: Text to extract entities from
            
        Returns:
            Dictionary of entity types and their values
        """
        entities = {}
        
        for entity_type, patterns in self.patterns.items():
            matches = []
            for pattern in patterns:
                found = re.findall(pattern, text, re.IGNORECASE)
                matches.extend(found)
            
            # Remove duplicates and sort
            if matches:
                entities[entity_type] = sorted(list(set(matches)))
        
        # Extract potential names (words starting with capital letters)
        potential_names = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b', text)
        if potential_names:
            entities['potential_names'] = sorted(list(set(potential_names)))
        
        return entities


class MediaEnhancer:
    """
    Media Enhancement Module for UFDR Pipeline
    Processes images (OCR) and audio (STT) with entity extraction
    """
    
    def __init__(
        self,
        ocr_enabled: bool = True,
        stt_enabled: bool = True,
        db_path: str = "data/ufdr_analysis.db",
        output_dir: str = "data/media_enhancements"
    ):
        """
        Initialize Media Enhancer
        
        Args:
            ocr_enabled: Enable OCR processing
            stt_enabled: Enable STT processing
            db_path: Path to database for storing results
            output_dir: Directory for output files
        """
        self.ocr_enabled = ocr_enabled and OCR_AVAILABLE
        self.stt_enabled = stt_enabled and ASR_AVAILABLE
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize workers
        self.ocr_worker = None
        self.stt_worker = None
        self.entity_extractor = EntityExtractor()
        
        if self.ocr_enabled:
            try:
                self.ocr_worker = OCRWorker(
                    output_dir=str(self.output_dir / "ocr"),
                    use_gpu=False  # Set to True if GPU available
                )
                logger.info("✅ OCR worker initialized")
            except Exception as e:
                logger.warning(f"OCR worker initialization failed: {e}")
                self.ocr_enabled = False
        
        if self.stt_enabled:
            try:
                self.stt_worker = ASRWorker(
                    output_dir=str(self.output_dir / "asr"),
                    model_size="base",  # Options: tiny, base, small, medium, large
                    device="auto"
                )
                logger.info("✅ STT worker initialized")
            except Exception as e:
                logger.warning(f"STT worker initialization failed: {e}")
                self.stt_enabled = False
        
        # Initialize database
        self._init_database()
        
        logger.info(f"Media Enhancer initialized (OCR: {self.ocr_enabled}, STT: {self.stt_enabled})")
    
    def _init_database(self):
        """Initialize database tables for media enhancements"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create media_enhancements table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_enhancements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                extracted_text TEXT,
                confidence REAL,
                language TEXT,
                processing_time REAL,
                sha256_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(case_id, sha256_hash)
            )
        """)
        
        # Create extracted_entities table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS extracted_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enhancement_id INTEGER NOT NULL,
                entity_type TEXT NOT NULL,
                entity_value TEXT NOT NULL,
                case_id TEXT NOT NULL,
                FOREIGN KEY (enhancement_id) REFERENCES media_enhancements(id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enhancements_case ON media_enhancements(case_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_case ON extracted_entities(case_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON extracted_entities(entity_type)")
        
        conn.commit()
        conn.close()
        
        logger.info("✅ Database tables initialized for media enhancements")
    
    def process_image(
        self,
        image_path: Path,
        case_id: str
    ) -> MediaEnhancementResult:
        """
        Process single image with OCR
        
        Args:
            image_path: Path to image file
            case_id: Case identifier
            
        Returns:
            MediaEnhancementResult
        """
        if not self.ocr_enabled or not self.ocr_worker:
            return MediaEnhancementResult(
                case_id=case_id,
                media_type='image',
                file_path=str(image_path),
                extracted_text='',
                confidence=0.0,
                language='',
                entities={},
                processing_time=0.0,
                sha256_hash='',
                metadata={},
                success=False,
                error="OCR not available"
            )
        
        try:
            # Run OCR
            ocr_result = self.ocr_worker.process_image(str(image_path), case_id)
            
            # Extract entities from text
            entities = self.entity_extractor.extract_entities(ocr_result.text)
            
            # Create enhancement result
            result = MediaEnhancementResult(
                case_id=case_id,
                media_type='image',
                file_path=str(image_path),
                extracted_text=ocr_result.text,
                confidence=ocr_result.confidence,
                language=ocr_result.language,
                entities=entities,
                processing_time=ocr_result.processing_time,
                sha256_hash=ocr_result.sha256_hash,
                metadata=ocr_result.metadata,
                success=True
            )
            
            # Store in database
            self._store_result(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to process image {image_path}: {e}")
            return MediaEnhancementResult(
                case_id=case_id,
                media_type='image',
                file_path=str(image_path),
                extracted_text='',
                confidence=0.0,
                language='',
                entities={},
                processing_time=0.0,
                sha256_hash='',
                metadata={},
                success=False,
                error=str(e)
            )
    
    def process_audio(
        self,
        audio_path: Path,
        case_id: str
    ) -> MediaEnhancementResult:
        """
        Process single audio file with STT
        
        Args:
            audio_path: Path to audio file
            case_id: Case identifier
            
        Returns:
            MediaEnhancementResult
        """
        if not self.stt_enabled or not self.stt_worker:
            return MediaEnhancementResult(
                case_id=case_id,
                media_type='audio',
                file_path=str(audio_path),
                extracted_text='',
                confidence=0.0,
                language='',
                entities={},
                processing_time=0.0,
                sha256_hash='',
                metadata={},
                success=False,
                error="STT not available"
            )
        
        try:
            # Run STT
            stt_result = self.stt_worker.process_audio(str(audio_path), case_id)
            
            # Extract entities from transcript
            entities = self.entity_extractor.extract_entities(stt_result.transcript)
            
            # Create enhancement result
            result = MediaEnhancementResult(
                case_id=case_id,
                media_type='audio',
                file_path=str(audio_path),
                extracted_text=stt_result.transcript,
                confidence=stt_result.confidence,
                language=stt_result.language,
                entities=entities,
                processing_time=stt_result.processing_time,
                sha256_hash=stt_result.sha256_hash,
                metadata={
                    **stt_result.metadata,
                    'segments': stt_result.segments
                },
                success=True
            )
            
            # Store in database
            self._store_result(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to process audio {audio_path}: {e}")
            return MediaEnhancementResult(
                case_id=case_id,
                media_type='audio',
                file_path=str(audio_path),
                extracted_text='',
                confidence=0.0,
                language='',
                entities={},
                processing_time=0.0,
                sha256_hash='',
                metadata={},
                success=False,
                error=str(e)
            )
    
    def process_media_directory(
        self,
        media_dir: Path,
        case_id: str,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> List[MediaEnhancementResult]:
        """
        Process all media files in a directory
        
        Args:
            media_dir: Directory containing media files
            case_id: Case identifier
            progress_callback: Optional callback for progress updates
            
        Returns:
            List of MediaEnhancementResults
        """
        results = []
        
        # Find all media files
        image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.gif'}
        audio_extensions = {'.wav', '.mp3', '.m4a', '.flac', '.ogg', '.aac', '.wma', '.amr'}
        
        media_files = []
        
        if self.ocr_enabled:
            for ext in image_extensions:
                media_files.extend(list(media_dir.glob(f'**/*{ext}')))
                media_files.extend(list(media_dir.glob(f'**/*{ext.upper()}')))
        
        if self.stt_enabled:
            for ext in audio_extensions:
                media_files.extend(list(media_dir.glob(f'**/*{ext}')))
                media_files.extend(list(media_dir.glob(f'**/*{ext.upper()}')))
        
        total_files = len(media_files)
        logger.info(f"Found {total_files} media files to process in {media_dir}")
        
        if progress_callback:
            progress_callback(0, total_files, f"Starting media enhancement for {total_files} files")
        
        # Process each file
        for idx, file_path in enumerate(media_files, 1):
            ext = file_path.suffix.lower()
            
            if ext in image_extensions:
                result = self.process_image(file_path, case_id)
            elif ext in audio_extensions:
                result = self.process_audio(file_path, case_id)
            else:
                continue
            
            results.append(result)
            
            if progress_callback:
                progress_callback(
                    idx,
                    total_files,
                    f"Processed {file_path.name} ({result.media_type})"
                )
            
            if result.success:
                logger.info(f"✅ Processed {file_path.name}: {len(result.extracted_text)} chars extracted")
            else:
                logger.warning(f"⚠️ Failed to process {file_path.name}: {result.error}")
        
        return results
    
    def _store_result(self, result: MediaEnhancementResult):
        """Store enhancement result in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Insert media enhancement
            cursor.execute("""
                INSERT OR REPLACE INTO media_enhancements 
                (case_id, media_type, file_path, extracted_text, confidence, 
                 language, processing_time, sha256_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.case_id,
                result.media_type,
                result.file_path,
                result.extracted_text,
                result.confidence,
                result.language,
                result.processing_time,
                result.sha256_hash
            ))
            
            enhancement_id = cursor.lastrowid
            
            # Insert extracted entities
            for entity_type, entity_values in result.entities.items():
                for entity_value in entity_values:
                    cursor.execute("""
                        INSERT INTO extracted_entities 
                        (enhancement_id, entity_type, entity_value, case_id)
                        VALUES (?, ?, ?, ?)
                    """, (enhancement_id, entity_type, entity_value, result.case_id))
            
            conn.commit()
            logger.debug(f"Stored enhancement result for {result.file_path}")
            
        except Exception as e:
            logger.error(f"Failed to store result in database: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_enhancement_stats(self, case_id: str) -> Dict:
        """
        Get statistics about media enhancements for a case
        
        Args:
            case_id: Case identifier
            
        Returns:
            Dictionary with statistics
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Count by media type
        cursor.execute("""
            SELECT media_type, COUNT(*), AVG(confidence), SUM(LENGTH(extracted_text))
            FROM media_enhancements
            WHERE case_id = ?
            GROUP BY media_type
        """, (case_id,))
        
        stats = {
            'case_id': case_id,
            'by_media_type': {},
            'total_files': 0,
            'total_text_extracted': 0,
            'avg_confidence': 0.0
        }
        
        for row in cursor.fetchall():
            media_type, count, avg_conf, total_text = row
            stats['by_media_type'][media_type] = {
                'count': count,
                'avg_confidence': avg_conf or 0.0,
                'total_text_length': total_text or 0
            }
            stats['total_files'] += count
            stats['total_text_extracted'] += (total_text or 0)
        
        # Count entities by type
        cursor.execute("""
            SELECT entity_type, COUNT(DISTINCT entity_value)
            FROM extracted_entities
            WHERE case_id = ?
            GROUP BY entity_type
        """, (case_id,))
        
        stats['entities_by_type'] = dict(cursor.fetchall())
        stats['total_entities'] = sum(stats['entities_by_type'].values())
        
        conn.close()
        return stats
    
    def search_entities(
        self,
        case_id: str,
        entity_type: Optional[str] = None,
        entity_value: Optional[str] = None
    ) -> List[Dict]:
        """
        Search for extracted entities
        
        Args:
            case_id: Case identifier
            entity_type: Optional entity type filter
            entity_value: Optional entity value filter (partial match)
            
        Returns:
            List of matching entities with their source files
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = """
            SELECT e.entity_type, e.entity_value, m.file_path, m.media_type, m.confidence
            FROM extracted_entities e
            JOIN media_enhancements m ON e.enhancement_id = m.id
            WHERE e.case_id = ?
        """
        params = [case_id]
        
        if entity_type:
            query += " AND e.entity_type = ?"
            params.append(entity_type)
        
        if entity_value:
            query += " AND e.entity_value LIKE ?"
            params.append(f"%{entity_value}%")
        
        query += " ORDER BY e.entity_type, e.entity_value"
        
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'entity_type': row[0],
                'entity_value': row[1],
                'source_file': row[2],
                'media_type': row[3],
                'confidence': row[4]
            })
        
        conn.close()
        return results


def get_media_enhancer(db_path: str = "data/ufdr_analysis.db") -> MediaEnhancer:
    """
    Get singleton instance of MediaEnhancer
    
    Args:
        db_path: Path to database
        
    Returns:
        MediaEnhancer instance
    """
    global _media_enhancer_instance
    if '_media_enhancer_instance' not in globals():
        _media_enhancer_instance = MediaEnhancer(db_path=db_path)
    return _media_enhancer_instance


# Singleton instance
_media_enhancer_instance = None


def main():
    """Test media enhancer"""
    print("=" * 70)
    print("STEP 12: MEDIA ENHANCEMENT MODULE TEST")
    print("=" * 70)
    
    # Initialize enhancer
    enhancer = MediaEnhancer(
        ocr_enabled=True,
        stt_enabled=True,
        db_path="data/ufdr_analysis.db"
    )
    
    print(f"\n✅ Media Enhancer initialized")
    print(f"   OCR enabled: {enhancer.ocr_enabled}")
    print(f"   STT enabled: {enhancer.stt_enabled}")
    
    # Test entity extraction
    print(f"\n📝 Testing Entity Extraction:")
    test_text = """
    Contact: John Doe at john.doe@example.com
    Phone: +91-9876543210
    Bitcoin wallet: 1A2B3C4D5E6F7G8H9I0J1K2L3M4N5O6P7Q8R9S0
    Meeting at 123 Main Street, New York
    Time: 14:30
    Amount: $5,000.00
    """
    
    extractor = EntityExtractor()
    entities = extractor.extract_entities(test_text)
    
    for entity_type, values in entities.items():
        print(f"   {entity_type}: {values}")
    
    print(f"\n{'=' * 70}")
    print(f"✅ Step 12 module created successfully!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
