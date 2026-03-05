"""
OCR Worker for Image Text Extraction
Processes images from UFDR archives using Tesseract OCR
"""

import logging
import hashlib
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from PIL import Image
import numpy as np
from dataclasses import dataclass, asdict

# Try to import OCR libraries
try:
    import pytesseract
    import cv2
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    print("Warning: pytesseract not installed. OCR features will be limited.")

# Defer TrOCR import to avoid TensorFlow conflicts
# Will be imported only when actually needed
TROCR_AVAILABLE = False
try:
    # Just check if transformers is available, don't import yet
    import importlib.util
    spec = importlib.util.find_spec("transformers")
    if spec is not None:
        TROCR_AVAILABLE = True
except ImportError:
    TROCR_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class OCRResult:
    """Result from OCR processing"""
    image_path: str
    text: str
    confidence: float
    language: str
    processing_time: float
    metadata: Dict
    sha256_hash: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


class OCRWorker:
    """OCR processing worker for forensic image analysis"""
    
    def __init__(self, 
                 output_dir: str = "data/ocr_output",
                 tesseract_path: Optional[str] = None,
                 use_gpu: bool = False):
        """
        Initialize OCR Worker
        
        Args:
            output_dir: Directory for OCR output
            tesseract_path: Path to tesseract executable
            use_gpu: Use GPU for processing if available
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure Tesseract
        if tesseract_path and TESSERACT_AVAILABLE:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        
        self.use_gpu = use_gpu
        self.processor = None
        self.model = None
        
        # Initialize TrOCR if available
        if TROCR_AVAILABLE and use_gpu:
            self._init_trocr()
        
        # Supported image formats
        self.supported_formats = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.gif'}
        
        # OCR configuration
        self.ocr_config = {
            'lang': 'eng+hin+ara',  # English, Hindi, Arabic
            'oem': 3,  # OCR Engine Mode (LSTM)
            'psm': 3   # Page segmentation mode (automatic)
        }
    
    def _init_trocr(self):
        """Initialize TrOCR model for advanced OCR"""
        try:
            # Lazy import to avoid TensorFlow conflicts
            import os
            os.environ['TRANSFORMERS_NO_TF'] = '1'
            from transformers import TrOCRProcessor, VisionEncoderDecoderModel
            
            self.processor = TrOCRProcessor.from_pretrained('microsoft/trocr-base-printed')
            self.model = VisionEncoderDecoderModel.from_pretrained('microsoft/trocr-base-printed')
            if self.use_gpu:
                import torch
                if torch.cuda.is_available():
                    self.model = self.model.cuda()
            logger.info("TrOCR model initialized")
        except Exception as e:
            logger.error(f"Failed to initialize TrOCR: {e}")
            self.processor = None
            self.model = None
    
    def preprocess_image(self, image_path: str) -> np.ndarray:
        """
        Preprocess image for better OCR results
        
        Args:
            image_path: Path to image file
            
        Returns:
            Preprocessed image as numpy array
        """
        if not TESSERACT_AVAILABLE:
            raise ImportError("OpenCV (cv2) is required for image preprocessing")
        
        # Read image
        img = cv2.imread(str(image_path))
        
        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Apply thresholding
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Denoise
        denoised = cv2.fastNlMeansDenoising(thresh)
        
        # Deskew image
        coords = np.column_stack(np.where(denoised > 0))
        angle = cv2.minAreaRect(coords)[-1]
        
        if angle < -45:
            angle = 90 + angle
        
        (h, w) = denoised.shape[:2]
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, angle, 1.0)
        rotated = cv2.warpAffine(denoised, M, (w, h),
                                 flags=cv2.INTER_CUBIC,
                                 borderMode=cv2.BORDER_REPLICATE)
        
        return rotated
    
    def extract_text_tesseract(self, image_path: str) -> Tuple[str, float]:
        """
        Extract text using Tesseract OCR
        
        Args:
            image_path: Path to image file
            
        Returns:
            Tuple of (extracted text, confidence score)
        """
        if not TESSERACT_AVAILABLE:
            return "OCR not available (pytesseract not installed)", 0.0
        
        try:
            # Preprocess image
            processed_img = self.preprocess_image(image_path)
            
            # Get OCR data with confidence scores
            data = pytesseract.image_to_data(
                processed_img,
                output_type=pytesseract.Output.DICT,
                config=self._get_tesseract_config()
            )
            
            # Extract text and calculate confidence
            text_parts = []
            confidences = []
            
            for i, conf in enumerate(data['conf']):
                if int(conf) > 0:  # Filter out empty results
                    text_parts.append(data['text'][i])
                    confidences.append(int(conf))
            
            text = ' '.join(text_parts)
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0
            
            return text.strip(), avg_confidence / 100.0
            
        except Exception as e:
            logger.error(f"Tesseract OCR failed: {e}")
            return "", 0.0
    
    def extract_text_trocr(self, image_path: str) -> Tuple[str, float]:
        """
        Extract text using TrOCR model
        
        Args:
            image_path: Path to image file
            
        Returns:
            Tuple of (extracted text, confidence score)
        """
        if not self.model or not self.processor:
            return "", 0.0
        
        try:
            from PIL import Image
            import torch
            
            # Load and preprocess image
            image = Image.open(image_path).convert("RGB")
            pixel_values = self.processor(images=image, return_tensors="pt").pixel_values
            
            if self.use_gpu and torch.cuda.is_available():
                pixel_values = pixel_values.cuda()
            
            # Generate text
            generated_ids = self.model.generate(pixel_values)
            text = self.processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
            
            # Estimate confidence (simplified)
            confidence = 0.9  # TrOCR usually has high confidence
            
            return text, confidence
            
        except Exception as e:
            logger.error(f"TrOCR failed: {e}")
            return "", 0.0
    
    def _get_tesseract_config(self) -> str:
        """Build Tesseract configuration string"""
        config_parts = []
        for key, value in self.ocr_config.items():
            if key == 'lang':
                config_parts.append(f'-l {value}')
            elif key == 'oem':
                config_parts.append(f'--oem {value}')
            elif key == 'psm':
                config_parts.append(f'--psm {value}')
        return ' '.join(config_parts)
    
    def process_image(self, image_path: str, case_id: str) -> OCRResult:
        """
        Process single image for OCR
        
        Args:
            image_path: Path to image file
            case_id: Case identifier
            
        Returns:
            OCRResult object
        """
        start_time = datetime.now()
        image_path = Path(image_path)
        
        # Calculate hash
        sha256_hash = self._calculate_hash(image_path)
        
        # Extract text using available methods
        text = ""
        confidence = 0.0
        
        # Try TrOCR first if available
        if TROCR_AVAILABLE and self.model:
            text, confidence = self.extract_text_trocr(image_path)
        
        # Fall back to Tesseract
        if not text and TESSERACT_AVAILABLE:
            text, confidence = self.extract_text_tesseract(image_path)
        
        # If no OCR available, return empty result
        if not text and not TESSERACT_AVAILABLE:
            text = "[OCR not available - install pytesseract]"
            confidence = 0.0
        
        # Extract metadata
        metadata = self._extract_image_metadata(image_path)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Create result
        result = OCRResult(
            image_path=str(image_path),
            text=text,
            confidence=confidence,
            language=self.ocr_config.get('lang', 'eng'),
            processing_time=processing_time,
            metadata=metadata,
            sha256_hash=sha256_hash
        )
        
        # Save result
        self._save_result(result, case_id)
        
        return result
    
    def process_directory(self, directory: str, case_id: str) -> List[OCRResult]:
        """
        Process all images in a directory
        
        Args:
            directory: Directory containing images
            case_id: Case identifier
            
        Returns:
            List of OCR results
        """
        directory = Path(directory)
        results = []
        
        # Find all image files
        image_files = []
        for ext in self.supported_formats:
            image_files.extend(directory.glob(f'**/*{ext}'))
            image_files.extend(directory.glob(f'**/*{ext.upper()}'))
        
        logger.info(f"Found {len(image_files)} images to process")
        
        # Process each image
        for image_path in image_files:
            try:
                result = self.process_image(image_path, case_id)
                results.append(result)
                logger.info(f"Processed {image_path.name}: {len(result.text)} chars extracted")
            except Exception as e:
                logger.error(f"Failed to process {image_path}: {e}")
        
        return results
    
    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _extract_image_metadata(self, image_path: Path) -> Dict:
        """Extract metadata from image"""
        metadata = {
            'filename': image_path.name,
            'size': image_path.stat().st_size,
            'modified': datetime.fromtimestamp(image_path.stat().st_mtime).isoformat()
        }
        
        try:
            with Image.open(image_path) as img:
                metadata['format'] = img.format
                metadata['mode'] = img.mode
                metadata['dimensions'] = img.size
                
                # Extract EXIF data if available
                if hasattr(img, '_getexif'):
                    exif = img._getexif()
                    if exif:
                        metadata['exif'] = {
                            'make': exif.get(271, 'Unknown'),
                            'model': exif.get(272, 'Unknown'),
                            'datetime': exif.get(306, 'Unknown'),
                            'gps': self._extract_gps(exif)
                        }
        except Exception as e:
            logger.debug(f"Could not extract image metadata: {e}")
        
        return metadata
    
    def _extract_gps(self, exif: Dict) -> Optional[Dict]:
        """Extract GPS coordinates from EXIF data"""
        gps_tags = {
            1: 'latitude_ref',
            2: 'latitude',
            3: 'longitude_ref',
            4: 'longitude',
            5: 'altitude_ref',
            6: 'altitude'
        }
        
        gps_data = {}
        gps_info = exif.get(34853)  # GPS IFD
        
        if not gps_info:
            return None
        
        for tag, name in gps_tags.items():
            if tag in gps_info:
                gps_data[name] = gps_info[tag]
        
        # Convert to decimal degrees if coordinates exist
        if 'latitude' in gps_data and 'longitude' in gps_data:
            lat = self._convert_to_degrees(gps_data['latitude'])
            if gps_data.get('latitude_ref') == 'S':
                lat = -lat
            
            lon = self._convert_to_degrees(gps_data['longitude'])
            if gps_data.get('longitude_ref') == 'W':
                lon = -lon
            
            return {'latitude': lat, 'longitude': lon}
        
        return None
    
    def _convert_to_degrees(self, value):
        """Convert GPS coordinates to decimal degrees"""
        d, m, s = value
        return d + (m / 60.0) + (s / 3600.0)
    
    def _save_result(self, result: OCRResult, case_id: str):
        """Save OCR result to file"""
        output_file = self.output_dir / f"{case_id}_ocr_results.jsonl"
        
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(result.to_dict(), ensure_ascii=False) + '\n')
    
    def search_text(self, case_id: str, query: str) -> List[Dict]:
        """
        Search for text in OCR results
        
        Args:
            case_id: Case identifier
            query: Search query
            
        Returns:
            List of matching results
        """
        results_file = self.output_dir / f"{case_id}_ocr_results.jsonl"
        
        if not results_file.exists():
            return []
        
        matches = []
        query_lower = query.lower()
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                if query_lower in result['text'].lower():
                    matches.append(result)
        
        return matches
    
    def generate_report(self, case_id: str) -> Dict:
        """
        Generate OCR analysis report
        
        Args:
            case_id: Case identifier
            
        Returns:
            Report dictionary
        """
        results_file = self.output_dir / f"{case_id}_ocr_results.jsonl"
        
        if not results_file.exists():
            return {'error': 'No OCR results found'}
        
        report = {
            'case_id': case_id,
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_images': 0,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'total_text_length': 0,
                'average_confidence': 0.0,
                'languages_detected': set()
            },
            'high_confidence_texts': [],
            'low_confidence_texts': [],
            'gps_locations': []
        }
        
        confidences = []
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                report['statistics']['total_images'] += 1
                
                if result['text']:
                    report['statistics']['successful_extractions'] += 1
                    report['statistics']['total_text_length'] += len(result['text'])
                    confidences.append(result['confidence'])
                    
                    if result['confidence'] > 0.8:
                        report['high_confidence_texts'].append({
                            'image': result['image_path'],
                            'text': result['text'][:200],
                            'confidence': result['confidence']
                        })
                    elif result['confidence'] < 0.5:
                        report['low_confidence_texts'].append({
                            'image': result['image_path'],
                            'confidence': result['confidence']
                        })
                else:
                    report['statistics']['failed_extractions'] += 1
                
                # Extract GPS locations
                if 'metadata' in result and 'exif' in result['metadata']:
                    gps = result['metadata']['exif'].get('gps')
                    if gps:
                        report['gps_locations'].append({
                            'image': result['image_path'],
                            'coordinates': gps
                        })
        
        if confidences:
            report['statistics']['average_confidence'] = sum(confidences) / len(confidences)
        
        return report


def main():
    """Test OCR worker"""
    print("OCR Worker Test")
    print("=" * 50)
    
    # Initialize worker
    worker = OCRWorker()
    
    # Test with a sample image (if exists)
    test_image = "data/samples/test_image.jpg"
    
    if Path(test_image).exists():
        print(f"\nProcessing: {test_image}")
        result = worker.process_image(test_image, "test_case")
        
        print(f"Text extracted: {result.text[:200]}...")
        print(f"Confidence: {result.confidence:.2%}")
        print(f"Processing time: {result.processing_time:.2f}s")
        print(f"SHA256: {result.sha256_hash}")
    else:
        print(f"\nTest image not found: {test_image}")
        print("Creating mock result...")
        
        # Create mock result for demonstration
        mock_result = OCRResult(
            image_path="mock_image.jpg",
            text="This is sample extracted text from a forensic image.",
            confidence=0.85,
            language="eng",
            processing_time=1.23,
            metadata={'dimensions': [1920, 1080]},
            sha256_hash="abc123def456"
        )
        
        print(f"Mock result: {mock_result.to_dict()}")
    
    print("\n" + "=" * 50)
    print("OCR Worker initialized successfully!")
    
    if not TESSERACT_AVAILABLE:
        print("\nNote: Install pytesseract and opencv-python for full OCR functionality:")
        print("  pip install pytesseract opencv-python")


if __name__ == "__main__":
    main()