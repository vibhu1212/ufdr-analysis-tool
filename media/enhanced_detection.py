"""
Enhanced Media Detection Pipeline
Integrates YOLOv8, face detection, and advanced media processing

Features:
- YOLOv8 object detection (weapons, vehicles, people)
- Face detection and recognition
- Perceptual hash calculation
- Tag generation with confidence scores
- Batch processing support
"""

import os
import cv2
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    """Object detection result"""
    class_name: str
    confidence: float
    bbox: Tuple[int, int, int, int]  # x1, y1, x2, y2
    track_id: Optional[int] = None


@dataclass
class FaceDetection:
    """Face detection result"""
    bbox: Tuple[int, int, int, int]
    confidence: float
    embedding: Optional[List[float]] = None
    face_id: Optional[str] = None
    landmarks: Optional[Dict] = None


@dataclass
class MediaAnalysis:
    """Complete media analysis result"""
    media_id: str
    media_path: str
    media_type: str  # image, video
    objects: List[DetectionResult] = field(default_factory=list)
    faces: List[FaceDetection] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    perceptual_hash: Optional[str] = None
    ocr_text: Optional[str] = None
    analyzed_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'media_id': self.media_id,
            'media_path': self.media_path,
            'media_type': self.media_type,
            'objects': [
                {
                    'class': obj.class_name,
                    'confidence': obj.confidence,
                    'bbox': obj.bbox
                }
                for obj in self.objects
            ],
            'faces': [
                {
                    'bbox': face.bbox,
                    'confidence': face.confidence,
                    'face_id': face.face_id
                }
                for face in self.faces
            ],
            'tags': self.tags,
            'perceptual_hash': self.perceptual_hash,
            'ocr_text': self.ocr_text,
            'analyzed_at': self.analyzed_at.isoformat()
        }


class ObjectDetector:
    """
    YOLOv8-based object detector
    Detects weapons, vehicles, people, and other objects
    """
    
    # Forensically relevant object classes
    WEAPON_CLASSES = ['knife', 'gun', 'rifle', 'pistol', 'weapon']
    VEHICLE_CLASSES = ['car', 'truck', 'motorcycle', 'bus', 'van']
    PERSON_CLASSES = ['person', 'people']
    SUSPICIOUS_CLASSES = ['backpack', 'suitcase', 'handbag']
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize object detector
        
        Args:
            model_path: Path to YOLOv8 model (defaults to yolov8n.pt)
        """
        self.model_path = model_path or "yolov8n.pt"
        self.model = None
        self._load_model()
        
        logger.info(f"Object detector initialized with model: {self.model_path}")
    
    def _load_model(self):
        """Load YOLOv8 model"""
        try:
            from ultralytics import YOLO
            
            if os.path.exists(self.model_path):
                self.model = YOLO(self.model_path)
                logger.info("YOLOv8 model loaded successfully")
            else:
                logger.warning(f"Model not found: {self.model_path}")
                logger.info("Using mock detector for development")
                self.model = None
        except ImportError:
            logger.warning("ultralytics not installed, using mock detector")
            self.model = None
    
    def detect(self, 
               image_path: str, 
               confidence_threshold: float = 0.5) -> List[DetectionResult]:
        """
        Detect objects in an image
        
        Args:
            image_path: Path to image file
            confidence_threshold: Minimum confidence for detection
            
        Returns:
            List of detection results
        """
        if self.model is None:
            # Mock detections for development
            return self._mock_detect(image_path)
        
        try:
            # Run YOLOv8 detection
            results = self.model(image_path, conf=confidence_threshold)
            
            detections = []
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    conf = float(box.conf[0])
                    cls = int(box.cls[0])
                    class_name = result.names[cls]
                    
                    detection = DetectionResult(
                        class_name=class_name,
                        confidence=conf,
                        bbox=(int(x1), int(y1), int(x2), int(y2))
                    )
                    detections.append(detection)
            
            return detections
        
        except Exception as e:
            logger.error(f"Detection failed: {e}")
            return []
    
    def _mock_detect(self, image_path: str) -> List[DetectionResult]:
        """Mock detector for development"""
        # Return some mock detections
        return [
            DetectionResult(
                class_name='person',
                confidence=0.92,
                bbox=(100, 100, 300, 400)
            )
        ]
    
    def is_weapon_detected(self, detections: List[DetectionResult]) -> bool:
        """Check if any weapon was detected"""
        return any(
            d.class_name.lower() in self.WEAPON_CLASSES 
            for d in detections
        )
    
    def get_forensic_tags(self, detections: List[DetectionResult]) -> List[str]:
        """
        Generate forensic tags from detections
        
        Args:
            detections: List of detections
            
        Returns:
            List of forensic-relevant tags
        """
        tags = set()
        
        for detection in detections:
            class_lower = detection.class_name.lower()
            
            # Weapons
            if class_lower in self.WEAPON_CLASSES:
                tags.add('weapon_detected')
                tags.add(f'weapon_{class_lower}')
            
            # Vehicles
            if class_lower in self.VEHICLE_CLASSES:
                tags.add('vehicle_detected')
                tags.add(f'vehicle_{class_lower}')
            
            # People
            if class_lower in self.PERSON_CLASSES:
                tags.add('person_detected')
            
            # Suspicious items
            if class_lower in self.SUSPICIOUS_CLASSES:
                tags.add('suspicious_item')
                tags.add(f'item_{class_lower}')
        
        return sorted(list(tags))


class FaceDetector:
    """
    Face detection and recognition
    Uses dlib or similar face detection library
    """
    
    def __init__(self):
        """Initialize face detector"""
        self.detector = None
        self._load_detector()
        
        logger.info("Face detector initialized")
    
    def _load_detector(self):
        """Load face detection model"""
        try:
            import dlib
            self.detector = dlib.get_frontal_face_detector()
            logger.info("dlib face detector loaded")
        except ImportError:
            logger.warning("dlib not installed, using mock face detector")
            self.detector = None
    
    def detect_faces(self, image_path: str) -> List[FaceDetection]:
        """
        Detect faces in an image
        
        Args:
            image_path: Path to image file
            
        Returns:
            List of face detections
        """
        if self.detector is None:
            return self._mock_detect_faces()
        
        try:
            # Read image
            img = cv2.imread(image_path)
            if img is None:
                return []
            
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Detect faces
            faces = self.detector(gray, 1)
            
            detections = []
            for face in faces:
                detection = FaceDetection(
                    bbox=(face.left(), face.top(), face.right(), face.bottom()),
                    confidence=0.9,  # dlib doesn't provide confidence
                    face_id=None  # Will be assigned during matching
                )
                detections.append(detection)
            
            return detections
        
        except Exception as e:
            logger.error(f"Face detection failed: {e}")
            return []
    
    def _mock_detect_faces(self) -> List[FaceDetection]:
        """Mock face detector for development"""
        return [
            FaceDetection(
                bbox=(150, 150, 250, 250),
                confidence=0.95
            )
        ]
    
    def extract_embedding(self, 
                         image_path: str, 
                         face_bbox: Tuple[int, int, int, int]) -> Optional[List[float]]:
        """
        Extract face embedding for recognition
        
        Args:
            image_path: Path to image
            face_bbox: Face bounding box
            
        Returns:
            Face embedding vector
        """
        # Placeholder - implement with face recognition library
        # For now, return a mock embedding
        return [0.1] * 128  # 128-dimensional mock embedding


class PerceptualHasher:
    """
    Perceptual hashing for image similarity
    """
    
    @staticmethod
    def compute_phash(image_path: str, hash_size: int = 8) -> Optional[str]:
        """
        Compute perceptual hash (pHash)
        
        Args:
            image_path: Path to image
            hash_size: Hash size (default 8x8)
            
        Returns:
            Hexadecimal hash string
        """
        try:
            # Try using imagehash library
            from PIL import Image
            import imagehash
            
            img = Image.open(image_path)
            phash = imagehash.phash(img, hash_size=hash_size)
            return str(phash)
        
        except ImportError:
            # Fallback to manual implementation
            return PerceptualHasher._manual_phash(image_path, hash_size)
        except Exception as e:
            logger.error(f"pHash computation failed: {e}")
            return None
    
    @staticmethod
    def _manual_phash(image_path: str, hash_size: int = 8) -> Optional[str]:
        """Manual pHash implementation using OpenCV"""
        try:
            # Read image
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            # Resize to hash_size + 1
            img = cv2.resize(img, (hash_size + 1, hash_size))
            
            # Compute DCT
            dct = cv2.dct(np.float32(img))
            
            # Keep top-left corner
            dct_low = dct[:hash_size, :hash_size]
            
            # Compute median
            median = np.median(dct_low)
            
            # Create hash
            hash_bits = (dct_low > median).flatten()
            
            # Convert to hex string
            hash_str = ''.join('1' if b else '0' for b in hash_bits)
            hash_hex = hex(int(hash_str, 2))[2:].zfill(hash_size * hash_size // 4)
            
            return hash_hex
        
        except Exception as e:
            logger.error(f"Manual pHash failed: {e}")
            return None


class MediaAnalyzer:
    """
    Complete media analysis pipeline
    Combines object detection, face detection, and hashing
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """Initialize media analyzer"""
        self.object_detector = ObjectDetector(model_path)
        self.face_detector = FaceDetector()
        self.hasher = PerceptualHasher()
        
        logger.info("Media analyzer initialized")
    
    def analyze_image(self, 
                     image_path: str, 
                     media_id: str) -> MediaAnalysis:
        """
        Complete analysis of an image
        
        Args:
            image_path: Path to image file
            media_id: Unique media identifier
            
        Returns:
            MediaAnalysis result
        """
        logger.info(f"Analyzing image: {image_path}")
        
        # Object detection
        objects = self.object_detector.detect(image_path)
        logger.info(f"Detected {len(objects)} objects")
        
        # Face detection
        faces = self.face_detector.detect_faces(image_path)
        logger.info(f"Detected {len(faces)} faces")
        
        # Generate tags
        tags = self.object_detector.get_forensic_tags(objects)
        if faces:
            tags.append('face_detected')
        
        # Compute perceptual hash
        phash = self.hasher.compute_phash(image_path)
        
        # Create analysis result
        analysis = MediaAnalysis(
            media_id=media_id,
            media_path=image_path,
            media_type='image',
            objects=objects,
            faces=faces,
            tags=tags,
            perceptual_hash=phash
        )
        
        return analysis
    
    def analyze_video(self, 
                     video_path: str, 
                     media_id: str,
                     sample_interval: int = 30) -> MediaAnalysis:
        """
        Analyze video by sampling frames
        
        Args:
            video_path: Path to video file
            media_id: Unique media identifier
            sample_interval: Frame sampling interval
            
        Returns:
            MediaAnalysis result
        """
        logger.info(f"Analyzing video: {video_path}")
        
        all_objects = []
        all_faces = []
        
        try:
            # Open video
            cap = cv2.VideoCapture(video_path)
            frame_count = 0
            
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Sample frames
                if frame_count % sample_interval == 0:
                    # Save temp frame
                    temp_frame = f"temp_frame_{frame_count}.jpg"
                    cv2.imwrite(temp_frame, frame)
                    
                    # Analyze frame
                    objects = self.object_detector.detect(temp_frame)
                    faces = self.face_detector.detect_faces(temp_frame)
                    
                    all_objects.extend(objects)
                    all_faces.extend(faces)
                    
                    # Clean up
                    os.remove(temp_frame)
                
                frame_count += 1
            
            cap.release()
            
            # Deduplicate and aggregate
            unique_tags = set()
            for obj in all_objects:
                unique_tags.update(self.object_detector.get_forensic_tags([obj]))
            
            if all_faces:
                unique_tags.add('face_detected')
            
            analysis = MediaAnalysis(
                media_id=media_id,
                media_path=video_path,
                media_type='video',
                objects=all_objects[:100],  # Limit to first 100
                faces=all_faces[:50],  # Limit to first 50
                tags=sorted(list(unique_tags))
            )
            
            return analysis
        
        except Exception as e:
            logger.error(f"Video analysis failed: {e}")
            return MediaAnalysis(
                media_id=media_id,
                media_path=video_path,
                media_type='video'
            )
    
    def batch_analyze(self, 
                     media_files: List[Tuple[str, str]]) -> List[MediaAnalysis]:
        """
        Batch analyze multiple media files
        
        Args:
            media_files: List of (media_id, file_path) tuples
            
        Returns:
            List of analysis results
        """
        results = []
        
        for media_id, file_path in media_files:
            # Determine media type
            ext = Path(file_path).suffix.lower()
            
            if ext in ['.jpg', '.jpeg', '.png', '.bmp']:
                analysis = self.analyze_image(file_path, media_id)
            elif ext in ['.mp4', '.avi', '.mov', '.mkv']:
                analysis = self.analyze_video(file_path, media_id)
            else:
                logger.warning(f"Unsupported file type: {ext}")
                continue
            
            results.append(analysis)
        
        return results
    
    def export_results(self, 
                      results: List[MediaAnalysis], 
                      output_file: str):
        """
        Export analysis results to JSON
        
        Args:
            results: List of analysis results
            output_file: Output file path
        """
        data = {
            'analysis_count': len(results),
            'results': [r.to_dict() for r in results],
            'export_time': datetime.now().isoformat()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(results)} analysis results to {output_file}")


# Example usage
if __name__ == "__main__":
    # Initialize analyzer
    analyzer = MediaAnalyzer()
    
    # Analyze single image
    if os.path.exists("test_image.jpg"):
        result = analyzer.analyze_image("test_image.jpg", "IMG_001")
        
        print(f"\nAnalysis Results:")
        print(f"Objects detected: {len(result.objects)}")
        for obj in result.objects:
            print(f"  - {obj.class_name}: {obj.confidence:.2f}")
        
        print(f"\nFaces detected: {len(result.faces)}")
        print(f"Tags: {', '.join(result.tags)}")
        print(f"pHash: {result.perceptual_hash}")
        
        # Export
        analyzer.export_results([result], "analysis_results.json")
    else:
        print("No test image found. Analyzer is ready for use.")
        print("\nUsage:")
        print("  analyzer = MediaAnalyzer()")
        print("  result = analyzer.analyze_image('path/to/image.jpg', 'media_id')")
        print("  print(result.tags)")