"""
Face Recognition Module
Detect faces, generate embeddings, and perform cross-case matching
Uses DeepFace for recognition and OpenCV for fast detection
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
import numpy as np
import cv2
import importlib.util

# Lazy import to avoid loading TensorFlow at module import time
DEEPFACE_AVAILABLE = None
DeepFace = None

def _ensure_deepface():
    """Lazy load DeepFace only when needed"""
    global DEEPFACE_AVAILABLE, DeepFace
    if DEEPFACE_AVAILABLE is None:
        try:
            from deepface import DeepFace as _DeepFace
            DeepFace = _DeepFace
            DEEPFACE_AVAILABLE = True
        except ImportError:
            DEEPFACE_AVAILABLE = False
    return DEEPFACE_AVAILABLE

logger = logging.getLogger(__name__)

# Singleton instance
_face_recognizer_instance = None


def is_deepface_available() -> bool:
    """Check if DeepFace is available without loading it"""
    global DEEPFACE_AVAILABLE
    if DEEPFACE_AVAILABLE is None:
        try:
            spec = importlib.util.find_spec("deepface")
            DEEPFACE_AVAILABLE = spec is not None
        except (ImportError, ValueError, AttributeError):
            DEEPFACE_AVAILABLE = False
    return DEEPFACE_AVAILABLE


class FaceRecognizer:
    """
    Face recognition for forensic analysis
    - Detect faces in images
    - Generate face embeddings
    - Match faces across cases
    - Build person identity database
    """
    
    def __init__(
        self,
        model_name: str = "Facenet512",  # Best balance of speed and accuracy
        detector_backend: str = "opencv",  # Fast detection
        device: Optional[str] = None
    ):
        """
        Initialize face recognizer
        
        Args:
            model_name: Recognition model (Facenet512, ArcFace, VGG-Face, etc.)
            detector_backend: Face detector (opencv, ssd, dlib, mtcnn, retinaface)
            device: Device ('cuda' or 'cpu')
        """
        if not _ensure_deepface():
            raise ImportError("DeepFace not installed. Run: pip install deepface")
        
        self.model_name = model_name
        self.detector_backend = detector_backend
        self.device = device or "cpu"  # DeepFace handles GPU automatically
        
        # Face detection cascade (for fast pre-filtering)
        self.face_cascade = cv2.CascadeClassifier(
            cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'
        )
        
        logger.info(f"Face Recognizer initialized (model={model_name}, detector={detector_backend})")
    
    def detect_faces(
        self,
        image_path: Union[str, Path],
        min_confidence: float = 0.9
    ) -> List[Dict[str, any]]:
        """
        Detect all faces in an image
        
        Args:
            image_path: Path to image file
            min_confidence: Minimum detection confidence
        
        Returns:
            List of face detections with bounding boxes and metadata
        """
        try:
            # Use DeepFace for detection
            faces = DeepFace.extract_faces(
                img_path=str(image_path),
                detector_backend=self.detector_backend,
                enforce_detection=False,
                align=True
            )
            
            # Filter by confidence
            detections = []
            for i, face in enumerate(faces):
                if face.get('confidence', 0) >= min_confidence:
                    detections.append({
                        'face_id': f"face_{i}",
                        'bbox': face.get('facial_area', {}),
                        'confidence': face.get('confidence', 0),
                        'face_array': face.get('face', None)
                    })
            
            logger.debug(f"Detected {len(detections)} faces in {Path(image_path).name}")
            return detections
            
        except Exception as e:
            logger.error(f"Failed to detect faces in {image_path}: {e}")
            return []
    
    def detect_faces_fast(
        self,
        image_path: Union[str, Path]
    ) -> int:
        """
        Fast face counting using OpenCV (no embedding generation)
        
        Args:
            image_path: Path to image file
        
        Returns:
            Number of faces detected
        """
        try:
            # Load image
            img = cv2.imread(str(image_path))
            if img is None:
                return 0
            
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Detect faces
            faces = self.face_cascade.detectMultiScale(
                gray,
                scaleFactor=1.1,
                minNeighbors=5,
                minSize=(30, 30)
            )
            
            return len(faces)
            
        except Exception as e:
            logger.error(f"Fast face detection failed for {image_path}: {e}")
            return 0
    
    def generate_face_embedding(
        self,
        image_path: Union[str, Path],
        face_index: int = 0
    ) -> Optional[np.ndarray]:
        """
        Generate embedding for a specific face in an image
        
        Args:
            image_path: Path to image file
            face_index: Which face to extract (if multiple faces)
        
        Returns:
            Face embedding vector (512D for Facenet512)
        """
        try:
            # Generate embedding
            embedding_objs = DeepFace.represent(
                img_path=str(image_path),
                model_name=self.model_name,
                detector_backend=self.detector_backend,
                enforce_detection=False
            )
            
            if not embedding_objs or face_index >= len(embedding_objs):
                return None
            
            embedding = np.array(embedding_objs[face_index]['embedding'])
            
            logger.debug(f"Generated embedding for face {face_index} in {Path(image_path).name}")
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate embedding for {image_path}: {e}")
            return None
    
    def analyze_faces(
        self,
        image_path: Union[str, Path]
    ) -> List[Dict[str, any]]:
        """
        Comprehensive face analysis: detection + recognition + attributes
        
        Args:
            image_path: Path to image file
        
        Returns:
            List of face analysis results with embeddings and attributes
        """
        try:
            # Detect and analyze faces
            analyses = DeepFace.analyze(
                img_path=str(image_path),
                actions=['age', 'gender', 'emotion'],
                detector_backend=self.detector_backend,
                enforce_detection=False,
                silent=True
            )
            
            if not isinstance(analyses, list):
                analyses = [analyses]
            
            # Also get embeddings
            embeddings = DeepFace.represent(
                img_path=str(image_path),
                model_name=self.model_name,
                detector_backend=self.detector_backend,
                enforce_detection=False
            )
            
            # Combine analysis and embeddings
            results = []
            for i, (analysis, embedding_obj) in enumerate(zip(analyses, embeddings)):
                results.append({
                    'face_id': f"face_{i}",
                    'bbox': analysis.get('region', {}),
                    'embedding': np.array(embedding_obj['embedding']),
                    'age': analysis.get('age', None),
                    'gender': analysis.get('dominant_gender', 'unknown'),
                    'emotion': analysis.get('dominant_emotion', 'unknown'),
                    'confidence': analysis.get('face_confidence', 0)
                })
            
            logger.info(f"Analyzed {len(results)} faces in {Path(image_path).name}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to analyze faces in {image_path}: {e}")
            return []
    
    def verify_faces(
        self,
        img1_path: Union[str, Path],
        img2_path: Union[str, Path],
        threshold: Optional[float] = None
    ) -> Dict[str, any]:
        """
        Verify if two images contain the same person
        
        Args:
            img1_path: Path to first image
            img2_path: Path to second image
            threshold: Distance threshold for matching (None = use model default)
        
        Returns:
            Verification result with distance and decision
        """
        try:
            result = DeepFace.verify(
                img1_path=str(img1_path),
                img2_path=str(img2_path),
                model_name=self.model_name,
                detector_backend=self.detector_backend,
                enforce_detection=False,
                distance_metric='cosine'
            )
            
            return {
                'verified': result.get('verified', False),
                'distance': result.get('distance', 1.0),
                'threshold': result.get('threshold', 0.4),
                'similarity': 1.0 - result.get('distance', 1.0),
                'model': result.get('model', self.model_name)
            }
            
        except Exception as e:
            logger.error(f"Face verification failed: {e}")
            return {
                'verified': False,
                'distance': 1.0,
                'threshold': 0.4,
                'similarity': 0.0,
                'error': str(e)
            }
    
    def find_similar_faces(
        self,
        query_image: Union[str, Path],
        database_images: List[Union[str, Path]],
        threshold: float = 0.4,
        top_k: int = 10
    ) -> List[Dict[str, any]]:
        """
        Find similar faces in a database of images
        
        Args:
            query_image: Path to query image (reference face)
            database_images: List of image paths to search
            threshold: Maximum distance for a match
            top_k: Return top K matches
        
        Returns:
            List of matches sorted by similarity
        """
        # Get query embedding
        query_embedding = self.generate_face_embedding(query_image)
        if query_embedding is None:
            return []
        
        matches = []
        
        for db_image in database_images:
            try:
                result = self.verify_faces(query_image, db_image)
                
                if result['distance'] <= threshold:
                    matches.append({
                        'image_path': str(db_image),
                        'distance': result['distance'],
                        'similarity': result['similarity'],
                        'verified': result['verified']
                    })
                    
            except Exception as e:
                logger.warning(f"Failed to compare with {db_image}: {e}")
                continue
        
        # Sort by similarity (highest first)
        matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        logger.info(f"Found {len(matches)} matching faces (threshold={threshold})")
        
        return matches[:top_k]
    
    def batch_extract_faces(
        self,
        image_paths: List[Union[str, Path]]
    ) -> Dict[str, List[Dict[str, any]]]:
        """
        Extract faces from multiple images (for indexing)
        
        Args:
            image_paths: List of image paths to process
        
        Returns:
            Dictionary mapping image paths to face analysis results
        """
        results = {}
        
        logger.info(f"Extracting faces from {len(image_paths)} images...")
        
        for i, image_path in enumerate(image_paths):
            logger.debug(f"Processing {i+1}/{len(image_paths)}: {Path(image_path).name}")
            
            faces = self.analyze_faces(image_path)
            results[str(image_path)] = faces
        
        total_faces = sum(len(faces) for faces in results.values())
        logger.info(f"✅ Extracted {total_faces} faces from {len(image_paths)} images")
        
        return results


# Singleton instance
_face_recognizer = None

def get_face_recognizer(model_name: str = "Facenet512", lazy: bool = True) -> Optional[FaceRecognizer]:
    """Get or create face recognizer singleton
    
    Args:
        model_name: Recognition model to use
        lazy: If True, return None if DeepFace not available (don't raise error)
    
    Returns:
        FaceRecognizer instance or None if not available and lazy=True
    """
    global _face_recognizer
    
    # Check availability first without loading
    if not is_deepface_available():
        if lazy:
            logger.warning("DeepFace not available, face recognition disabled")
            return None
        else:
            raise ImportError("DeepFace not installed. Run: pip install deepface")
    
    if _face_recognizer is None:
        try:
            _face_recognizer = FaceRecognizer(model_name=model_name)
        except Exception as e:
            if lazy:
                logger.error(f"Failed to initialize FaceRecognizer: {e}")
                return None
            else:
                raise
    
    return _face_recognizer


# Convenience functions
def detect_faces(image_path: Union[str, Path]) -> List[Dict[str, any]]:
    """Detect faces in an image"""
    recognizer = get_face_recognizer()
    if recognizer is None:
        return []
    return recognizer.detect_faces(image_path)


def count_faces(image_path: Union[str, Path]) -> int:
    """Fast face counting"""
    recognizer = get_face_recognizer()
    if recognizer is None:
        return 0
    return recognizer.detect_faces_fast(image_path)


def find_person_in_images(
    reference_image: Union[str, Path],
    search_images: List[Union[str, Path]],
    threshold: float = 0.4
) -> List[Dict[str, any]]:
    """Find all images containing the same person as reference"""
    recognizer = get_face_recognizer()
    if recognizer is None:
        return []
    return recognizer.find_similar_faces(reference_image, search_images, threshold)
