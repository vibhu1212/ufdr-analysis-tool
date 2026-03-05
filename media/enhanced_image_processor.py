"""
Enhanced Image Processing Pipeline
Integrates YOLO-World, Face Recognition, CLIP, and metadata extraction
Complete forensic image analysis in one pipeline
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict, field
from datetime import datetime
import hashlib

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

import numpy as np

# Import our custom modules
from media.clip_embedder import get_clip_embedder
from media.yolo_detector import get_yolo_detector
from media.face_recognizer import get_face_recognizer
from media.blip2_analyzer import get_blip2_analyzer
from vector.multimodal_index import get_multimodal_index

logger = logging.getLogger(__name__)


@dataclass
class EnhancedImageResult:
    """
    Complete forensic image analysis result
    Combines YOLO, Face Recognition, CLIP, and metadata
    """
    # Basic info
    image_id: str
    case_id: str
    file_path: str
    file_hash: str
    
    # Visual embeddings
    clip_embedding: np.ndarray
    
    # Object detection (YOLO-World)
    detected_objects: Dict[str, Any] = field(default_factory=dict)
    object_count: int = 0
    forensic_summary: Dict[str, Any] = field(default_factory=dict)
    
    # Face recognition
    faces: List[Dict[str, Any]] = field(default_factory=list)
    face_count: int = 0
    face_embeddings: List[np.ndarray] = field(default_factory=list)
    
    # Image description (BLIP-2)
    blip_description: str = ""
    blip_scene_type: str = "unknown"
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    location: Optional[Dict[str, float]] = None
    
    # Status
    success: bool = True
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        
        # Convert embeddings to lists
        if isinstance(data['clip_embedding'], np.ndarray):
            data['clip_embedding'] = data['clip_embedding'].tolist()
        
        # Convert face embeddings
        data['face_embeddings'] = [
            emb.tolist() if isinstance(emb, np.ndarray) else emb
            for emb in data['face_embeddings']
        ]
        
        # Convert datetime
        if data['timestamp']:
            data['timestamp'] = data['timestamp'].isoformat()
        
        return data
    
    def get_text_description(self) -> str:
        """Generate natural language description of image content"""
        parts = []
        
        # Start with BLIP description (most natural)
        if self.blip_description:
            parts.append(f"Description: {self.blip_description}")
        
        # Objects
        if self.detected_objects:
            obj_list = []
            for obj_class, obj_data in self.detected_objects.items():
                count = obj_data['count']
                if count == 1:
                    obj_list.append(f"one {obj_class}")
                else:
                    obj_list.append(f"{count} {obj_class}s")
            
            if obj_list:
                parts.append(f"Contains {', '.join(obj_list)}")
        
        # People
        if self.forensic_summary.get('person_count', 0) > 0:
            parts.append(f"{self.forensic_summary['person_count']} person(s) detected")
        
        # Faces
        if self.face_count > 0:
            parts.append(f"{self.face_count} face(s) identified")
        
        # Vehicles
        if self.forensic_summary.get('has_vehicles'):
            vehicle_types = self.forensic_summary.get('vehicle_types', [])
            parts.append(f"Vehicles: {', '.join(vehicle_types)}")
        
        # Weapons
        if self.forensic_summary.get('has_weapons'):
            parts.append("⚠️ Weapons detected")
        
        return ". ".join(parts) if parts else "No significant objects detected"


class EnhancedImageProcessor:
    """
    Complete forensic image processing pipeline
    
    Features:
    - YOLO-World object detection (80+ classes, open-vocabulary)
    - Face detection and recognition
    - CLIP semantic embeddings
    - BLIP-2 image captioning
    - EXIF metadata extraction
    - Automatic FAISS indexing
    
    Supported formats: .jpg, .jpeg, .png, .bmp, .gif, .tiff, .tif, .webp, .ico, .jfif
    """
    
    def __init__(
        self,
        auto_index: bool = True,
        enable_yolo: bool = True,
        enable_faces: bool = True,
        enable_clip: bool = True,
        enable_blip: bool = True
    ):
        """
        Initialize enhanced image processor
        
        Args:
            auto_index: Automatically index in FAISS
            enable_yolo: Enable YOLO-World object detection
            enable_faces: Enable face recognition
            enable_clip: Enable CLIP embeddings
            enable_blip: Enable BLIP-2 image captioning
        """
        if not PIL_AVAILABLE:
            raise ImportError("PIL required. Install: pip install pillow")
        
        self.auto_index = auto_index
        self.enable_yolo = enable_yolo
        self.enable_faces = enable_faces
        self.enable_clip = enable_clip
        self.enable_blip = enable_blip
        
        # Initialize components
        if enable_clip:
            self.clip_embedder = get_clip_embedder()
        
        if enable_yolo:
            self.yolo_detector = get_yolo_detector()
        
        if enable_faces:
            self.face_recognizer = get_face_recognizer()
        
        if enable_blip:
            self.blip_analyzer = get_blip2_analyzer()
        
        if auto_index:
            self.multimodal_index = get_multimodal_index()
        
        logger.info(f"✅ Enhanced Image Processor initialized")
        logger.info(f"   YOLO: {enable_yolo}, Faces: {enable_faces}, CLIP: {enable_clip}, BLIP: {enable_blip}")
    
    def process_image(
        self,
        image_path: Path,
        case_id: str,
        image_id: Optional[str] = None
    ) -> EnhancedImageResult:
        """
        Process a single image with full analysis
        
        Args:
            image_path: Path to image file
            case_id: Case identifier
            image_id: Optional custom image ID
        
        Returns:
            EnhancedImageResult with complete analysis
        """
        image_path = Path(image_path)
        
        if not image_path.exists():
            return EnhancedImageResult(
                image_id=image_id or "unknown",
                case_id=case_id,
                file_path=str(image_path),
                file_hash="",
                clip_embedding=np.array([]),
                success=False,
                error="File not found"
            )
        
        try:
            # Generate image ID
            if not image_id:
                image_id = self._generate_image_id(image_path)
            
            # Compute file hash
            file_hash = self._compute_file_hash(image_path)
            
            # Extract EXIF metadata
            metadata = self._extract_exif(image_path)
            
            # CLIP embedding (semantic)
            clip_embedding = np.array([])
            if self.enable_clip:
                clip_embedding = self.clip_embedder.encode_image(image_path)
                if clip_embedding is None:
                    clip_embedding = np.array([])
            
            # YOLO-World object detection
            detected_objects = {}
            object_count = 0
            forensic_summary = {}
            
            if self.enable_yolo:
                yolo_results = self.yolo_detector.detect(image_path)
                detected_objects = yolo_results.get('objects', {})
                object_count = yolo_results.get('total_objects', 0)
                forensic_summary = yolo_results.get('forensic_summary', {})
                
                # Add to metadata
                metadata['yolo_detections'] = detected_objects
                metadata['yolo_summary'] = forensic_summary
            
            # Face detection and recognition
            faces = []
            face_count = 0
            face_embeddings = []
            
            if self.enable_faces:
                try:
                    # Fast face count first
                    face_count = self.face_recognizer.detect_faces_fast(image_path)
                    
                    # If faces found, do detailed analysis
                    if face_count > 0:
                        face_results = self.face_recognizer.analyze_faces(image_path)
                        faces = face_results
                        face_embeddings = [f.get('embedding', np.array([])) for f in face_results]
                        
                        # Add to metadata
                        metadata['faces'] = [{
                            'bbox': f.get('bbox'),
                            'age': f.get('age'),
                            'gender': f.get('gender'),
                            'emotion': f.get('emotion')
                        } for f in faces]
                
                except Exception as e:
                    logger.warning(f"Face detection failed for {image_path.name}: {e}")
            
            # BLIP-2 image description
            blip_description = ""
            blip_scene_type = "unknown"
            
            if self.enable_blip:
                try:
                    blip_analysis = self.blip_analyzer.analyze_image(image_path, style="forensic")
                    blip_description = blip_analysis.get('description', '')
                    blip_scene_type = blip_analysis.get('scene_type', 'unknown')
                    
                    # Add to metadata
                    metadata['blip_description'] = blip_description
                    metadata['blip_scene_type'] = blip_scene_type
                    
                except Exception as e:
                    logger.warning(f"BLIP description failed for {image_path.name}: {e}")
            
            # Create result
            result = EnhancedImageResult(
                image_id=image_id,
                case_id=case_id,
                file_path=str(image_path),
                file_hash=file_hash,
                clip_embedding=clip_embedding,
                detected_objects=detected_objects,
                object_count=object_count,
                forensic_summary=forensic_summary,
                faces=faces,
                face_count=face_count,
                face_embeddings=face_embeddings,
                blip_description=blip_description,
                blip_scene_type=blip_scene_type,
                metadata=metadata,
                timestamp=metadata.get('timestamp'),
                location=metadata.get('location'),
                success=True
            )
            
            # Auto-index if enabled
            if self.auto_index:
                self._index_result(result)
            
            logger.info(f"✅ Processed {image_path.name}: {object_count} objects, {face_count} faces")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process {image_path}: {e}")
            return EnhancedImageResult(
                image_id=image_id or "unknown",
                case_id=case_id,
                file_path=str(image_path),
                file_hash="",
                clip_embedding=np.array([]),
                success=False,
                error=str(e)
            )
    
    def process_images_batch(
        self,
        image_paths: List[Path],
        case_id: str
    ) -> List[EnhancedImageResult]:
        """
        Process multiple images
        
        Args:
            image_paths: List of image paths
            case_id: Case identifier
        
        Returns:
            List of EnhancedImageResult objects
        """
        results = []
        
        logger.info(f"Processing {len(image_paths)} images for case {case_id}")
        
        for i, image_path in enumerate(image_paths, 1):
            logger.info(f"[{i}/{len(image_paths)}] {image_path.name}")
            result = self.process_image(image_path, case_id)
            results.append(result)
        
        logger.info(f"✅ Processed {len(results)} images")
        
        return results
    
    def _index_result(self, result: EnhancedImageResult):
        """Index result in FAISS"""
        if not result.success:
            return
        
        try:
            # Index CLIP embedding
            if len(result.clip_embedding) > 0:
                self.multimodal_index.add_embeddings(
                    modality="image",
                    ids=[result.image_id],
                    embeddings=result.clip_embedding.reshape(1, -1),
                    metadatas=[{
                        "case_id": result.case_id,
                        "file_path": result.file_path,
                        "content": result.get_text_description(),
                        "blip_description": result.blip_description,
                        "scene_type": result.blip_scene_type,
                        "object_count": result.object_count,
                        "face_count": result.face_count,
                        "has_people": result.forensic_summary.get('has_people', False),
                        "has_vehicles": result.forensic_summary.get('has_vehicles', False),
                        "has_weapons": result.forensic_summary.get('has_weapons', False)
                    }]
                )
            
            # Index BLIP description as text for text-based search
            if result.blip_description:
                try:
                    # Encode BLIP description using CLIP text encoder for semantic search
                    text_embedding = self.clip_embedder.encode_text(result.blip_description)
                    if text_embedding is not None and len(text_embedding) > 0:
                        self.multimodal_index.add_embeddings(
                            modality="text",
                            ids=[f"{result.image_id}_caption"],
                            embeddings=text_embedding.reshape(1, -1),
                            metadatas=[{
                                "case_id": result.case_id,
                                "source_image": result.image_id,
                                "source_path": result.file_path,
                                "content": result.blip_description,
                                "type": "image_caption",
                                "scene_type": result.blip_scene_type
                            }]
                        )
                except Exception as e:
                    logger.warning(f"Failed to index BLIP description as text: {e}")
            
            # Index face embeddings in dedicated face index
            if result.face_embeddings:
                # Filter valid embeddings
                valid_embeddings = [emb for emb in result.face_embeddings if len(emb) > 0]
                
                if valid_embeddings:
                    # Stack embeddings properly
                    if len(valid_embeddings) == 1:
                        face_embs = valid_embeddings[0].reshape(1, -1)  # Single embedding: reshape to 2D
                    else:
                        face_embs = np.vstack(valid_embeddings)  # Multiple: stack vertically
                    
                    # Build face IDs and metadata
                    face_ids = []
                    face_metadatas = []
                    for i, face in enumerate(result.faces):
                        face_ids.append(f"{result.image_id}_face_{i}")
                        face_metadatas.append({
                            "case_id": result.case_id,
                            "source_image": result.image_id,
                            "source_path": result.file_path,
                            "face_index": i,
                            "age": face.get('age'),
                            "gender": face.get('gender'), 
                            "emotion": face.get('emotion'),
                            "bbox": face.get('bbox'),
                            "content": f"Face from {Path(result.file_path).name}: {face.get('gender', 'unknown')} aged ~{face.get('age', 'unknown')}"
                        })
                    
                    self.multimodal_index.add_embeddings(
                        modality="face",  # Use dedicated face index
                        ids=face_ids,
                        embeddings=face_embs,
                        metadatas=face_metadatas
                    )
            
            # Save indices
            self.multimodal_index.save_indices()
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to index {result.image_id}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _generate_image_id(self, image_path: Path) -> str:
        """Generate unique image ID from file path"""
        path_str = str(image_path.resolve())
        hash_obj = hashlib.md5(path_str.encode())
        return f"img_{hash_obj.hexdigest()[:16]}"
    
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception as e:
            logger.error(f"Failed to compute hash for {file_path}: {e}")
            return ""
    
    def _extract_exif(self, image_path: Path) -> Dict[str, Any]:
        """Extract EXIF metadata from image"""
        metadata = {
            "filename": image_path.name,
            "file_size": image_path.stat().st_size,
        }
        
        try:
            image = Image.open(image_path)
            
            # Get image dimensions
            metadata["width"], metadata["height"] = image.size
            metadata["format"] = image.format
            metadata["mode"] = image.mode
            
            # Extract EXIF data
            exif_data = image.getexif()
            
            if exif_data:
                for tag_id, value in exif_data.items():
                    tag_name = TAGS.get(tag_id, tag_id)
                    
                    if tag_name == "DateTime":
                        try:
                            timestamp = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                            metadata["timestamp"] = timestamp
                        except:
                            pass
                    
                    elif isinstance(value, (str, int, float)):
                        metadata[f"exif_{tag_name}"] = value
            
        except Exception as e:
            logger.warning(f"Failed to extract EXIF from {image_path}: {e}")
        
        return metadata


# Convenience function
def process_case_images(case_id: str, images_dir: Path) -> List[EnhancedImageResult]:
    """
    Process all images in a case directory
    
    Args:
        case_id: Case identifier
        images_dir: Directory containing images
    
    Returns:
        List of processing results
    """
    processor = EnhancedImageProcessor(auto_index=True)
    
    # Find all image files
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']
    image_paths = []
    
    for ext in image_extensions:
        image_paths.extend(images_dir.glob(f"**/*{ext}"))
        image_paths.extend(images_dir.glob(f"**/*{ext.upper()}"))
    
    logger.info(f"Found {len(image_paths)} images in {images_dir}")
    
    if not image_paths:
        logger.warning("No images found")
        return []
    
    # Process in batch
    results = processor.process_images_batch(image_paths, case_id)
    
    return results
