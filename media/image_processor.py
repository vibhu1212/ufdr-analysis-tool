"""
Image Processing Pipeline
Complete pipeline for processing forensic images:
- Generate CLIP embeddings
- Extract metadata (EXIF, file info)
- Store in multi-modal FAISS index
- Enable semantic search
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib
import json

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

import numpy as np

# Import our custom modules
from media.clip_embedder import get_clip_embedder
from vector.multimodal_index import get_multimodal_index

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ImageProcessingResult:
    """Result from processing an image"""
    image_id: str
    case_id: str
    file_path: str
    file_hash: str
    embedding: np.ndarray
    metadata: Dict[str, Any]
    timestamp: Optional[datetime]
    location: Optional[Dict[str, float]]
    success: bool
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        # Convert embedding to list for JSON serialization
        if isinstance(data['embedding'], np.ndarray):
            data['embedding'] = data['embedding'].tolist()
        # Convert datetime to ISO format
        if data['timestamp']:
            data['timestamp'] = data['timestamp'].isoformat()
        return data


class ImageProcessor:
    """
    Complete image processing pipeline
    
    Features:
    - CLIP embeddings for semantic search
    - EXIF metadata extraction
    - File hashing for integrity
    - Automatic indexing in FAISS
    """
    
    def __init__(self, auto_index: bool = True):
        """
        Initialize image processor
        
        Args:
            auto_index: Automatically index embeddings in FAISS
        """
        if not PIL_AVAILABLE:
            raise ImportError("PIL required. Install: pip install pillow")
        
        self.auto_index = auto_index
        self.clip_embedder = get_clip_embedder()
        
        if auto_index:
            self.multimodal_index = get_multimodal_index()
        
        logger.info(f"✅ Image Processor initialized (auto_index={auto_index})")
    
    def process_image(
        self, 
        image_path: Path, 
        case_id: str,
        image_id: Optional[str] = None
    ) -> ImageProcessingResult:
        """
        Process a single image
        
        Args:
            image_path: Path to image file
            case_id: Case identifier
            image_id: Optional custom image ID
            
        Returns:
            ImageProcessingResult with embedding and metadata
        """
        image_path = Path(image_path)
        
        if not image_path.exists():
            return ImageProcessingResult(
                image_id=image_id or "unknown",
                case_id=case_id,
                file_path=str(image_path),
                file_hash="",
                embedding=np.array([]),
                metadata={},
                timestamp=None,
                location=None,
                success=False,
                error="File not found"
            )
        
        try:
            # Generate image ID if not provided
            if not image_id:
                image_id = self._generate_image_id(image_path)
            
            # Compute file hash
            file_hash = self._compute_file_hash(image_path)
            
            # Extract EXIF metadata
            metadata = self._extract_exif(image_path)
            
            # Generate CLIP embedding
            embedding = self.clip_embedder.encode_image(image_path)
            
            if embedding is None:
                return ImageProcessingResult(
                    image_id=image_id,
                    case_id=case_id,
                    file_path=str(image_path),
                    file_hash=file_hash,
                    embedding=np.array([]),
                    metadata=metadata,
                    timestamp=metadata.get('timestamp'),
                    location=metadata.get('location'),
                    success=False,
                    error="Failed to generate embedding"
                )
            
            # Create result
            result = ImageProcessingResult(
                image_id=image_id,
                case_id=case_id,
                file_path=str(image_path),
                file_hash=file_hash,
                embedding=embedding,
                metadata=metadata,
                timestamp=metadata.get('timestamp'),
                location=metadata.get('location'),
                success=True
            )
            
            # Auto-index if enabled
            if self.auto_index:
                self._index_result(result)
            
            logger.info(f"✅ Processed image: {image_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process image {image_path}: {e}")
            return ImageProcessingResult(
                image_id=image_id or "unknown",
                case_id=case_id,
                file_path=str(image_path),
                file_hash="",
                embedding=np.array([]),
                metadata={},
                timestamp=None,
                location=None,
                success=False,
                error=str(e)
            )
    
    def process_images_batch(
        self,
        image_paths: List[Path],
        case_id: str,
        batch_size: int = 32
    ) -> List[ImageProcessingResult]:
        """
        Process multiple images in batches
        
        Args:
            image_paths: List of image paths
            case_id: Case identifier
            batch_size: Batch size for CLIP encoding
            
        Returns:
            List of ImageProcessingResult objects
        """
        logger.info(f"Processing {len(image_paths)} images for case {case_id}")
        
        results = []
        
        # Extract metadata for all images first
        metadata_list = []
        valid_paths = []
        valid_ids = []
        
        for path in image_paths:
            if not path.exists():
                results.append(ImageProcessingResult(
                    image_id=self._generate_image_id(path),
                    case_id=case_id,
                    file_path=str(path),
                    file_hash="",
                    embedding=np.array([]),
                    metadata={},
                    timestamp=None,
                    location=None,
                    success=False,
                    error="File not found"
                ))
                continue
            
            image_id = self._generate_image_id(path)
            file_hash = self._compute_file_hash(path)
            metadata = self._extract_exif(path)
            metadata['file_hash'] = file_hash
            metadata['image_id'] = image_id
            
            metadata_list.append(metadata)
            valid_paths.append(path)
            valid_ids.append(image_id)
        
        if not valid_paths:
            logger.warning("No valid images to process")
            return results
        
        # Generate embeddings in batch (faster)
        logger.info(f"Generating embeddings for {len(valid_paths)} images...")
        embeddings = self.clip_embedder.encode_images(valid_paths, batch_size=batch_size)
        
        # Create results
        for i, (path, image_id, metadata, embedding) in enumerate(zip(
            valid_paths, valid_ids, metadata_list, embeddings
        )):
            result = ImageProcessingResult(
                image_id=image_id,
                case_id=case_id,
                file_path=str(path),
                file_hash=metadata.get('file_hash', ''),
                embedding=embedding,
                metadata=metadata,
                timestamp=metadata.get('timestamp'),
                location=metadata.get('location'),
                success=True
            )
            results.append(result)
        
        # Auto-index all results if enabled
        if self.auto_index and results:
            self._index_results_batch(results)
        
        logger.info(f"✅ Processed {len(results)} images successfully")
        return results
    
    def _index_result(self, result: ImageProcessingResult):
        """Index a single image result in FAISS"""
        if not result.success:
            return
        
        try:
            # Prepare metadata
            index_metadata = {
                "case_id": result.case_id,
                "file_path": result.file_path,
                "file_hash": result.file_hash,
                "content": f"Image {result.image_id}",  # Required field
                "timestamp": result.timestamp.timestamp() if result.timestamp else 0,
            }
            
            # Add location if available
            if result.location:
                index_metadata["location"] = json.dumps(result.location)
            
            # Add other metadata
            index_metadata.update(result.metadata)
            
            # Add to FAISS index
            self.multimodal_index.add_embeddings(
                modality="image",
                ids=[result.image_id],
                embeddings=result.embedding.reshape(1, -1),
                metadatas=[index_metadata]
            )
            
        except Exception as e:
            logger.error(f"Failed to index image {result.image_id}: {e}")
    
    def _index_results_batch(self, results: List[ImageProcessingResult]):
        """Index multiple image results in FAISS"""
        successful_results = [r for r in results if r.success]
        
        if not successful_results:
            return
        
        try:
            # Prepare data
            ids = [r.image_id for r in successful_results]
            embeddings = np.vstack([r.embedding for r in successful_results])
            
            metadatas = []
            for r in successful_results:
                metadata = {
                    "case_id": r.case_id,
                    "file_path": r.file_path,
                    "file_hash": r.file_hash,
                    "content": f"Image {r.image_id}",
                    "timestamp": r.timestamp.timestamp() if r.timestamp else 0,
                }
                
                if r.location:
                    metadata["location"] = json.dumps(r.location)
                
                metadata.update(r.metadata)
                metadatas.append(metadata)
            
            # Add batch to FAISS index
            self.multimodal_index.add_embeddings(
                modality="image",
                ids=ids,
                embeddings=embeddings,
                metadatas=metadatas
            )
            
            # Save index
            self.multimodal_index.save_indices()
            
            logger.info(f"✅ Indexed {len(ids)} images in FAISS")
            
        except Exception as e:
            logger.error(f"Failed to index batch: {e}")
    
    def _generate_image_id(self, image_path: Path) -> str:
        """Generate unique image ID from file path"""
        # Use file path hash as ID
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
                    
                    # Handle specific tags
                    if tag_name == "DateTime":
                        try:
                            timestamp = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                            metadata["timestamp"] = timestamp
                        except:
                            pass
                    
                    elif tag_name == "GPSInfo":
                        # Extract GPS coordinates
                        gps_data = self._parse_gps_info(value)
                        if gps_data:
                            metadata["location"] = gps_data
                    
                    elif isinstance(value, (str, int, float)):
                        metadata[f"exif_{tag_name}"] = value
            
        except Exception as e:
            logger.warning(f"Failed to extract EXIF from {image_path}: {e}")
        
        return metadata
    
    def _parse_gps_info(self, gps_info: Dict) -> Optional[Dict[str, float]]:
        """Parse GPS info from EXIF"""
        try:
            if not gps_info:
                return None
            
            # GPS coordinates are in degrees, minutes, seconds
            def convert_to_degrees(value):
                d, m, s = value
                return d + (m / 60.0) + (s / 3600.0)
            
            gps_data = {}
            
            if 2 in gps_info and 1 in gps_info:  # Latitude
                lat = convert_to_degrees(gps_info[2])
                if gps_info[1] == 'S':
                    lat = -lat
                gps_data["latitude"] = lat
            
            if 4 in gps_info and 3 in gps_info:  # Longitude
                lon = convert_to_degrees(gps_info[4])
                if gps_info[3] == 'W':
                    lon = -lon
                gps_data["longitude"] = lon
            
            if 5 in gps_info and 6 in gps_info:  # Altitude
                gps_data["altitude"] = float(gps_info[6])
            
            return gps_data if gps_data else None
            
        except Exception as e:
            logger.warning(f"Failed to parse GPS info: {e}")
            return None


# Convenience function
def process_case_images(case_id: str, images_dir: Path) -> List[ImageProcessingResult]:
    """
    Process all images in a case directory
    
    Args:
        case_id: Case identifier
        images_dir: Directory containing images
        
    Returns:
        List of processing results
    """
    processor = ImageProcessor(auto_index=True)
    
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
