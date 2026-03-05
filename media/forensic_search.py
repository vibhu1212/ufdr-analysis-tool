"""
Forensic Image Search Interface
Unified search across CLIP embeddings, face embeddings, and YOLO metadata
"""

import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Union
from dataclasses import dataclass

from media.clip_embedder import get_clip_embedder
from media.face_recognizer import get_face_recognizer
from vector.multimodal_index import get_multimodal_index, MultiModalSearchResult

logger = logging.getLogger(__name__)


@dataclass
class ForensicSearchResult:
    """
    Unified search result for forensic images
    """
    image_id: str
    case_id: str
    file_path: str
    search_type: str  # 'semantic', 'face', 'hybrid'
    confidence: float
    distance: float
    
    # Content info
    description: str
    object_count: int = 0
    face_count: int = 0
    
    # Forensic flags
    has_people: bool = False
    has_vehicles: bool = False
    has_weapons: bool = False
    
    # Face-specific (if face match)
    matched_face: Optional[Dict[str, Any]] = None
    
    # Raw metadata
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict:
        result = {
            'image_id': self.image_id,
            'case_id': self.case_id,
            'file_path': self.file_path,
            'search_type': self.search_type,
            'confidence': self.confidence,
            'distance': self.distance,
            'description': self.description,
            'object_count': self.object_count,
            'face_count': self.face_count,
            'has_people': self.has_people,
            'has_vehicles': self.has_vehicles,
            'has_weapons': self.has_weapons,
        }
        
        if self.matched_face:
            result['matched_face'] = self.matched_face
        
        if self.metadata:
            result['metadata'] = self.metadata
        
        return result


class ForensicSearchEngine:
    """
    Forensic Image Search Engine
    
    Search modalities:
    1. Semantic search - Natural language or image similarity (CLIP)
    2. Face search - Find images containing similar faces
    3. Object filtering - Filter by detected objects (YOLO)
    4. Hybrid search - Combine multiple modalities
    """
    
    def __init__(self):
        """Initialize search engine with all components"""
        self.clip_embedder = get_clip_embedder()
        self.face_recognizer = get_face_recognizer()
        self.multimodal_index = get_multimodal_index()
        
        logger.info("✅ Forensic Search Engine initialized")
        logger.info(f"   Index stats: {self.multimodal_index.get_stats()}")
    
    def search_by_text(
        self,
        query: str,
        n_results: int = 10,
        case_id: Optional[str] = None,
        object_filter: Optional[List[str]] = None
    ) -> List[ForensicSearchResult]:
        """
        Search images by natural language description
        
        Args:
            query: Text query (e.g., "person holding weapon", "red car at night")
            n_results: Number of results to return
            case_id: Optional filter by case
            object_filter: Optional filter by detected objects (e.g., ["person", "car"])
        
        Returns:
            List of matching images
        """
        logger.info(f"Text search: '{query}'")
        
        # Encode query with CLIP
        query_embedding = self.clip_embedder.encode_text(query)
        
        if query_embedding is None:
            logger.error("Failed to encode query")
            return []
        
        # Search image index
        results = self.multimodal_index.search(
            modality="image",
            query_embedding=query_embedding,
            n_results=n_results * 2 if object_filter else n_results,  # Get more if filtering
            case_id_filter=case_id
        )
        
        # Apply object filter if specified
        if object_filter:
            filtered_results = []
            for r in results:
                # Check if any required objects are present
                # This is a simple check - could be enhanced
                content_lower = r.content.lower()
                if any(obj.lower() in content_lower for obj in object_filter):
                    filtered_results.append(r)
                    if len(filtered_results) >= n_results:
                        break
            results = filtered_results
        
        # Convert to ForensicSearchResult
        return self._convert_results(results, search_type="semantic")
    
    def search_by_image(
        self,
        image_path: Union[str, Path],
        n_results: int = 10,
        case_id: Optional[str] = None
    ) -> List[ForensicSearchResult]:
        """
        Find similar images using CLIP similarity
        
        Args:
            image_path: Path to query image
            n_results: Number of results
            case_id: Optional filter by case
        
        Returns:
            List of similar images
        """
        image_path = Path(image_path)
        logger.info(f"Image similarity search: {image_path.name}")
        
        # Encode image with CLIP
        query_embedding = self.clip_embedder.encode_image(image_path)
        
        if query_embedding is None:
            logger.error(f"Failed to encode image: {image_path}")
            return []
        
        # Search image index
        results = self.multimodal_index.search(
            modality="image",
            query_embedding=query_embedding,
            n_results=n_results + 1,  # +1 because query image might be in index
            case_id_filter=case_id
        )
        
        # Remove self-match if present
        results = [r for r in results if Path(r.metadata.get('file_path', '')).name != image_path.name][:n_results]
        
        return self._convert_results(results, search_type="semantic")
    
    def search_by_face(
        self,
        face_image_path: Union[str, Path],
        n_results: int = 10,
        case_id: Optional[str] = None,
        similarity_threshold: float = 0.6
    ) -> List[ForensicSearchResult]:
        """
        Find images containing similar faces
        
        Args:
            face_image_path: Path to image containing reference face
            n_results: Number of results
            case_id: Optional filter by case
            similarity_threshold: Minimum similarity (0-1)
        
        Returns:
            List of images with matching faces
        """
        face_image_path = Path(face_image_path)
        logger.info(f"Face search: {face_image_path.name}")
        
        # Extract face embedding from query image
        faces = self.face_recognizer.analyze_faces(face_image_path)
        
        if not faces:
            logger.warning(f"No faces found in {face_image_path}")
            return []
        
        # Use first face as query
        query_face = faces[0]
        query_embedding = query_face.get('embedding')
        
        if query_embedding is None or len(query_embedding) == 0:
            logger.error("Failed to get face embedding")
            return []
        
        logger.info(f"Extracted face: {query_face.get('gender')}, age ~{query_face.get('age')}")
        
        # Search face index
        results = self.multimodal_index.search(
            modality="face",
            query_embedding=query_embedding,
            n_results=n_results * 2,  # Get more to filter by threshold
            case_id_filter=case_id
        )
        
        # Filter by similarity threshold and remove self-match
        filtered_results = []
        for r in results:
            if r.confidence >= similarity_threshold:
                # Check if it's the source image
                source_path = Path(r.metadata.get('source_path', ''))
                if source_path.name != face_image_path.name:
                    filtered_results.append(r)
        
        # Convert and add face info
        forensic_results = []
        for r in filtered_results[:n_results]:
            forensic_result = self._convert_single_result(r, search_type="face")
            forensic_result.matched_face = {
                'age': r.metadata.get('age'),
                'gender': r.metadata.get('gender'),
                'emotion': r.metadata.get('emotion'),
                'face_index': r.metadata.get('face_index')
            }
            forensic_results.append(forensic_result)
        
        logger.info(f"Found {len(forensic_results)} face matches")
        return forensic_results
    
    def search_by_objects(
        self,
        required_objects: List[str],
        n_results: int = 10,
        case_id: Optional[str] = None
    ) -> List[ForensicSearchResult]:
        """
        Find images containing specific objects
        
        Args:
            required_objects: List of objects (e.g., ["person", "weapon"])
            n_results: Number of results
            case_id: Optional filter by case
        
        Returns:
            List of matching images
        """
        logger.info(f"Object search: {required_objects}")
        
        # Build text query from objects
        query = f"image containing {' and '.join(required_objects)}"
        
        return self.search_by_text(
            query=query,
            n_results=n_results,
            case_id=case_id,
            object_filter=required_objects
        )
    
    def hybrid_search(
        self,
        text_query: Optional[str] = None,
        face_image: Optional[Union[str, Path]] = None,
        required_objects: Optional[List[str]] = None,
        n_results: int = 10,
        case_id: Optional[str] = None
    ) -> Dict[str, List[ForensicSearchResult]]:
        """
        Perform hybrid search across multiple modalities
        
        Args:
            text_query: Optional text query
            face_image: Optional face reference image
            required_objects: Optional list of required objects
            n_results: Number of results per modality
            case_id: Optional filter by case
        
        Returns:
            Dictionary mapping search type to results
        """
        logger.info("🔍 Hybrid search")
        
        results = {}
        
        if text_query:
            logger.info(f"  • Text: '{text_query}'")
            results['semantic'] = self.search_by_text(
                text_query,
                n_results=n_results,
                case_id=case_id,
                object_filter=required_objects
            )
        
        if face_image:
            logger.info(f"  • Face: {Path(face_image).name}")
            results['face'] = self.search_by_face(
                face_image,
                n_results=n_results,
                case_id=case_id
            )
        
        if required_objects and not text_query:  # Only if not already done
            logger.info(f"  • Objects: {required_objects}")
            results['objects'] = self.search_by_objects(
                required_objects,
                n_results=n_results,
                case_id=case_id
            )
        
        return results
    
    def _convert_results(
        self,
        results: List[MultiModalSearchResult],
        search_type: str
    ) -> List[ForensicSearchResult]:
        """Convert MultiModalSearchResult to ForensicSearchResult"""
        return [self._convert_single_result(r, search_type) for r in results]
    
    def _convert_single_result(
        self,
        result: MultiModalSearchResult,
        search_type: str
    ) -> ForensicSearchResult:
        """Convert single result"""
        metadata = result.metadata
        
        return ForensicSearchResult(
            image_id=result.id,
            case_id=result.case_id,
            file_path=metadata.get('file_path', metadata.get('source_path', '')),
            search_type=search_type,
            confidence=result.confidence,
            distance=result.distance,
            description=result.content,
            object_count=metadata.get('object_count', 0),
            face_count=metadata.get('face_count', 0),
            has_people=metadata.get('has_people', False),
            has_vehicles=metadata.get('has_vehicles', False),
            has_weapons=metadata.get('has_weapons', False),
            metadata=metadata
        )
    
    def get_index_stats(self) -> Dict[str, int]:
        """Get statistics about indexed data"""
        return self.multimodal_index.get_stats()


# Convenience function
def get_forensic_search_engine() -> ForensicSearchEngine:
    """Get singleton forensic search engine"""
    if not hasattr(get_forensic_search_engine, "_instance"):
        get_forensic_search_engine._instance = ForensicSearchEngine()
    return get_forensic_search_engine._instance
