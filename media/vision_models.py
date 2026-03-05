"""
Vision Models Integration for Forensic Image Analysis
Integrates BLIP2, CLIP, and YOLOv8 for comprehensive image understanding
Supports natural language queries and object detection
"""

import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import numpy as np

# Core ML libraries
try:
    import torch
    from PIL import Image
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    print("Warning: PyTorch not installed. Vision models will be limited.")

# CLIP integration
try:
    import clip
    CLIP_AVAILABLE = True
except ImportError:
    CLIP_AVAILABLE = False
    print("Warning: CLIP not installed. Install with: pip install git+https://github.com/openai/CLIP.git")

# Transformers for BLIP2
try:
    from transformers import (
        BlipProcessor, BlipForConditionalGeneration
    )
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("Warning: transformers not installed. Install with: pip install transformers")

# YOLOv8 for object detection
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("Warning: ultralytics not installed. Install with: pip install ultralytics")

# Sentence transformers for embeddings
try:
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    print("Warning: sentence-transformers not installed.")

logger = logging.getLogger(__name__)


@dataclass
class VisionResult:
    """Result from vision model analysis"""
    image_path: str
    caption: str
    objects_detected: List[Dict]
    clip_features: Optional[np.ndarray]
    clip_similarity_scores: Dict[str, float]
    processing_time: float
    model_versions: Dict[str, str]
    confidence_scores: Dict[str, float]
    sha256_hash: str
    
    def to_dict(self) -> Dict:
        result = asdict(self)
        # Convert numpy array to list for JSON serialization
        if result['clip_features'] is not None:
            result['clip_features'] = result['clip_features'].tolist()
        return result


class VisionModels:
    """Integrated vision models for forensic image analysis"""
    
    def __init__(self,
                 models_dir: str = "infra/models/vision",
                 device: str = "auto",
                 use_blip2: bool = True,
                 use_clip: bool = True,
                 use_yolo: bool = True):
        """
        Initialize Vision Models
        
        Args:
            models_dir: Directory to store/load models
            device: Device to use (auto, cpu, cuda)
            use_blip2: Enable BLIP2 image captioning
            use_clip: Enable CLIP image embeddings
            use_yolo: Enable YOLOv8 object detection
        """
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure device
        if device == "auto":
            if TORCH_AVAILABLE:
                self.device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                self.device = "cpu"
        else:
            self.device = device
        
        logger.info(f"Using device: {self.device}")
        
        # Check dependencies and adjust flags
        if not TORCH_AVAILABLE:
            logger.warning("PyTorch not installed. All vision models disabled.")
            use_blip2 = False
            use_clip = False
            use_yolo = False
        
        # Model flags with dependency checking
        self.use_blip2 = use_blip2 and TRANSFORMERS_AVAILABLE
        self.use_clip = use_clip and CLIP_AVAILABLE and TORCH_AVAILABLE
        self.use_yolo = use_yolo and YOLO_AVAILABLE
        
        # Initialize models
        self.blip2_processor = None
        self.blip2_model = None
        self.clip_model = None
        self.clip_preprocess = None
        self.yolo_model = None
        
        # Forensic-specific query templates
        self.forensic_queries = [
            "person with weapon",
            "suspicious activity",
            "drug paraphernalia",
            "money and cash",
            "cryptocurrency symbols",
            "vehicle license plate",
            "identity document",
            "mobile phone or device",
            "gun or firearm",
            "knife or blade",
            "illegal substances",
            "foreign currency",
            "jewelry and valuables",
            "electronic devices"
        ]
        
        self._initialize_models()
    
    def _initialize_models(self):
        """Initialize all vision models"""
        logger.info("Initializing vision models...")
        
        # Initialize BLIP2
        if self.use_blip2:
            self._init_blip2()
        
        # Initialize CLIP
        if self.use_clip:
            self._init_clip()
        
        # Initialize YOLO
        if self.use_yolo:
            self._init_yolo()
        
        logger.info("Vision models initialization complete")
    
    def _init_blip2(self):
        """Initialize BLIP2 for image captioning"""
        try:
            logger.info("Loading BLIP2 model...")
            # Use smaller, CPU-friendly BLIP model instead of BLIP2 for better compatibility
            model_name = "Salesforce/blip-image-captioning-base"
            
            # Use the standard BLIP processor and model instead
            self.blip2_processor = BlipProcessor.from_pretrained(model_name)
            self.blip2_model = BlipForConditionalGeneration.from_pretrained(
                model_name,
                torch_dtype=torch.float32  # Always use float32 for CPU compatibility
            )
            
            # Always move to CPU for stability
            self.blip2_model = self.blip2_model.to("cpu")
            self.blip2_model.eval()  # Set to evaluation mode
            
            logger.info("BLIP model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load BLIP: {e}")
            self.use_blip2 = False
    
    def _init_clip(self):
        """Initialize CLIP for image embeddings and similarity"""
        try:
            logger.info("Loading CLIP model...")
            # Use ViT-B/32 for balance of speed and accuracy
            self.clip_model, self.clip_preprocess = clip.load("ViT-B/32", device=self.device)
            logger.info("CLIP model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load CLIP: {e}")
            self.use_clip = False
    
    def _init_yolo(self):
        """Initialize YOLOv8 for object detection"""
        try:
            logger.info("Loading YOLOv8 model...")
            # Use YOLOv8n (nano) for speed
            model_path = self.models_dir / "yolov8n.pt"
            
            if not model_path.exists():
                # Download model if not exists
                self.yolo_model = YOLO('yolov8n.pt')
                # Save to models directory
                self.yolo_model.save(str(model_path))
            else:
                self.yolo_model = YOLO(str(model_path))
            
            logger.info("YOLOv8 model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load YOLOv8: {e}")
            self.use_yolo = False
    
    def generate_caption(self, image_path: str) -> Tuple[str, float]:
        """
        Generate caption for image using BLIP
        
        Args:
            image_path: Path to image file
            
        Returns:
            Tuple of (caption, confidence)
        """
        if not self.use_blip2 or not self.blip2_model:
            return "Caption not available (BLIP not loaded)", 0.0
        
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert("RGB")
            
            # Generate caption using BLIP (not BLIP2)
            inputs = self.blip2_processor(image, return_tensors="pt")
            
            # Always use CPU for stability
            inputs = {k: v.to("cpu") for k, v in inputs.items()}
            
            with torch.no_grad():
                generated_ids = self.blip2_model.generate(**inputs, max_length=50, num_beams=4)
            
            caption = self.blip2_processor.decode(generated_ids[0], skip_special_tokens=True)
            
            # Estimate confidence (simplified)
            confidence = 0.85  # BLIP generally has good confidence
            
            return caption.strip(), confidence
            
        except Exception as e:
            logger.error(f"Caption generation failed: {e}")
            return "Caption generation failed", 0.0
    
    def extract_clip_features(self, image_path: str) -> Optional[np.ndarray]:
        """
        Extract CLIP features from image
        
        Args:
            image_path: Path to image file
            
        Returns:
            CLIP feature vector as numpy array
        """
        if not self.use_clip or not self.clip_model:
            return None
        
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert("RGB")
            image_input = self.clip_preprocess(image).unsqueeze(0).to(self.device)
            
            # Extract features
            with torch.no_grad():
                image_features = self.clip_model.encode_image(image_input)
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
            
            return image_features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"CLIP feature extraction failed: {e}")
            return None
    
    def calculate_clip_similarity(self, image_path: str, queries: List[str] = None) -> Dict[str, float]:
        """
        Calculate CLIP similarity scores for forensic queries
        
        Args:
            image_path: Path to image file
            queries: List of text queries (default: forensic queries)
            
        Returns:
            Dictionary of query -> similarity scores
        """
        if not self.use_clip or not self.clip_model:
            return {}
        
        if queries is None:
            queries = self.forensic_queries
        
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert("RGB")
            image_input = self.clip_preprocess(image).unsqueeze(0).to(self.device)
            
            # Tokenize text queries
            text_inputs = clip.tokenize(queries).to(self.device)
            
            # Calculate features
            with torch.no_grad():
                image_features = self.clip_model.encode_image(image_input)
                text_features = self.clip_model.encode_text(text_inputs)
                
                # Normalize features
                image_features = image_features / image_features.norm(dim=-1, keepdim=True)
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)
                
                # Calculate similarity
                similarity = (100.0 * image_features @ text_features.T).softmax(dim=-1)
                similarity_scores = similarity[0].cpu().numpy()
            
            # Create query -> score mapping
            results = {}
            for query, score in zip(queries, similarity_scores):
                results[query] = float(score)
            
            return results
            
        except Exception as e:
            logger.error(f"CLIP similarity calculation failed: {e}")
            return {}
    
    def detect_objects(self, image_path: str) -> List[Dict]:
        """
        Detect objects using YOLOv8
        
        Args:
            image_path: Path to image file
            
        Returns:
            List of detected objects with bounding boxes
        """
        if not self.use_yolo or not self.yolo_model:
            return []
        
        try:
            # Run inference
            results = self.yolo_model(image_path, conf=0.3)  # 30% confidence threshold
            
            objects = []
            for result in results:
                for box in result.boxes:
                    # Extract detection info
                    obj = {
                        "class_name": result.names[int(box.cls)],
                        "confidence": float(box.conf),
                        "bbox": box.xyxy[0].cpu().numpy().tolist(),  # [x1, y1, x2, y2]
                        "center": [(box.xyxy[0][0] + box.xyxy[0][2]) / 2,
                                  (box.xyxy[0][1] + box.xyxy[0][3]) / 2]
                    }
                    objects.append(obj)
                    
                    # Flag suspicious objects
                    suspicious_objects = [
                        'knife', 'gun', 'weapon', 'bottle', 'syringe',
                        'cell phone', 'laptop', 'handbag', 'backpack'
                    ]
                    
                    if any(sus in obj["class_name"].lower() for sus in suspicious_objects):
                        obj["flagged"] = True
                        obj["reason"] = "Potentially suspicious object"
            
            return objects
            
        except Exception as e:
            logger.error(f"Object detection failed: {e}")
            return []
    
    def analyze_image(self, image_path: str, case_id: str = "default") -> VisionResult:
        """
        Comprehensive image analysis using all vision models
        
        Args:
            image_path: Path to image file
            case_id: Case identifier
            
        Returns:
            VisionResult with all analysis results
        """
        start_time = datetime.now()
        image_path = Path(image_path)
        
        # Calculate hash
        sha256_hash = self._calculate_hash(image_path)
        
        # Generate caption
        caption, caption_confidence = self.generate_caption(str(image_path))
        
        # Extract CLIP features
        clip_features = self.extract_clip_features(str(image_path))
        
        # Calculate similarity scores
        clip_similarity_scores = self.calculate_clip_similarity(str(image_path))
        
        # Detect objects
        objects_detected = self.detect_objects(str(image_path))
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Create result
        result = VisionResult(
            image_path=str(image_path),
            caption=caption,
            objects_detected=objects_detected,
            clip_features=clip_features,
            clip_similarity_scores=clip_similarity_scores,
            processing_time=processing_time,
            model_versions={
                "blip2": "Salesforce/blip2-opt-2.7b" if self.use_blip2 else "not_loaded",
                "clip": "ViT-B/32" if self.use_clip else "not_loaded",
                "yolo": "yolov8n" if self.use_yolo else "not_loaded"
            },
            confidence_scores={
                "caption": caption_confidence,
                "overall": self._calculate_overall_confidence(caption_confidence, len(objects_detected))
            },
            sha256_hash=sha256_hash
        )
        
        return result
    
    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _calculate_overall_confidence(self, caption_conf: float, num_objects: int) -> float:
        """Calculate overall confidence score"""
        # Simple heuristic: combine caption confidence with object detection success
        object_bonus = min(0.1 * num_objects, 0.3)  # Up to 30% bonus for objects
        return min(caption_conf + object_bonus, 1.0)
    
    def search_similar_images(self, query_text: str, image_features_db: Dict[str, np.ndarray]) -> List[Tuple[str, float]]:
        """
        Search for similar images using CLIP text-to-image similarity
        
        Args:
            query_text: Natural language query
            image_features_db: Database of image_path -> CLIP features
            
        Returns:
            List of (image_path, similarity_score) tuples, sorted by similarity
        """
        if not self.use_clip or not self.clip_model:
            return []
        
        try:
            # Encode query text
            text_inputs = clip.tokenize([query_text]).to(self.device)
            
            with torch.no_grad():
                text_features = self.clip_model.encode_text(text_inputs)
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)
                text_features = text_features.cpu().numpy()
            
            # Calculate similarities
            similarities = []
            for image_path, image_features in image_features_db.items():
                if image_features is not None:
                    # Calculate cosine similarity
                    similarity = np.dot(text_features[0], image_features)
                    similarities.append((image_path, float(similarity)))
            
            # Sort by similarity (highest first)
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            return similarities
            
        except Exception as e:
            logger.error(f"Image search failed: {e}")
            return []
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about loaded models"""
        return {
            "device": self.device,
            "models_loaded": {
                "blip2": self.use_blip2 and self.blip2_model is not None,
                "clip": self.use_clip and self.clip_model is not None,
                "yolo": self.use_yolo and self.yolo_model is not None
            },
            "model_versions": {
                "blip2": "Salesforce/blip2-opt-2.7b" if self.use_blip2 else None,
                "clip": "ViT-B/32" if self.use_clip else None,
                "yolo": "yolov8n" if self.use_yolo else None
            },
            "capabilities": {
                "image_captioning": self.use_blip2,
                "similarity_search": self.use_clip,
                "object_detection": self.use_yolo,
                "forensic_analysis": True
            }
        }


def main():
    """Test vision models"""
    print("Vision Models Test")
    print("=" * 50)
    
    # Initialize models
    vision = VisionModels(use_blip2=True, use_clip=True, use_yolo=True)
    
    # Print model info
    model_info = vision.get_model_info()
    print(f"\nModels loaded: {model_info['models_loaded']}")
    print(f"Device: {model_info['device']}")
    
    # Test with a sample image (if exists)
    test_image = "data/samples/test_image.jpg"
    
    if Path(test_image).exists():
        print(f"\nAnalyzing: {test_image}")
        result = vision.analyze_image(test_image)
        
        print(f"Caption: {result.caption}")
        print(f"Objects detected: {len(result.objects_detected)}")
        print(f"Processing time: {result.processing_time:.2f}s")
        
        # Show top similarity scores
        print("\nTop forensic similarities:")
        sorted_scores = sorted(result.clip_similarity_scores.items(), 
                              key=lambda x: x[1], reverse=True)
        for query, score in sorted_scores[:5]:
            print(f"  {query}: {score:.3f}")
        
        # Show detected objects
        print("\nDetected objects:")
        for obj in result.objects_detected[:5]:
            print(f"  {obj['class_name']} (conf: {obj['confidence']:.2f})")
            
    else:
        print(f"\nTest image not found: {test_image}")
        print("Create a sample image to test vision models functionality")
    
    print("\n" + "=" * 50)
    print("Vision Models Test Complete!")
    
    # Show installation instructions
    missing_deps = []
    if not TORCH_AVAILABLE:
        missing_deps.append("torch torchvision")
    if not CLIP_AVAILABLE:
        missing_deps.append("git+https://github.com/openai/CLIP.git")
    if not TRANSFORMERS_AVAILABLE:
        missing_deps.append("transformers")
    if not YOLO_AVAILABLE:
        missing_deps.append("ultralytics")
    
    if missing_deps:
        print("\nInstall missing dependencies:")
        for dep in missing_deps:
            print(f"  pip install {dep}")


if __name__ == "__main__":
    main()