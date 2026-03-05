"""
YOLO-World Object Detector
Open-vocabulary object detection for forensic image analysis
Detects ANY object via text prompts (not limited to 80 COCO classes)
Forensic-specific: weapons, drugs, evidence, contraband, and more
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

try:
    from ultralytics import YOLOWorld
    import torch
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False

logger = logging.getLogger(__name__)


# COCO dataset class names (80 classes)
COCO_CLASSES = [
    'person', 'bicycle', 'car', 'motorcycle', 'airplane', 'bus', 'train', 'truck', 'boat',
    'traffic light', 'fire hydrant', 'stop sign', 'parking meter', 'bench', 'bird', 'cat',
    'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear', 'zebra', 'giraffe', 'backpack',
    'umbrella', 'handbag', 'tie', 'suitcase', 'frisbee', 'skis', 'snowboard', 'sports ball',
    'kite', 'baseball bat', 'baseball glove', 'skateboard', 'surfboard', 'tennis racket',
    'bottle', 'wine glass', 'cup', 'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple',
    'sandwich', 'orange', 'broccoli', 'carrot', 'hot dog', 'pizza', 'donut', 'cake', 'chair',
    'couch', 'potted plant', 'bed', 'dining table', 'toilet', 'tv', 'laptop', 'mouse',
    'remote', 'keyboard', 'cell phone', 'microwave', 'oven', 'toaster', 'sink', 'refrigerator',
    'book', 'clock', 'vase', 'scissors', 'teddy bear', 'hair drier', 'toothbrush'
]

# Forensically interesting categories
FORENSIC_CATEGORIES = {
    'people': ['person'],
    'vehicles': ['bicycle', 'car', 'motorcycle', 'bus', 'train', 'truck', 'boat', 'airplane'],
    'weapons': ['knife', 'scissors', 'baseball bat'],  # Limited in COCO
    'electronics': ['cell phone', 'laptop', 'tv', 'remote', 'keyboard', 'mouse'],
    'bags': ['backpack', 'handbag', 'suitcase'],
    'furniture': ['chair', 'couch', 'bed', 'dining table']
}


class YOLODetector:
    """
    YOLO-World/YOLOv8-based object detector for forensic analysis
    Supports both fixed 80-class COCO and open-vocabulary detection
    """
    
    def __init__(
        self,
        model_type: str = "yolov8",  # "yolov8" or "yolo-world"
        model_size: str = "n",  # n, s, m, l, x (nano to extra-large)
        device: Optional[str] = None,
        confidence_threshold: float = 0.25,
        custom_classes: Optional[List[str]] = None
    ):
        """
        Initialize YOLO detector
        
        Args:
            model_size: Model size (n=fastest, x=most accurate)
                - yolov8n: Fastest, ~3ms
                - yolov8s: Fast, ~5ms
                - yolov8m: Medium, ~10ms
                - yolov8l: Large, ~15ms
                - yolov8x: Extra large, ~20ms
            device: Device ('cuda', 'cpu', or None for auto)
            confidence_threshold: Minimum confidence for detections
        """
        if not YOLO_AVAILABLE:
            raise ImportError("ultralytics not installed. Run: pip install ultralytics")
        
        self.model_size = model_size
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.confidence_threshold = confidence_threshold
        
        self.model = None
        
        logger.info(f"YOLO Detector initialized (model={model_size}, device={self.device})")
    
    def _load_model(self):
        """Lazy load the YOLO-World model"""
        if self.model is not None:
            return
        
        # Use YOLO-World for open-vocabulary detection
        # Model sizes: s (small), m (medium), l (large)
        # Note: YOLO-World doesn't have 'n' (nano) size, use 's' as smallest
        if self.model_size == 'n':
            self.model_size = 's'  # YOLO-World smallest is 's'
        
        model_name = f"yolov8{self.model_size}-world.pt"
        logger.info(f"Loading YOLO-World model: {model_name}")
        logger.info("Open-vocabulary detection enabled!")
        
        try:
            self.model = YOLOWorld(model_name)
            # Set default COCO classes (can be customized later)
            self.model.set_classes(COCO_CLASSES)
            logger.info(f"✅ YOLO-World model loaded successfully")
            logger.info(f"   Device: {self.device}")
            logger.info(f"   Classes: {len(COCO_CLASSES)} (customizable)")
        except Exception as e:
            logger.error(f"Failed to load YOLO-World model: {e}")
            raise
    
    def detect(
        self,
        image_path: Union[str, Path],
        conf_threshold: Optional[float] = None
    ) -> Dict[str, any]:
        """
        Detect objects in an image
        
        Args:
            image_path: Path to image file
            conf_threshold: Override confidence threshold
        
        Returns:
            Detection results with objects, counts, and bounding boxes
        """
        self._load_model()
        
        conf = conf_threshold or self.confidence_threshold
        
        try:
            # Run inference
            results = self.model(image_path, conf=conf, verbose=False)[0]
            
            # Parse results
            detections = self._parse_results(results)
            
            logger.debug(f"Detected {len(detections['objects'])} objects in {Path(image_path).name}")
            
            return detections
            
        except Exception as e:
            logger.error(f"Failed to detect objects in {image_path}: {e}")
            return {
                'objects': {},
                'total_objects': 0,
                'raw_detections': [],
                'error': str(e)
            }
    
    def batch_detect(
        self,
        image_paths: List[Union[str, Path]],
        conf_threshold: Optional[float] = None
    ) -> List[Dict[str, any]]:
        """
        Detect objects in multiple images (batched for speed)
        
        Args:
            image_paths: List of image paths
            conf_threshold: Override confidence threshold
        
        Returns:
            List of detection results
        """
        self._load_model()
        
        conf = conf_threshold or self.confidence_threshold
        
        logger.info(f"Running YOLO detection on {len(image_paths)} images...")
        
        try:
            # Batch inference (faster than one-by-one)
            results = self.model(image_paths, conf=conf, verbose=False)
            
            # Parse all results
            all_detections = []
            for result in results:
                detections = self._parse_results(result)
                all_detections.append(detections)
            
            logger.info(f"✅ Completed YOLO detection on {len(image_paths)} images")
            
            return all_detections
            
        except Exception as e:
            logger.error(f"Failed batch detection: {e}")
            return [{'objects': {}, 'total_objects': 0, 'error': str(e)} for _ in image_paths]
    
    def _parse_results(self, result) -> Dict[str, any]:
        """Parse YOLO results into structured format"""
        
        # Extract boxes, classes, and confidences
        boxes = result.boxes
        
        if boxes is None or len(boxes) == 0:
            return {
                'objects': {},
                'total_objects': 0,
                'raw_detections': [],
                'forensic_summary': self._summarize_forensic({})
            }
        
        # Group detections by class
        objects = {}
        raw_detections = []
        
        for box in boxes:
            # Get class and confidence
            cls_id = int(box.cls[0])
            confidence = float(box.conf[0])
            class_name = COCO_CLASSES[cls_id] if cls_id < len(COCO_CLASSES) else "unknown"
            
            # Get bounding box coordinates
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            bbox = {
                'x1': round(x1, 2),
                'y1': round(y1, 2),
                'x2': round(x2, 2),
                'y2': round(y2, 2),
                'width': round(x2 - x1, 2),
                'height': round(y2 - y1, 2)
            }
            
            # Add to objects dictionary
            if class_name not in objects:
                objects[class_name] = {
                    'count': 0,
                    'confidences': [],
                    'bboxes': []
                }
            
            objects[class_name]['count'] += 1
            objects[class_name]['confidences'].append(round(confidence, 3))
            objects[class_name]['bboxes'].append(bbox)
            
            # Add to raw detections
            raw_detections.append({
                'class': class_name,
                'confidence': round(confidence, 3),
                'bbox': bbox
            })
        
        return {
            'objects': objects,
            'total_objects': len(raw_detections),
            'raw_detections': raw_detections,
            'forensic_summary': self._summarize_forensic(objects)
        }
    
    def _summarize_forensic(self, objects: Dict) -> Dict[str, any]:
        """Summarize detections by forensic categories"""
        summary = {
            'has_people': 'person' in objects,
            'person_count': objects.get('person', {}).get('count', 0),
            'has_vehicles': False,
            'vehicle_types': [],
            'has_weapons': False,
            'weapon_types': [],
            'has_electronics': False,
            'electronic_types': [],
            'has_bags': False,
            'bag_types': []
        }
        
        # Check for vehicles
        for vehicle_type in FORENSIC_CATEGORIES['vehicles']:
            if vehicle_type in objects:
                summary['has_vehicles'] = True
                summary['vehicle_types'].append(vehicle_type)
        
        # Check for weapons
        for weapon_type in FORENSIC_CATEGORIES['weapons']:
            if weapon_type in objects:
                summary['has_weapons'] = True
                summary['weapon_types'].append(weapon_type)
        
        # Check for electronics
        for elec_type in FORENSIC_CATEGORIES['electronics']:
            if elec_type in objects:
                summary['has_electronics'] = True
                summary['electronic_types'].append(elec_type)
        
        # Check for bags
        for bag_type in FORENSIC_CATEGORIES['bags']:
            if bag_type in objects:
                summary['has_bags'] = True
                summary['bag_types'].append(bag_type)
        
        return summary
    
    def generate_text_description(self, detections: Dict[str, any]) -> str:
        """
        Generate natural language description of detections
        (For indexing in text search)
        """
        objects = detections.get('objects', {})
        
        if not objects:
            return "No objects detected in this image."
        
        description_parts = []
        
        # Add object counts
        for obj_class, obj_data in sorted(objects.items(), key=lambda x: x[1]['count'], reverse=True):
            count = obj_data['count']
            if count == 1:
                description_parts.append(f"one {obj_class}")
            else:
                description_parts.append(f"{count} {obj_class}s")
        
        # Build description
        if len(description_parts) == 1:
            description = f"This image contains {description_parts[0]}."
        elif len(description_parts) == 2:
            description = f"This image contains {description_parts[0]} and {description_parts[1]}."
        else:
            description = f"This image contains {', '.join(description_parts[:-1])}, and {description_parts[-1]}."
        
        # Add forensic highlights
        forensic = detections.get('forensic_summary', {})
        highlights = []
        
        if forensic.get('person_count', 0) > 0:
            highlights.append(f"{forensic['person_count']} person(s)")
        
        if forensic.get('has_vehicles'):
            highlights.append(f"vehicles ({', '.join(forensic['vehicle_types'])})")
        
        if forensic.get('has_weapons'):
            highlights.append(f"weapons ({', '.join(forensic['weapon_types'])})")
        
        if highlights:
            description += f" Notable: {', '.join(highlights)}."
        
        return description
    
    def __del__(self):
        """Cleanup"""
        if self.model is not None:
            del self.model
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


# Singleton instance
_yolo_detector = None

def get_yolo_detector(model_size: str = "n") -> YOLODetector:
    """Get or create YOLO detector singleton"""
    global _yolo_detector
    if _yolo_detector is None:
        _yolo_detector = YOLODetector(model_size=model_size)
    return _yolo_detector


# Convenience functions
def detect_objects(image_path: Union[str, Path]) -> Dict[str, any]:
    """Detect objects in a single image"""
    detector = get_yolo_detector()
    return detector.detect(image_path)


def batch_detect_objects(image_paths: List[Union[str, Path]]) -> List[Dict[str, any]]:
    """Detect objects in multiple images"""
    detector = get_yolo_detector()
    return detector.batch_detect(image_paths)
