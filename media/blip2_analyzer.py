"""
BLIP-2 Image Analyzer
Provides detailed image analysis using BLIP-2 (Salesforce)
Generates captions, visual question answering, and scene understanding
Optimized for RTX 3050 (6GB VRAM)
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
from PIL import Image
import torch
from transformers import BlipProcessor, BlipForConditionalGeneration

logger = logging.getLogger(__name__)


class BLIP2Analyzer:
    """
    BLIP-2 based image analyzer for forensic analysis
    Provides detailed descriptions, captions, and visual question answering
    """
    
    def __init__(
        self,
        model_name: str = "Salesforce/blip-image-captioning-base",
        device: Optional[str] = None,
        use_fp16: bool = True
    ):
        """
        Initialize BLIP analyzer (lightweight version)
        
        Args:
            model_name: HuggingFace model ID
                - "Salesforce/blip-image-captioning-base" (~990MB) - RECOMMENDED for RTX 3050
                - "Salesforce/blip-image-captioning-large" (~12GB download) - Better quality but huge
            device: Device to run on ('cuda', 'cpu', or None for auto)
            use_fp16: Use FP16 precision to save memory (recommended for GPU)
        """
        self.model_name = model_name
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.use_fp16 = use_fp16 and self.device == "cuda"
        
        self.processor = None
        self.model = None
        
        logger.info(f"BLIP-2 Analyzer initialized (device: {self.device})")
    
    def _load_model(self):
        """Lazy load the BLIP-2 model"""
        if self.model is not None:
            return
        
        logger.info(f"Loading BLIP model: {self.model_name}")
        logger.info(f"   This may take a moment (downloading ~1GB if first time)...")
        
        try:
            # Load processor
            self.processor = BlipProcessor.from_pretrained(self.model_name)
            
            # Load model (using safetensors for security)
            if self.use_fp16:
                logger.info("   Using FP16 precision for memory efficiency")
                self.model = BlipForConditionalGeneration.from_pretrained(
                    self.model_name,
                    torch_dtype=torch.float16,
                    use_safetensors=True  # Use safetensors format for security
                ).to(self.device)
            else:
                self.model = BlipForConditionalGeneration.from_pretrained(
                    self.model_name,
                    use_safetensors=True  # Use safetensors format for security
                ).to(self.device)
            
            logger.info("✅ BLIP model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load BLIP-2 model: {e}")
            raise
    
    def generate_caption(
        self,
        image_path: Union[str, Path],
        prompt: Optional[str] = None,
        max_length: int = 50
    ) -> str:
        """
        Generate a caption for an image
        
        Args:
            image_path: Path to image file
            prompt: Optional prompt prefix (e.g., "Question: what is in the image? Answer:")
            max_length: Maximum caption length
        
        Returns:
            Generated caption string
        """
        self._load_model()
        
        try:
            # Load image
            image = Image.open(image_path).convert("RGB")
            
            # Prepare inputs
            if prompt:
                inputs = self.processor(image, text=prompt, return_tensors="pt").to(
                    self.device, torch.float16 if self.use_fp16 else torch.float32
                )
            else:
                inputs = self.processor(image, return_tensors="pt").to(
                    self.device, torch.float16 if self.use_fp16 else torch.float32
                )
            
            # Generate caption
            with torch.no_grad():
                generated_ids = self.model.generate(
                    **inputs,
                    max_length=max_length,
                    num_beams=5,
                    early_stopping=True
                )
            
            # Decode caption
            caption = self.processor.batch_decode(
                generated_ids, skip_special_tokens=True
            )[0].strip()
            
            logger.debug(f"Generated caption for {Path(image_path).name}")
            
            return caption
            
        except Exception as e:
            logger.error(f"Failed to generate caption for {image_path}: {e}")
            return ""
    
    def answer_question(
        self,
        image_path: Union[str, Path],
        question: str,
        max_length: int = 30
    ) -> str:
        """
        Visual Question Answering - ask a question about the image
        
        Args:
            image_path: Path to image
            question: Question to ask (e.g., "What is the person doing?")
            max_length: Maximum answer length
        
        Returns:
            Answer string
        """
        # Format question as prompt
        prompt = f"Question: {question} Answer:"
        
        return self.generate_caption(image_path, prompt=prompt, max_length=max_length)
    
    def analyze_image(
        self,
        image_path: Union[str, Path],
        style: str = "forensic"
    ) -> Dict[str, any]:
        """
        Comprehensive image analysis
        
        Args:
            image_path: Path to image file
            style: Analysis style ('brief', 'detailed', 'forensic')
        
        Returns:
            Dictionary with analysis results
        """
        try:
            image_path = Path(image_path)
            
            # Generate main description (BLIP works best without prompts)
            if style == "brief":
                description = self.generate_caption(image_path, max_length=30)
            elif style == "detailed":
                description = self.generate_caption(image_path, max_length=75)
            else:  # forensic - use longer caption for more detail
                description = self.generate_caption(image_path, max_length=100)
            
            # Extract structured information
            analysis = {
                "description": description,
                "scene_type": self._detect_scene_type(description),
                "objects": self._extract_objects(description),
                "people": self._detect_people(image_path),
                "raw_description": description
            }
            
            logger.debug(f"Analyzed {image_path.name}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze {image_path}: {e}")
            return {
                "description": "",
                "scene_type": "unknown",
                "objects": [],
                "people": [],
                "raw_description": "",
                "error": str(e)
            }
    
    def batch_analyze(
        self,
        image_paths: List[Union[str, Path]],
        style: str = "forensic"
    ) -> List[Dict[str, any]]:
        """
        Analyze multiple images
        
        Args:
            image_paths: List of image paths
            style: Analysis style
        
        Returns:
            List of analysis results
        """
        results = []
        
        logger.info(f"Analyzing {len(image_paths)} images with BLIP-2...")
        
        for i, image_path in enumerate(image_paths, 1):
            logger.info(f"[{i}/{len(image_paths)}] {Path(image_path).name}")
            result = self.analyze_image(image_path, style)
            results.append(result)
        
        logger.info(f"✅ Analyzed {len(results)} images")
        
        return results
    
    def detect_objects(
        self,
        image_path: Union[str, Path],
        object_types: Optional[List[str]] = None
    ) -> List[str]:
        """
        Detect specific objects in an image using VQA
        
        Args:
            image_path: Path to image
            object_types: Specific objects to look for (None = general detection)
        
        Returns:
            List of detected objects
        """
        if object_types:
            # Ask about specific objects
            objects_str = ", ".join(object_types)
            question = f"Are there any of these visible: {objects_str}?"
            answer = self.answer_question(image_path, question)
            
            # Parse answer for object names
            detected = []
            for obj in object_types:
                if obj.lower() in answer.lower():
                    detected.append(obj)
            return detected
        else:
            # General object detection
            question = "What objects are visible in this image?"
            answer = self.answer_question(image_path, question, max_length=50)
            return self._extract_objects(answer)
    
    def detect_people(
        self,
        image_path: Union[str, Path]
    ) -> Dict[str, any]:
        """
        Detect and describe people in the image
        
        Args:
            image_path: Path to image
        
        Returns:
            Dictionary with people count and descriptions
        """
        # Count people
        count_answer = self.answer_question(
            image_path,
            "How many people are in this image?"
        )
        
        # Get description of people
        desc_answer = self.answer_question(
            image_path,
            "Describe the people in this image.",
            max_length=100
        )
        
        return {
            "count_text": count_answer,
            "description": desc_answer,
            "raw_analysis": desc_answer
        }
    
    def _detect_people(self, image_path: Union[str, Path]) -> List[str]:
        """Helper to detect people for analyze_image"""
        result = self.detect_people(image_path)
        desc = result.get("description", "")
        
        if "no people" in desc.lower() or "no one" in desc.lower():
            return []
        elif desc:
            return [desc]
        else:
            return []
    
    def _detect_scene_type(self, description: str) -> str:
        """Extract scene type from description"""
        desc_lower = description.lower()
        
        if any(word in desc_lower for word in ["indoor", "inside", "room", "building interior"]):
            return "indoor"
        elif any(word in desc_lower for word in ["outdoor", "outside", "street", "park", "landscape"]):
            return "outdoor"
        else:
            return "unknown"
    
    def _extract_objects(self, text: str) -> List[str]:
        """Extract object mentions from text"""
        # Common forensic objects
        forensic_objects = [
            "weapon", "gun", "knife", "firearm", "blade",
            "car", "vehicle", "truck", "motorcycle", "bike",
            "person", "people", "man", "woman", "child",
            "phone", "smartphone", "mobile",
            "document", "paper", "id", "card",
            "bag", "backpack", "suitcase",
            "building", "house", "structure",
            "tree", "plant", "vegetation",
            "sign", "signage", "billboard",
            "blood", "stain", "evidence"
        ]
        
        text_lower = text.lower()
        detected = []
        
        for obj in forensic_objects:
            if obj in text_lower:
                detected.append(obj)
        
        return list(set(detected))  # Remove duplicates
    
    def __del__(self):
        """Cleanup resources"""
        if self.model is not None:
            del self.model
            del self.processor
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


# Convenience functions
def analyze_image(image_path: Union[str, Path], style: str = "forensic") -> Dict[str, any]:
    """Analyze a single image (creates analyzer instance)"""
    analyzer = BLIP2Analyzer()
    return analyzer.analyze_image(image_path, style)


def generate_caption(image_path: Union[str, Path]) -> str:
    """Generate a caption for an image"""
    analyzer = BLIP2Analyzer()
    return analyzer.generate_caption(image_path)


def answer_question(image_path: Union[str, Path], question: str) -> str:
    """Answer a question about an image"""
    analyzer = BLIP2Analyzer()
    return analyzer.answer_question(image_path, question)


# Singleton instance for efficiency
_global_analyzer = None

def get_blip2_analyzer() -> BLIP2Analyzer:
    """Get singleton BLIP-2 analyzer instance"""
    global _global_analyzer
    if _global_analyzer is None:
        _global_analyzer = BLIP2Analyzer()
    return _global_analyzer
