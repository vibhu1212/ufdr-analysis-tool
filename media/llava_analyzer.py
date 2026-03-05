"""
BLIP-2 Image Analyzer
Provides detailed image analysis using BLIP-2 (Salesforce)
Generates captions, detects objects, and understands scenes
Optimized for RTX 3050 (6GB VRAM)
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
from PIL import Image
import torch
from transformers import LlavaNextProcessor, LlavaNextForConditionalGeneration

logger = logging.getLogger(__name__)


class LLaVAAnalyzer:
    """
    LLaVA-based image analyzer for forensic analysis
    Provides detailed descriptions, object detection, and scene understanding
    """
    
    def __init__(
        self,
        model_name: str = "llava-hf/llava-v1.6-mistral-7b-hf",
        device: Optional[str] = None,
        load_in_8bit: bool = True
    ):
        """
        Initialize LLaVA analyzer
        
        Args:
            model_name: HuggingFace model ID (llava-v1.6-mistral-7b is good balance)
            device: Device to run on ('cuda', 'cpu', or None for auto)
            load_in_8bit: Load model in 8-bit quantization (saves memory)
        """
        self.model_name = model_name
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.load_in_8bit = load_in_8bit
        
        self.processor = None
        self.model = None
        
        logger.info(f"LLaVA Analyzer initialized (device: {self.device})")
    
    def _load_model(self):
        """Lazy load the LLaVA model"""
        if self.model is not None:
            return
        
        logger.info(f"Loading LLaVA model: {self.model_name}")
        
        try:
            # Load processor
            self.processor = LlavaNextProcessor.from_pretrained(self.model_name)
            
            # Load model with optional quantization
            if self.load_in_8bit and self.device == "cuda":
                logger.info("Loading model in 8-bit quantization mode")
                self.model = LlavaNextForConditionalGeneration.from_pretrained(
                    self.model_name,
                    torch_dtype=torch.float16,
                    load_in_8bit=True,
                    device_map="auto"
                )
            else:
                self.model = LlavaNextForConditionalGeneration.from_pretrained(
                    self.model_name,
                    torch_dtype=torch.float16 if self.device == "cuda" else torch.float32,
                    device_map=self.device
                )
            
            logger.info("✅ LLaVA model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load LLaVA model: {e}")
            raise
    
    def analyze_image(
        self,
        image_path: Union[str, Path],
        prompt: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Analyze a single image with LLaVA
        
        Args:
            image_path: Path to image file
            prompt: Custom prompt (uses forensic default if None)
        
        Returns:
            Dictionary with analysis results
        """
        self._load_model()
        
        try:
            # Load image
            image = Image.open(image_path).convert("RGB")
            
            # Use forensic-focused prompt if none provided
            if prompt is None:
                prompt = self._get_forensic_prompt()
            
            # Prepare inputs
            conversation = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image"},
                    ],
                },
            ]
            
            prompt_text = self.processor.apply_chat_template(
                conversation, add_generation_prompt=True
            )
            
            inputs = self.processor(
                images=image,
                text=prompt_text,
                return_tensors="pt"
            ).to(self.device)
            
            # Generate description
            with torch.no_grad():
                output = self.model.generate(
                    **inputs,
                    max_new_tokens=256,
                    do_sample=False
                )
            
            # Decode response
            response = self.processor.decode(
                output[0][inputs['input_ids'].shape[1]:],
                skip_special_tokens=True
            ).strip()
            
            # Parse the response
            analysis = self._parse_response(response)
            analysis["raw_description"] = response
            
            logger.debug(f"Analyzed image: {Path(image_path).name}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze image {image_path}: {e}")
            return {
                "description": "",
                "objects": [],
                "people": [],
                "scene_type": "unknown",
                "raw_description": "",
                "error": str(e)
            }
    
    def batch_analyze(
        self,
        image_paths: List[Union[str, Path]],
        prompt: Optional[str] = None
    ) -> List[Dict[str, any]]:
        """
        Analyze multiple images
        
        Args:
            image_paths: List of image paths
            prompt: Custom prompt for all images
        
        Returns:
            List of analysis results
        """
        results = []
        
        logger.info(f"Analyzing {len(image_paths)} images with LLaVA...")
        
        for i, image_path in enumerate(image_paths):
            logger.debug(f"Processing image {i+1}/{len(image_paths)}: {Path(image_path).name}")
            result = self.analyze_image(image_path, prompt)
            results.append(result)
        
        logger.info(f"✅ Analyzed {len(results)} images")
        
        return results
    
    def generate_caption(
        self,
        image_path: Union[str, Path],
        style: str = "detailed"
    ) -> str:
        """
        Generate a caption for an image
        
        Args:
            image_path: Path to image
            style: Caption style ('brief', 'detailed', 'forensic')
        
        Returns:
            Generated caption
        """
        prompts = {
            "brief": "Describe this image in one sentence.",
            "detailed": "Describe this image in detail.",
            "forensic": "Describe this image as if for a forensic investigation. Include all visible people, objects, locations, and activities."
        }
        
        prompt = prompts.get(style, prompts["detailed"])
        result = self.analyze_image(image_path, prompt)
        
        return result.get("raw_description", "")
    
    def detect_objects(
        self,
        image_path: Union[str, Path],
        object_types: Optional[List[str]] = None
    ) -> List[str]:
        """
        Detect specific objects in an image
        
        Args:
            image_path: Path to image
            object_types: Specific objects to look for (None = all)
        
        Returns:
            List of detected objects
        """
        if object_types:
            object_list = ", ".join(object_types)
            prompt = f"List all visible objects in this image, specifically looking for: {object_list}. Format: object1, object2, object3"
        else:
            prompt = "List all visible objects in this image. Format: object1, object2, object3"
        
        result = self.analyze_image(image_path, prompt)
        return result.get("objects", [])
    
    def detect_people(
        self,
        image_path: Union[str, Path]
    ) -> Dict[str, any]:
        """
        Detect and describe people in an image
        
        Args:
            image_path: Path to image
        
        Returns:
            Dictionary with people count and descriptions
        """
        prompt = """Analyze this image for people:
1. How many people are visible?
2. Describe each person (gender, approximate age, clothing, activity)
3. What are they doing?

Format your response as:
People count: [number]
Descriptions: [person 1], [person 2], etc."""
        
        result = self.analyze_image(image_path, prompt)
        
        return {
            "count": len(result.get("people", [])),
            "descriptions": result.get("people", []),
            "raw_analysis": result.get("raw_description", "")
        }
    
    def analyze_scene(
        self,
        image_path: Union[str, Path]
    ) -> Dict[str, any]:
        """
        Analyze the scene/context of an image
        
        Args:
            image_path: Path to image
        
        Returns:
            Scene analysis including type, location, time, mood
        """
        prompt = """Analyze this scene:
1. Scene type (indoor/outdoor, location type)
2. Time of day (if determinable)
3. Weather conditions (if outdoor)
4. Overall mood/atmosphere
5. Any suspicious or notable elements"""
        
        result = self.analyze_image(image_path, prompt)
        
        return {
            "scene_type": result.get("scene_type", "unknown"),
            "analysis": result.get("raw_description", "")
        }
    
    def _get_forensic_prompt(self) -> str:
        """Get the default forensic analysis prompt"""
        return """Analyze this image for forensic purposes. Provide:
1. A detailed description of what you see
2. All visible people (count, descriptions)
3. All visible objects
4. Scene type and location
5. Any weapons, suspicious items, or notable details

Be precise and objective."""
    
    def _parse_response(self, response: str) -> Dict[str, any]:
        """
        Parse LLaVA response into structured data
        
        Args:
            response: Raw text response from LLaVA
        
        Returns:
            Structured analysis dictionary
        """
        analysis = {
            "description": response,
            "objects": [],
            "people": [],
            "scene_type": "unknown"
        }
        
        # Extract key information using simple parsing
        response_lower = response.lower()
        
        # Detect scene type
        if "indoor" in response_lower:
            analysis["scene_type"] = "indoor"
        elif "outdoor" in response_lower:
            analysis["scene_type"] = "outdoor"
        
        # Try to extract people mentions
        people_keywords = ["person", "people", "man", "woman", "child", "individual"]
        for keyword in people_keywords:
            if keyword in response_lower:
                # Found people mention
                sentences = response.split(".")
                for sentence in sentences:
                    if keyword in sentence.lower():
                        analysis["people"].append(sentence.strip())
        
        # Try to extract objects (look for common nouns)
        # This is a simple heuristic - the description itself is more valuable
        common_objects = [
            "car", "vehicle", "gun", "weapon", "knife", "phone", "document",
            "building", "street", "tree", "table", "chair", "bag", "bottle"
        ]
        
        for obj in common_objects:
            if obj in response_lower:
                analysis["objects"].append(obj)
        
        return analysis
    
    def __del__(self):
        """Cleanup resources"""
        if self.model is not None:
            del self.model
            del self.processor
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


# Convenience functions
def analyze_image(image_path: Union[str, Path]) -> Dict[str, any]:
    """Analyze a single image (creates analyzer instance)"""
    analyzer = LLaVAAnalyzer()
    return analyzer.analyze_image(image_path)


def generate_caption(image_path: Union[str, Path], style: str = "detailed") -> str:
    """Generate a caption for an image"""
    analyzer = LLaVAAnalyzer()
    return analyzer.generate_caption(image_path, style)
