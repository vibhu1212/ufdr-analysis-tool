"""
Model Manager for Sequential Loading
Handles VRAM constraints by loading/unloading models on demand
Optimized for RTX 3050 (6GB VRAM)
"""

import logging
import torch
import gc
from typing import Dict, Any
from enum import Enum

logger = logging.getLogger(__name__)


class ModelType(Enum):
    """Available model types"""
    YOLO = "yolo"
    CLIP = "clip"
    FACE = "face"
    BLIP = "blip"
    LLAMA = "llama"


class ModelManager:
    """
    Manages model loading/unloading to fit within VRAM constraints
    
    Strategy: Sequential Loading
    - Load only what's needed for current task
    - Unload when done to free VRAM
    - Track which models are loaded
    
    VRAM Budget (RTX 3050 - 6GB):
    - Phase 1 (Image Processing): YOLO + CLIP + Face = 1GB
    - Phase 2 (Descriptions): BLIP = 2GB  
    - Phase 3 (Reports/SQL): Llama = 3-8GB (use smaller model or API)
    """
    
    def __init__(self):
        """Initialize model manager"""
        self.loaded_models: Dict[ModelType, Any] = {}
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        logger.info(f"Model Manager initialized (device: {self.device})")
        
        if self.device == "cuda":
            total_vram = torch.cuda.get_device_properties(0).total_memory / 1e9
            logger.info(f"   Total VRAM: {total_vram:.1f}GB")
    
    def load_visual_models(self) -> Dict[str, Any]:
        """
        Load models for visual analysis (Phase 1)
        YOLO + CLIP + Face = ~1GB VRAM
        
        Returns:
            Dict with loaded model instances
        """
        logger.info("Loading visual analysis models...")
        
        models = {}
        
        try:
            # Load YOLO
            from media.yolo_detector import get_yolo_detector
            models['yolo'] = get_yolo_detector()
            self.loaded_models[ModelType.YOLO] = models['yolo']
            
            # Load CLIP
            from media.clip_embedder import get_clip_embedder
            models['clip'] = get_clip_embedder()
            self.loaded_models[ModelType.CLIP] = models['clip']
            
            # Load Face Recognition
            from media.face_recognizer import get_face_recognizer
            models['face'] = get_face_recognizer()
            self.loaded_models[ModelType.FACE] = models['face']
            
            logger.info("✅ Visual models loaded (YOLO + CLIP + Face)")
            self._log_vram_usage()
            
            return models
            
        except Exception as e:
            logger.error(f"Failed to load visual models: {e}")
            raise
    
    def load_blip(self) -> Any:
        """
        Load BLIP for image descriptions (Phase 2)
        BLIP-large = ~2GB VRAM
        
        Note: Unload visual models first if needed
        
        Returns:
            BLIP analyzer instance
        """
        logger.info("Loading BLIP analyzer...")
        
        try:
            from media.blip2_analyzer import get_blip2_analyzer
            blip = get_blip2_analyzer()
            self.loaded_models[ModelType.BLIP] = blip
            
            logger.info("✅ BLIP loaded")
            self._log_vram_usage()
            
            return blip
            
        except Exception as e:
            logger.error(f"Failed to load BLIP: {e}")
            raise
    
    def load_llama(self, model_size: str = "3b") -> Any:
        """
        Load Llama for report generation / SQL queries (Phase 3)
        
        Args:
            model_size: "1b", "3b", or "8b" (larger = better quality, more VRAM)
                - 1B: ~1GB VRAM
                - 3B: ~3GB VRAM  
                - 8B: ~8GB VRAM (won't fit, use API instead)
        
        Returns:
            Llama model instance or API client
        """
        logger.info(f"Loading Llama {model_size} for report generation...")
        
        # For now, suggest using Ollama or API
        logger.warning("Llama loading not implemented yet")
        logger.info("   Recommendation: Use Ollama (CPU) or OpenAI API")
        
        return None
    
    def unload_visual_models(self):
        """
        Unload YOLO + CLIP + Face to free VRAM
        Call this before loading BLIP or Llama
        """
        logger.info("Unloading visual models...")
        
        # Remove references
        for model_type in [ModelType.YOLO, ModelType.CLIP, ModelType.FACE]:
            if model_type in self.loaded_models:
                del self.loaded_models[model_type]
        
        # Force garbage collection
        gc.collect()
        
        # Clear GPU cache
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        logger.info("✅ Visual models unloaded")
        self._log_vram_usage()
    
    def unload_blip(self):
        """Unload BLIP to free VRAM"""
        logger.info("Unloading BLIP...")
        
        if ModelType.BLIP in self.loaded_models:
            del self.loaded_models[ModelType.BLIP]
        
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        logger.info("✅ BLIP unloaded")
        self._log_vram_usage()
    
    def unload_llama(self):
        """Unload Llama to free VRAM"""
        logger.info("Unloading Llama...")
        
        if ModelType.LLAMA in self.loaded_models:
            del self.loaded_models[ModelType.LLAMA]
        
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        logger.info("✅ Llama unloaded")
        self._log_vram_usage()
    
    def unload_all(self):
        """Unload all models"""
        logger.info("Unloading all models...")
        
        self.loaded_models.clear()
        gc.collect()
        
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        
        logger.info("✅ All models unloaded")
        self._log_vram_usage()
    
    def get_loaded_models(self) -> list[ModelType]:
        """Get list of currently loaded models"""
        return list(self.loaded_models.keys())
    
    def is_loaded(self, model_type: ModelType) -> bool:
        """Check if a specific model is loaded"""
        return model_type in self.loaded_models
    
    def _log_vram_usage(self):
        """Log current VRAM usage"""
        if torch.cuda.is_available():
            allocated = torch.cuda.memory_allocated(0) / 1e9
            reserved = torch.cuda.memory_reserved(0) / 1e9
            logger.info(f"   VRAM: {allocated:.2f}GB allocated, {reserved:.2f}GB reserved")


# Global instance
_manager_instance = None


def get_model_manager() -> ModelManager:
    """Get singleton model manager"""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = ModelManager()
    return _manager_instance


# Convenience functions for common workflows

def process_images_sequential(image_paths: list, case_id: str) -> list:
    """
    Process images with sequential model loading
    
    Workflow:
    1. Load visual models (YOLO + CLIP + Face)
    2. Process all images - extract features
    3. Unload visual models
    4. Load BLIP
    5. Generate descriptions for all images
    6. Unload BLIP
    
    Args:
        image_paths: List of image paths to process
        case_id: Case identifier
    
    Returns:
        List of complete analysis results
    """
    manager = get_model_manager()
    
    logger.info(f"Sequential processing: {len(image_paths)} images")
    
    # Phase 1: Visual Analysis
    logger.info("\n=== PHASE 1: Visual Analysis ===")
    manager.load_visual_models()
    
    from media.enhanced_image_processor import EnhancedImageProcessor
    processor = EnhancedImageProcessor(
        auto_index=True,
        enable_yolo=True,
        enable_faces=True,
        enable_clip=True
    )
    
    visual_results = processor.process_images_batch(image_paths, case_id)
    
    # Unload to free VRAM
    manager.unload_visual_models()
    
    # Phase 2: Description Generation
    logger.info("\n=== PHASE 2: Description Generation ===")
    blip = manager.load_blip()
    
    for result in visual_results:
        if result.success:
            try:
                description = blip.generate_caption(result.file_path)
                result.metadata['blip_description'] = description
                logger.info(f"   {Path(result.file_path).name}: {description}")
            except Exception as e:
                logger.error(f"Failed to generate description: {e}")
    
    # Unload BLIP
    manager.unload_blip()
    
    logger.info("\n✅ Sequential processing complete")
    
    return visual_results


def generate_case_report(case_id: str, results: list) -> str:
    """
    Generate case report using Llama
    
    Workflow:
    1. Ensure all other models unloaded
    2. Load Llama (or use API)
    3. Generate report
    4. Unload Llama
    
    Args:
        case_id: Case identifier
        results: Analysis results from images
    
    Returns:
        Generated report text
    """
    manager = get_model_manager()
    
    logger.info("Generating case report...")
    
    # Unload everything first
    manager.unload_all()
    
    # TODO: Implement Llama report generation
    # For now, return template-based report
    
    report = f"Case Report: {case_id}\n"
    report += f"Total images: {len(results)}\n"
    report += f"Successfully processed: {sum(1 for r in results if r.success)}\n"
    
    return report
