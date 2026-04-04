"""
Video Processing Pipeline
Extracts keyframes from videos and applies image analysis pipeline
Uses FFmpeg for video processing and analysis
"""

import logging
import hashlib
import json
import subprocess
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass, asdict, field
from PIL import Image

# Try to import ML libraries
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("Warning: ultralytics not installed. Object detection limited.")

try:
    from sentence_transformers import SentenceTransformer
    CLIP_AVAILABLE = True
except ImportError:
    CLIP_AVAILABLE = False
    print("Warning: sentence-transformers not installed. Image embedding limited.")

# Try to import video processing libraries
try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("Warning: opencv-python not installed. Video processing will be limited.")

try:
    import ffmpeg
    FFMPEG_AVAILABLE = True
except ImportError:
    FFMPEG_AVAILABLE = False
    print("Warning: ffmpeg-python not installed. Video extraction limited.")

# Import our other media workers
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

try:
    from ocr_worker import OCRWorker
    from asr_worker import ASRWorker, ASRResult
    OCR_ASR_AVAILABLE = True
except ImportError:
    print("Warning: OCR/ASR workers not available")
    OCR_ASR_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class VideoFrame:
    """Individual video frame with analysis"""
    timestamp: float
    frame_index: int
    frame_path: str
    ocr_text: str
    confidence: float
    metadata: Dict
    detections: List[str] = field(default_factory=list)
    embedding: Optional[List[float]] = None


@dataclass
class VideoResult:
    """Result from video processing"""
    video_path: str
    duration: float
    fps: float
    frame_count: int
    keyframes: List[VideoFrame]
    audio_transcript: Optional[str]
    metadata: Dict
    processing_time: float
    sha256_hash: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


class VideoProcessor:
    """Video processing worker for forensic video analysis"""
    
    def __init__(self,
                 output_dir: str = "data/video_output",
                 keyframe_interval: float = 5.0,
                 max_keyframes: int = 100,
                 extract_audio: bool = True):
        """
        Initialize Video Processor
        
        Args:
            output_dir: Directory for video processing output
            keyframe_interval: Interval between keyframes in seconds
            max_keyframes: Maximum number of keyframes to extract
            extract_audio: Whether to extract and transcribe audio
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.keyframe_interval = keyframe_interval
        self.max_keyframes = max_keyframes
        self.extract_audio = extract_audio
        
        # Initialize sub-workers
        if OCR_ASR_AVAILABLE:
            self.ocr_worker = OCRWorker(output_dir=str(self.output_dir / "ocr"))
            if extract_audio:
                self.asr_worker = ASRWorker(output_dir=str(self.output_dir / "asr"))
            else:
                self.asr_worker = None
        else:
            self.ocr_worker = None
            self.asr_worker = None
        
        # Supported video formats
        self.supported_formats = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v'}
        
        # Initialize YOLO and CLIP
        self.yolo_model = None
        if YOLO_AVAILABLE:
            try:
                self.yolo_model = YOLO("yolov8n.pt") # Load nano model
            except Exception as e:
                logger.error(f"Failed to load YOLO: {e}")

        self.clip_model = None
        if CLIP_AVAILABLE:
            try:
                self.clip_model = SentenceTransformer('clip-ViT-B-32')
            except Exception as e:
                logger.error(f"Failed to load CLIP: {e}")

        # Check FFmpeg availability
        self.ffmpeg_available = self._check_ffmpeg()

    def analyze_frame(self, frame_path: Path, timestamp: float, frame_index: int, case_id: str, fps: float, method: str) -> VideoFrame:
        """Run OCR, Object Detection, and Embedding on a frame"""
        ocr_text = ""
        confidence = 0.0
        detections = []
        embedding = None

        try:
            # 1. OCR
            if self.ocr_worker and frame_path.exists():
                ocr_result = self.ocr_worker.process_image(str(frame_path), case_id)
                ocr_text = ocr_result.text
                confidence = ocr_result.confidence

            # 2. YOLO Object Detection
            if self.yolo_model and frame_path.exists():
                results = self.yolo_model(str(frame_path), verbose=False)
                for r in results:
                    for c in r.boxes.cls:
                        detections.append(self.yolo_model.names[int(c)])
                detections = list(set(detections)) # Deduplicate

            # 3. CLIP Embedding
            if self.clip_model and frame_path.exists():
                image = Image.open(frame_path)
                embedding = self.clip_model.encode(image).tolist()

        except Exception as e:
            logger.error(f"Frame analysis failed for {frame_path}: {e}")

        return VideoFrame(
            timestamp=timestamp,
            frame_index=frame_index,
            frame_path=str(frame_path),
            ocr_text=ocr_text,
            confidence=confidence,
            metadata={
                'video_fps': fps,
                'extraction_method': method
            },
            detections=detections,
            embedding=embedding
        )
    
    def _check_ffmpeg(self) -> bool:
        """Check if FFmpeg is available"""
        try:
            subprocess.run(['ffmpeg', '-version'], 
                          capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            try:
                # Try with ffmpeg-python
                return True
            except ImportError:
                logger.warning("FFmpeg not found. Video processing will be limited.")
                return False
    
    def get_video_info(self, video_path: str) -> Dict:
        """
        Get video metadata using FFmpeg
        
        Args:
            video_path: Path to video file
            
        Returns:
            Dictionary with video information
        """
        info = {
            'duration': 0.0,
            'fps': 0.0,
            'width': 0,
            'height': 0,
            'frame_count': 0,
            'format': '',
            'codec': ''
        }
        
        try:
            if FFMPEG_AVAILABLE:
                # Use ffmpeg-python
                probe = ffmpeg.probe(video_path)
                
                # Get video stream info
                video_stream = next((stream for stream in probe['streams'] 
                                   if stream['codec_type'] == 'video'), None)
                
                if video_stream:
                    info.update({
                        'duration': float(video_stream.get('duration', 0)),
                        'fps': eval(video_stream.get('r_frame_rate', '0/1')),
                        'width': int(video_stream.get('width', 0)),
                        'height': int(video_stream.get('height', 0)),
                        'frame_count': int(video_stream.get('nb_frames', 0)),
                        'codec': video_stream.get('codec_name', 'unknown')
                    })
                
                # Get format info
                format_info = probe.get('format', {})
                info['format'] = format_info.get('format_name', 'unknown')
                
            elif CV2_AVAILABLE:
                # Fallback to OpenCV
                cap = cv2.VideoCapture(str(video_path))
                if cap.isOpened():
                    info.update({
                        'fps': cap.get(cv2.CAP_PROP_FPS),
                        'width': int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
                        'height': int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
                        'frame_count': int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    })
                    info['duration'] = info['frame_count'] / info['fps'] if info['fps'] > 0 else 0
                cap.release()
        
        except Exception as e:
            logger.error(f"Failed to get video info: {e}")
        
        return info
    
    def extract_keyframes(self, video_path: str, case_id: str) -> List[VideoFrame]:
        """
        Extract keyframes from video
        
        Args:
            video_path: Path to video file
            case_id: Case identifier
            
        Returns:
            List of VideoFrame objects
        """
        keyframes = []
        video_path = Path(video_path)
        
        # Create keyframes directory
        keyframes_dir = self.output_dir / case_id / "keyframes" / video_path.stem
        keyframes_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            if FFMPEG_AVAILABLE and self.ffmpeg_available:
                # Use FFmpeg for extraction
                keyframes = self._extract_keyframes_ffmpeg(video_path, keyframes_dir, case_id)
            elif CV2_AVAILABLE:
                # Fallback to OpenCV
                keyframes = self._extract_keyframes_opencv(video_path, keyframes_dir, case_id)
            else:
                logger.error("No video processing library available")
                
        except Exception as e:
            logger.error(f"Keyframe extraction failed: {e}")
        
        return keyframes
    
    def _extract_keyframes_ffmpeg(self, video_path: Path, output_dir: Path, case_id: str) -> List[VideoFrame]:
        """Extract keyframes using FFmpeg"""
        keyframes = []
        
        try:
            # Get video info
            info = self.get_video_info(str(video_path))
            duration = info['duration']
            fps = info['fps']
            
            if duration <= 0:
                return keyframes
            
            # Calculate keyframe timestamps
            timestamps = []
            current_time = 0
            while current_time < duration and len(timestamps) < self.max_keyframes:
                timestamps.append(current_time)
                current_time += self.keyframe_interval
            
            # Extract frames at specific timestamps
            for i, timestamp in enumerate(timestamps):
                frame_filename = f"frame_{i:04d}_{timestamp:.2f}s.jpg"
                frame_path = output_dir / frame_filename
                
                try:
                    # Use ffmpeg-python to extract frame
                    (
                        ffmpeg
                        .input(str(video_path), ss=timestamp)
                        .output(str(frame_path), vframes=1, q_scale=2)
                        .overwrite_output()
                        .run(capture_output=True, check=True)
                    )
                    
                    # Analyze frame
                    frame = self.analyze_frame(
                        frame_path, timestamp, i, case_id, fps, 'ffmpeg'
                    )
                    
                    keyframes.append(frame)
                    
                    keyframes.append(frame)
                    
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Failed to extract frame at {timestamp}s: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"FFmpeg keyframe extraction failed: {e}")
        
        return keyframes
    
    def _extract_keyframes_opencv(self, video_path: Path, output_dir: Path, case_id: str) -> List[VideoFrame]:
        """Extract keyframes using OpenCV"""
        keyframes = []
        
        try:
            cap = cv2.VideoCapture(str(video_path))
            if not cap.isOpened():
                logger.error(f"Cannot open video: {video_path}")
                return keyframes
            
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            duration = total_frames / fps if fps > 0 else 0
            
            if fps <= 0 or duration <= 0:
                cap.release()
                return keyframes
            
            # Calculate frame intervals
            frame_interval = int(fps * self.keyframe_interval)
            
            frame_count = 0
            extracted_count = 0
            
            while cap.isOpened() and extracted_count < self.max_keyframes:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Extract keyframes at intervals
                if frame_count % frame_interval == 0:
                    timestamp = frame_count / fps
                    frame_filename = f"frame_{extracted_count:04d}_{timestamp:.2f}s.jpg"
                    frame_path = output_dir / frame_filename
                    
                    # Save frame
                    cv2.imwrite(str(frame_path), frame)
                    
                    # Analyze frame
                    video_frame = self.analyze_frame(
                        frame_path, timestamp, extracted_count, case_id, fps, 'opencv'
                    )
                    
                    keyframes.append(video_frame)
                    
                    keyframes.append(video_frame)
                    extracted_count += 1
                
                frame_count += 1
            
            cap.release()
        
        except Exception as e:
            logger.error(f"OpenCV keyframe extraction failed: {e}")
        
        return keyframes
    
    def extract_audio(self, video_path: str, case_id: str) -> Optional[ASRResult]:
        """
        Extract and transcribe audio from video
        
        Args:
            video_path: Path to video file
            case_id: Case identifier
            
        Returns:
            ASRResult object or None
        """
        if not self.extract_audio or not self.asr_worker:
            return None
        
        video_path = Path(video_path)
        audio_dir = self.output_dir / case_id / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        
        audio_path = audio_dir / f"{video_path.stem}.wav"
        
        try:
            if FFMPEG_AVAILABLE and self.ffmpeg_available:
                # Extract audio using FFmpeg
                (
                    ffmpeg
                    .input(str(video_path))
                    .output(str(audio_path), acodec='pcm_s16le', ac=1, ar=16000)
                    .overwrite_output()
                    .run(capture_output=True, check=True)
                )
                
                # Transcribe audio
                if audio_path.exists():
                    asr_result = self.asr_worker.process_audio(str(audio_path), case_id)
                    return asr_result
            
        except Exception as e:
            logger.error(f"Audio extraction failed: {e}")
        
        return None
    
    def process_video(self, video_path: str, case_id: str) -> VideoResult:
        """
        Process single video file
        
        Args:
            video_path: Path to video file
            case_id: Case identifier
            
        Returns:
            VideoResult object
        """
        start_time = datetime.now()
        video_path = Path(video_path)
        
        # Get video info
        info = self.get_video_info(str(video_path))
        
        # Calculate hash
        sha256_hash = self._calculate_hash(video_path)
        
        # Extract keyframes
        keyframes = self.extract_keyframes(str(video_path), case_id)
        
        # Extract and transcribe audio
        audio_result = self.extract_audio(str(video_path), case_id)
        audio_transcript = audio_result.transcript if audio_result else None
        
        # Extract metadata
        metadata = self._extract_video_metadata(video_path)
        metadata.update(info)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Create result
        result = VideoResult(
            video_path=str(video_path),
            duration=info['duration'],
            fps=info['fps'],
            frame_count=info['frame_count'],
            keyframes=keyframes,
            audio_transcript=audio_transcript,
            metadata=metadata,
            processing_time=processing_time,
            sha256_hash=sha256_hash
        )
        
        # Save result
        self._save_result(result, case_id)
        
        return result
    
    def process_directory(self, directory: str, case_id: str) -> List[VideoResult]:
        """
        Process all video files in a directory
        
        Args:
            directory: Directory containing video files
            case_id: Case identifier
            
        Returns:
            List of video results
        """
        directory = Path(directory)
        results = []
        
        # Find all video files
        video_files = []
        for ext in self.supported_formats:
            video_files.extend(directory.glob(f'**/*{ext}'))
            video_files.extend(directory.glob(f'**/*{ext.upper()}'))
        
        logger.info(f"Found {len(video_files)} video files to process")
        
        # Process each video file
        for video_path in video_files:
            try:
                result = self.process_video(str(video_path), case_id)
                results.append(result)
                logger.info(f"Processed {video_path.name}: {len(result.keyframes)} keyframes, "
                          f"audio: {'yes' if result.audio_transcript else 'no'}")
            except Exception as e:
                logger.error(f"Failed to process {video_path}: {e}")
        
        return results
    
    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _extract_video_metadata(self, video_path: Path) -> Dict:
        """Extract metadata from video file"""
        metadata = {
            'filename': video_path.name,
            'size': video_path.stat().st_size,
            'modified': datetime.fromtimestamp(video_path.stat().st_mtime).isoformat(),
            'format': video_path.suffix.lower()
        }
        
        return metadata
    
    def _save_result(self, result: VideoResult, case_id: str):
        """Save video result to file"""
        output_file = self.output_dir / f"{case_id}_video_results.jsonl"
        
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(result.to_dict(), ensure_ascii=False, default=str) + '\n')
    
    def search_video_content(self, case_id: str, query: str) -> List[Dict]:
        """
        Search for content in video results
        
        Args:
            case_id: Case identifier
            query: Search query
            
        Returns:
            List of matching results
        """
        results_file = self.output_dir / f"{case_id}_video_results.jsonl"
        
        if not results_file.exists():
            return []
        
        matches = []
        query_lower = query.lower()
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                
                # Search in audio transcript
                if result.get('audio_transcript') and query_lower in result['audio_transcript'].lower():
                    matches.append({
                        'video_path': result['video_path'],
                        'match_type': 'audio_transcript',
                        'transcript': result['audio_transcript'],
                        'duration': result['duration']
                    })
                
                # Search in OCR text from keyframes
                for frame in result.get('keyframes', []):
                    if frame.get('ocr_text') and query_lower in frame['ocr_text'].lower():
                        matches.append({
                            'video_path': result['video_path'],
                            'match_type': 'keyframe_ocr',
                            'timestamp': frame['timestamp'],
                            'frame_path': frame['frame_path'],
                            'text': frame['ocr_text'],
                            'confidence': frame['confidence']
                        })
        
        return matches
    
    def generate_report(self, case_id: str) -> Dict:
        """
        Generate video analysis report
        
        Args:
            case_id: Case identifier
            
        Returns:
            Report dictionary
        """
        results_file = self.output_dir / f"{case_id}_video_results.jsonl"
        
        if not results_file.exists():
            return {'error': 'No video results found'}
        
        report = {
            'case_id': case_id,
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_videos': 0,
                'total_duration': 0.0,
                'total_keyframes': 0,
                'videos_with_audio': 0,
                'average_fps': 0.0,
                'formats': {}
            },
            'keyframes_with_text': [],
            'audio_transcripts': [],
            'timeline': []
        }
        
        fps_values = []
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                report['statistics']['total_videos'] += 1
                report['statistics']['total_duration'] += result.get('duration', 0)
                report['statistics']['total_keyframes'] += len(result.get('keyframes', []))
                
                if result.get('audio_transcript'):
                    report['statistics']['videos_with_audio'] += 1
                    report['audio_transcripts'].append({
                        'video': result['video_path'],
                        'transcript': result['audio_transcript'][:200],
                        'duration': result['duration']
                    })
                
                fps = result.get('fps', 0)
                if fps > 0:
                    fps_values.append(fps)
                
                # Track formats
                video_format = result.get('metadata', {}).get('format', 'unknown')
                report['statistics']['formats'][video_format] = \
                    report['statistics']['formats'].get(video_format, 0) + 1
                
                # Keyframes with OCR text
                for frame in result.get('keyframes', []):
                    if frame.get('ocr_text') and frame['confidence'] > 0.5:
                        report['keyframes_with_text'].append({
                            'video': result['video_path'],
                            'timestamp': frame['timestamp'],
                            'text': frame['ocr_text'][:100],
                            'confidence': frame['confidence']
                        })
        
        if fps_values:
            report['statistics']['average_fps'] = sum(fps_values) / len(fps_values)
        
        return report


def main():
    """Test video processor"""
    print("Video Processor Test")
    print("=" * 50)
    
    # Initialize processor
    processor = VideoProcessor(keyframe_interval=10.0, max_keyframes=5)
    
    # Test with a sample video file (if exists)
    test_video = "data/samples/test_video.mp4"
    
    if Path(test_video).exists():
        print(f"\nProcessing: {test_video}")
        result = processor.process_video(test_video, "test_case")
        
        print(f"Duration: {result.duration:.2f}s")
        print(f"FPS: {result.fps:.2f}")
        print(f"Keyframes extracted: {len(result.keyframes)}")
        print(f"Audio transcript: {'yes' if result.audio_transcript else 'no'}")
        print(f"Processing time: {result.processing_time:.2f}s")
        print(f"SHA256: {result.sha256_hash}")
        
        # Show keyframes with OCR
        for i, frame in enumerate(result.keyframes[:3]):
            print(f"Frame {i+1} ({frame.timestamp:.2f}s): {frame.ocr_text[:50]}...")
    else:
        print(f"\nTest video not found: {test_video}")
        print("Creating mock result...")
        
        # Create mock result for demonstration
        mock_frames = [
            VideoFrame(
                timestamp=5.0,
                frame_index=0,
                frame_path="mock_frame_1.jpg",
                ocr_text="Sample text from video frame",
                confidence=0.85,
                metadata={'extraction_method': 'mock'}
            )
        ]
        
        mock_result = VideoResult(
            video_path="mock_video.mp4",
            duration=30.0,
            fps=30.0,
            frame_count=900,
            keyframes=mock_frames,
            audio_transcript="Sample audio transcript from video",
            metadata={'format': 'mp4'},
            processing_time=15.2,
            sha256_hash="abc123def456"
        )
        
        print(f"Mock result: Duration={mock_result.duration}s, Keyframes={len(mock_result.keyframes)}")
    
    print("\n" + "=" * 50)
    print("Video Processor initialized successfully!")
    
    if not processor.ffmpeg_available:
        print("\nNote: Install FFmpeg for full video functionality:")
        print("  pip install ffmpeg-python")
        print("  Or install FFmpeg binary from https://ffmpeg.org/")
    
    if not CV2_AVAILABLE:
        print("\nNote: Install OpenCV for video processing:")
        print("  pip install opencv-python")


if __name__ == "__main__":
    main()