"""
ASR (Automatic Speech Recognition) Worker
Processes audio files from UFDR archives using OpenAI Whisper
Supports multilingual transcription (Hindi, English, Arabic, etc.)
"""

import os
import logging
import hashlib
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
import numpy as np

# Try to import audio processing libraries
try:
    from faster_whisper import WhisperModel
    import torch
    WHISPER_AVAILABLE = True
except ImportError:
    WHISPER_AVAILABLE = False
    print("Warning: faster-whisper not installed. ASR features will be limited.")

try:
    import librosa
    import soundfile as sf
    AUDIO_LIBS_AVAILABLE = True
except ImportError:
    AUDIO_LIBS_AVAILABLE = False
    print("Warning: librosa/soundfile not installed. Audio preprocessing limited.")

try:
    from pydub import AudioSegment
    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False
    print("Warning: pydub not installed. Audio format conversion limited.")

logger = logging.getLogger(__name__)


@dataclass
class ASRResult:
    """Result from ASR processing"""
    audio_path: str
    transcript: str
    language: str
    confidence: float
    processing_time: float
    segments: List[Dict]  # Timestamped segments
    metadata: Dict
    sha256_hash: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class AudioSegment:
    """Individual audio segment with transcript"""
    start_time: float
    end_time: float
    text: str
    confidence: float


class ASRWorker:
    """ASR processing worker for forensic audio analysis"""
    
    def __init__(self,
                 output_dir: str = "data/asr_output",
                 model_size: str = "base",
                 device: str = "auto",
                 language: Optional[str] = None):
        """
        Initialize ASR Worker
        
        Args:
            output_dir: Directory for ASR output
            model_size: Whisper model size (tiny, base, small, medium, large)
            device: Device to use (auto, cpu, cuda)
            language: Force specific language (None for auto-detection)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model_size = model_size
        self.language = language
        self.model = None
        
        # Configure device
        if device == "auto":
            if WHISPER_AVAILABLE:
                self.device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                self.device = "cpu"
        else:
            self.device = device
        
        # Initialize Whisper model
        if WHISPER_AVAILABLE:
            self._init_whisper()
        
        # Supported audio formats
        self.supported_formats = {'.wav', '.mp3', '.m4a', '.flac', '.ogg', '.aac', '.wma', '.amr'}
        
        # Language mapping for forensic contexts
        self.forensic_languages = {
            'en': 'english',
            'hi': 'hindi',
            'ar': 'arabic',
            'ur': 'urdu',
            'bn': 'bengali',
            'ta': 'tamil',
            'te': 'telugu',
            'ml': 'malayalam',
            'kn': 'kannada',
            'gu': 'gujarati',
            'pa': 'punjabi',
            'mr': 'marathi'
        }
    
    def _init_whisper(self):
        """Initialize Faster-Whisper model"""
        try:
            logger.info(f"Loading Faster-Whisper model: {self.model_size}")
            compute_type = "float16" if self.device == "cuda" else "int8"
            self.model = WhisperModel(self.model_size, device=self.device, compute_type=compute_type)
            logger.info(f"Faster-Whisper model loaded on {self.device} with {compute_type} precision")
        except Exception as e:
            logger.error(f"Failed to load Whisper model: {e}")
            self.model = None
    
    def preprocess_audio(self, audio_path: str) -> str:
        """
        Preprocess audio file for better transcription
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            Path to preprocessed audio file
        """
        audio_path = Path(audio_path)
        
        # If no preprocessing libraries available, return original
        if not AUDIO_LIBS_AVAILABLE and not PYDUB_AVAILABLE:
            return str(audio_path)
        
        try:
            # Convert to WAV format if needed
            if audio_path.suffix.lower() not in ['.wav', '.flac']:
                if PYDUB_AVAILABLE:
                    # Use pydub for format conversion
                    audio = AudioSegment.from_file(str(audio_path))
                    
                    # Normalize audio levels
                    audio = audio.normalize()
                    
                    # Convert to mono if stereo
                    if audio.channels > 1:
                        audio = audio.set_channels(1)
                    
                    # Ensure 16kHz sample rate (Whisper's native rate)
                    audio = audio.set_frame_rate(16000)
                    
                    # Save preprocessed audio
                    temp_path = self.output_dir / f"temp_{audio_path.stem}.wav"
                    audio.export(str(temp_path), format="wav")
                    
                    return str(temp_path)
            
            # Use librosa for advanced preprocessing
            if AUDIO_LIBS_AVAILABLE:
                # Load audio
                y, sr = librosa.load(str(audio_path), sr=16000, mono=True)
                
                # Noise reduction (simple spectral subtraction)
                y_denoised = self._reduce_noise(y, sr)
                
                # Normalize volume
                y_normalized = librosa.util.normalize(y_denoised)
                
                # Save preprocessed audio
                temp_path = self.output_dir / f"preprocessed_{audio_path.stem}.wav"
                sf.write(str(temp_path), y_normalized, sr)
                
                return str(temp_path)
            
        except Exception as e:
            logger.warning(f"Audio preprocessing failed: {e}")
        
        return str(audio_path)
    
    def _reduce_noise(self, audio: np.ndarray, sr: int) -> np.ndarray:
        """Simple noise reduction using spectral subtraction"""
        # Compute STFT
        stft = librosa.stft(audio)
        magnitude = np.abs(stft)
        phase = np.angle(stft)
        
        # Estimate noise from first 0.5 seconds
        noise_duration = min(int(0.5 * sr / 512), magnitude.shape[1])
        noise_spectrum = np.mean(magnitude[:, :noise_duration], axis=1, keepdims=True)
        
        # Spectral subtraction
        alpha = 2.0  # Over-subtraction factor
        beta = 0.01  # Spectral floor
        
        magnitude_denoised = magnitude - alpha * noise_spectrum
        magnitude_denoised = np.maximum(magnitude_denoised, beta * magnitude)
        
        # Reconstruct audio
        stft_denoised = magnitude_denoised * np.exp(1j * phase)
        audio_denoised = librosa.istft(stft_denoised)
        
        return audio_denoised
    
    def transcribe_audio(self, audio_path: str) -> Tuple[str, List[Dict], str, float]:
        """
        Transcribe audio using Faster-Whisper
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            Tuple of (transcript, segments, detected_language, confidence)
        """
        if not WHISPER_AVAILABLE or not self.model:
            return "ASR not available (faster-whisper not installed)", [], "unknown", 0.0
        
        try:
            # Preprocess audio
            processed_path = self.preprocess_audio(audio_path)
            
            # Transcribe with Faster-Whisper
            # segments is a generator
            segments_generator, info = self.model.transcribe(
                processed_path, 
                beam_size=5,
                language=self.language,
                vad_filter=True  # useful for silence filtering
            )
            
            # Collect segments
            segments = []
            full_transcript = []
            avg_logprob = 0.0
            
            for segment in segments_generator:
                text = segment.text.strip()
                full_transcript.append(text)
                segments.append({
                    "start": segment.start,
                    "end": segment.end,
                    "text": text,
                    "confidence": np.exp(segment.avg_logprob) # fast-whisper returns logprob
                })
                avg_logprob += segment.avg_logprob
            
            transcript = " ".join(full_transcript)
            detected_language = info.language
            language_probability = info.language_probability
            
            # Calculate overall confidence
            if segments:
                # Use language probability combined with average segment confidence
                # But info.language_probability is for language detection confidence
                # Average segment confidence is a better metric for transcript accuracy
                avg_conf = np.mean([s["confidence"] for s in segments])
                confidence = avg_conf
            else:
                confidence = 0.0
            
            # Clean up temporary file
            if processed_path != str(audio_path):
                try:
                    os.remove(processed_path)
                except:
                    pass
            
            return transcript, segments, detected_language, confidence
            
        except Exception as e:
            logger.error(f"Whisper transcription failed: {e}")
            return "", [], "unknown", 0.0
    
    def process_audio(self, audio_path: str, case_id: str) -> ASRResult:
        """
        Process single audio file for ASR
        
        Args:
            audio_path: Path to audio file
            case_id: Case identifier
            
        Returns:
            ASRResult object
        """
        start_time = datetime.now()
        audio_path = Path(audio_path)
        
        # Calculate hash
        sha256_hash = self._calculate_hash(audio_path)
        
        # Transcribe audio
        transcript, segments, language, confidence = self.transcribe_audio(audio_path)
        
        # Extract metadata
        metadata = self._extract_audio_metadata(audio_path)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Create result
        result = ASRResult(
            audio_path=str(audio_path),
            transcript=transcript,
            language=language,
            confidence=confidence,
            processing_time=processing_time,
            segments=segments,
            metadata=metadata,
            sha256_hash=sha256_hash
        )
        
        # Save result
        self._save_result(result, case_id)
        
        return result
    
    def process_directory(self, directory: str, case_id: str) -> List[ASRResult]:
        """
        Process all audio files in a directory
        
        Args:
            directory: Directory containing audio files
            case_id: Case identifier
            
        Returns:
            List of ASR results
        """
        directory = Path(directory)
        results = []
        
        # Find all audio files
        audio_files = []
        for ext in self.supported_formats:
            audio_files.extend(directory.glob(f'**/*{ext}'))
            audio_files.extend(directory.glob(f'**/*{ext.upper()}'))
        
        logger.info(f"Found {len(audio_files)} audio files to process")
        
        # Process each audio file
        for audio_path in audio_files:
            try:
                result = self.process_audio(audio_path, case_id)
                results.append(result)
                logger.info(f"Processed {audio_path.name}: {len(result.transcript)} chars transcribed")
            except Exception as e:
                logger.error(f"Failed to process {audio_path}: {e}")
        
        return results
    
    def _calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def _extract_audio_metadata(self, audio_path: Path) -> Dict:
        """Extract metadata from audio file"""
        metadata = {
            'filename': audio_path.name,
            'size': audio_path.stat().st_size,
            'modified': datetime.fromtimestamp(audio_path.stat().st_mtime).isoformat()
        }
        
        try:
            if PYDUB_AVAILABLE:
                audio = AudioSegment.from_file(str(audio_path))
                metadata.update({
                    'format': audio_path.suffix.lower(),
                    'duration': len(audio) / 1000.0,  # Duration in seconds
                    'sample_rate': audio.frame_rate,
                    'channels': audio.channels,
                    'sample_width': audio.sample_width,
                    'bitrate': getattr(audio, 'bitrate', 'unknown')
                })
            elif AUDIO_LIBS_AVAILABLE:
                # Use librosa for basic info
                y, sr = librosa.load(str(audio_path), sr=None)
                metadata.update({
                    'duration': len(y) / sr,
                    'sample_rate': sr,
                    'channels': 1  # librosa loads as mono by default
                })
        except Exception as e:
            logger.debug(f"Could not extract audio metadata: {e}")
        
        return metadata
    
    def _save_result(self, result: ASRResult, case_id: str):
        """Save ASR result to file"""
        output_file = self.output_dir / f"{case_id}_asr_results.jsonl"
        
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(result.to_dict(), ensure_ascii=False) + '\n')
    
    def search_transcript(self, case_id: str, query: str) -> List[Dict]:
        """
        Search for text in ASR transcripts
        
        Args:
            case_id: Case identifier
            query: Search query
            
        Returns:
            List of matching results with timestamps
        """
        results_file = self.output_dir / f"{case_id}_asr_results.jsonl"
        
        if not results_file.exists():
            return []
        
        matches = []
        query_lower = query.lower()
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                
                # Search in full transcript
                if query_lower in result['transcript'].lower():
                    matches.append({
                        'audio_path': result['audio_path'],
                        'transcript': result['transcript'],
                        'language': result['language'],
                        'confidence': result['confidence'],
                        'match_type': 'full_transcript'
                    })
                
                # Search in timestamped segments
                for segment in result.get('segments', []):
                    if query_lower in segment['text'].lower():
                        matches.append({
                            'audio_path': result['audio_path'],
                            'text': segment['text'],
                            'start_time': segment['start'],
                            'end_time': segment['end'],
                            'confidence': segment['confidence'],
                            'language': result['language'],
                            'match_type': 'segment'
                        })
        
        return matches
    
    def generate_report(self, case_id: str) -> Dict:
        """
        Generate ASR analysis report
        
        Args:
            case_id: Case identifier
            
        Returns:
            Report dictionary
        """
        results_file = self.output_dir / f"{case_id}_asr_results.jsonl"
        
        if not results_file.exists():
            return {'error': 'No ASR results found'}
        
        report = {
            'case_id': case_id,
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_files': 0,
                'successful_transcriptions': 0,
                'failed_transcriptions': 0,
                'total_duration': 0.0,
                'total_transcript_length': 0,
                'average_confidence': 0.0,
                'languages_detected': {}
            },
            'high_confidence_transcripts': [],
            'low_confidence_transcripts': [],
            'keyword_matches': {},
            'timeline': []
        }
        
        confidences = []
        
        # Keywords to flag for forensic analysis
        forensic_keywords = [
            'crypto', 'bitcoin', 'wallet', 'transfer', 'money',
            'weapon', 'gun', 'bomb', 'drugs', 'illegal',
            'police', 'arrest', 'hide', 'delete', 'destroy'
        ]
        
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                result = json.loads(line)
                report['statistics']['total_files'] += 1
                
                if result['transcript']:
                    report['statistics']['successful_transcriptions'] += 1
                    report['statistics']['total_transcript_length'] += len(result['transcript'])
                    confidences.append(result['confidence'])
                    
                    # Track languages
                    lang = result['language']
                    report['statistics']['languages_detected'][lang] = \
                        report['statistics']['languages_detected'].get(lang, 0) + 1
                    
                    # Duration
                    duration = result['metadata'].get('duration', 0)
                    report['statistics']['total_duration'] += duration
                    
                    # High/low confidence
                    if result['confidence'] > 0.8:
                        report['high_confidence_transcripts'].append({
                            'audio': result['audio_path'],
                            'transcript': result['transcript'][:200],
                            'confidence': result['confidence'],
                            'language': result['language']
                        })
                    elif result['confidence'] < 0.5:
                        report['low_confidence_transcripts'].append({
                            'audio': result['audio_path'],
                            'confidence': result['confidence'],
                            'language': result['language']
                        })
                    
                    # Keyword detection
                    transcript_lower = result['transcript'].lower()
                    for keyword in forensic_keywords:
                        if keyword in transcript_lower:
                            if keyword not in report['keyword_matches']:
                                report['keyword_matches'][keyword] = []
                            report['keyword_matches'][keyword].append({
                                'audio': result['audio_path'],
                                'confidence': result['confidence'],
                                'language': result['language']
                            })
                else:
                    report['statistics']['failed_transcriptions'] += 1
        
        if confidences:
            report['statistics']['average_confidence'] = sum(confidences) / len(confidences)
        
        return report
    
    def extract_speaker_segments(self, audio_path: str) -> List[Dict]:
        """
        Extract speaker segments (basic implementation)
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            List of speaker segments
        """
        # This is a simplified implementation
        # For production, you'd want to use a proper speaker diarization system
        # like pyannote.audio or resemblyzer
        
        segments = []
        try:
            if AUDIO_LIBS_AVAILABLE:
                y, sr = librosa.load(audio_path, sr=16000)
                
                # Simple voice activity detection using energy
                frame_length = 2048
                hop_length = 512
                
                energy = librosa.feature.rms(y=y, frame_length=frame_length, 
                                           hop_length=hop_length)[0]
                
                # Threshold for voice activity
                threshold = np.mean(energy) * 0.5
                voice_frames = energy > threshold
                
                # Convert frame indices to time
                times = librosa.frames_to_time(range(len(voice_frames)), 
                                             sr=sr, hop_length=hop_length)
                
                # Group consecutive voice segments
                in_segment = False
                start_time = 0
                
                for i, (time, is_voice) in enumerate(zip(times, voice_frames)):
                    if is_voice and not in_segment:
                        start_time = time
                        in_segment = True
                    elif not is_voice and in_segment:
                        segments.append({
                            'start': start_time,
                            'end': time,
                            'speaker': 'unknown',
                            'confidence': 0.5
                        })
                        in_segment = False
                
        except Exception as e:
            logger.error(f"Speaker segmentation failed: {e}")
        
        return segments


def main():
    """Test ASR worker"""
    print("ASR Worker Test")
    print("=" * 50)
    
    # Initialize worker
    worker = ASRWorker(model_size="tiny")  # Use tiny model for testing
    
    # Test with a sample audio file (if exists)
    test_audio = "data/samples/test_audio.wav"
    
    if Path(test_audio).exists():
        print(f"\nProcessing: {test_audio}")
        result = worker.process_audio(test_audio, "test_case")
        
        print(f"Transcript: {result.transcript[:200]}...")
        print(f"Language: {result.language}")
        print(f"Confidence: {result.confidence:.2%}")
        print(f"Processing time: {result.processing_time:.2f}s")
        print(f"Segments: {len(result.segments)}")
        print(f"SHA256: {result.sha256_hash}")
    else:
        print(f"\nTest audio not found: {test_audio}")
        print("Creating mock result...")
        
        # Create mock result for demonstration
        mock_result = ASRResult(
            audio_path="mock_audio.wav",
            transcript="This is sample transcribed text from a forensic audio recording.",
            language="english",
            confidence=0.92,
            processing_time=5.67,
            segments=[
                {"start": 0.0, "end": 3.5, "text": "This is sample transcribed text", "confidence": 0.95},
                {"start": 3.5, "end": 7.2, "text": "from a forensic audio recording.", "confidence": 0.89}
            ],
            metadata={'duration': 7.2, 'sample_rate': 16000},
            sha256_hash="def456abc789"
        )
        
        print(f"Mock result: {mock_result.to_dict()}")
    
    print("\n" + "=" * 50)
    print("ASR Worker initialized successfully!")
    
    if not WHISPER_AVAILABLE:
        print("\nNote: Install whisper for full ASR functionality:")
        print("  pip install openai-whisper torch")
    
    if not AUDIO_LIBS_AVAILABLE:
        print("\nNote: Install audio libraries for advanced preprocessing:")
        print("  pip install librosa soundfile")
    
    if not PYDUB_AVAILABLE:
        print("\nNote: Install pydub for audio format conversion:")
        print("  pip install pydub")


if __name__ == "__main__":
    main()