"""
Step 7: Multi-Signal Name Detection
Combines multiple signals to detect person names with high confidence
"""

import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import unicodedata

logger = logging.getLogger(__name__)


class NameConfidence(Enum):
    """Confidence levels for name detection"""
    HIGH = "high"      # 0.9+
    MEDIUM = "medium"  # 0.7-0.9
    LOW = "low"        # 0.5-0.7
    NONE = "none"      # < 0.5


class UnicodeScript(Enum):
    """Unicode scripts for name detection"""
    DEVANAGARI = "Devanagari"  # Hindi, Marathi, Nepali
    BENGALI = "Bengali"
    TELUGU = "Telugu"
    TAMIL = "Tamil"
    GUJARATI = "Gujarati"
    KANNADA = "Kannada"
    MALAYALAM = "Malayalam"
    GURMUKHI = "Gurmukhi"  # Punjabi
    ORIYA = "Oriya"
    LATIN = "Latin"
    ARABIC = "Arabic"
    CYRILLIC = "Cyrillic"
    HAN = "Han"  # Chinese
    HANGUL = "Hangul"  # Korean
    UNKNOWN = "Unknown"


@dataclass
class NameDetectionResult:
    """Result of name detection"""
    text: str
    is_person_name: bool
    confidence: float
    reasons: List[str]
    detected_script: Optional[UnicodeScript]
    honorific: Optional[str]
    token_count: int
    phone_country_hint: Optional[str]


class HonorificDetector:
    """Detects honorifics in different languages"""
    
    # Multi-language honorifics
    HONORIFICS = {
        # English
        'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'miss', 'dr', 'dr.', 'prof', 'prof.',
        # Hindi/Indian
        'shri', 'smt', 'smt.', 'kumari', 'sri', 'kumar',
        # Other
        'captain', 'col', 'col.', 'major', 'sir', 'madam', 'lord', 'lady'
    }
    
    @staticmethod
    def detect(text: str) -> Optional[str]:
        """Detect honorific in text"""
        words = text.lower().split()
        if not words:
            return None
        
        first_word = words[0].strip('.,')
        if first_word in HonorificDetector.HONORIFICS:
            return first_word
        
        return None


class ScriptDetector:
    """Detects Unicode script of text"""
    
    @staticmethod
    def detect(text: str) -> UnicodeScript:
        """Detect dominant Unicode script"""
        if not text:
            return UnicodeScript.UNKNOWN
        
        script_counts = {}
        
        for char in text:
            if char.isspace() or char in '.,;:!?':
                continue
            
            # Get Unicode script
            try:
                script_name = unicodedata.name(char).split()[0]
                
                if any(s in script_name for s in ['DEVANAGARI', 'HINDI']):
                    script = UnicodeScript.DEVANAGARI
                elif 'BENGALI' in script_name:
                    script = UnicodeScript.BENGALI
                elif 'TELUGU' in script_name:
                    script = UnicodeScript.TELUGU
                elif 'TAMIL' in script_name:
                    script = UnicodeScript.TAMIL
                elif 'GUJARATI' in script_name:
                    script = UnicodeScript.GUJARATI
                elif 'KANNADA' in script_name:
                    script = UnicodeScript.KANNADA
                elif 'MALAYALAM' in script_name:
                    script = UnicodeScript.MALAYALAM
                elif 'GURMUKHI' in script_name:
                    script = UnicodeScript.GURMUKHI
                elif 'ORIYA' in script_name:
                    script = UnicodeScript.ORIYA
                elif 'LATIN' in script_name:
                    script = UnicodeScript.LATIN
                elif 'ARABIC' in script_name:
                    script = UnicodeScript.ARABIC
                elif 'CYRILLIC' in script_name:
                    script = UnicodeScript.CYRILLIC
                elif 'CJK' in script_name or 'IDEOGRAPH' in script_name:
                    script = UnicodeScript.HAN
                elif 'HANGUL' in script_name:
                    script = UnicodeScript.HANGUL
                else:
                    script = UnicodeScript.UNKNOWN
                
                script_counts[script] = script_counts.get(script, 0) + 1
            except:
                continue
        
        if not script_counts:
            return UnicodeScript.UNKNOWN
        
        # Return dominant script
        return max(script_counts, key=script_counts.get)


class NameListMatcher:
    """Matches against curated name lists"""
    
    # Common Indian first names (sample - expand as needed)
    INDIAN_FIRST_NAMES = {
        # Hindi/Sanskrit
        'राज', 'राजेश', 'सुरेश', 'महेश', 'प्रिया', 'अनिता', 'सुनीता',
        'रवि', 'विजय', 'अजय', 'संजय', 'मोहन', 'सोहन',
        # English transliterations
        'raj', 'rajesh', 'suresh', 'mahesh', 'priya', 'anita', 'sunita',
        'ravi', 'vijay', 'ajay', 'sanjay', 'mohan', 'sohan',
        'amit', 'sumit', 'deepak', 'anil', 'sunil', 'kapil',
        'neha', 'pooja', 'sneha', 'ritu', 'meera', 'kavita'
    }
    
    # Common Indian last names (sample)
    INDIAN_LAST_NAMES = {
        'kumar', 'sharma', 'singh', 'verma', 'gupta', 'patel', 'reddy',
        'desai', 'rao', 'iyer', 'joshi', 'mehta', 'agarwal', 'shah'
    }
    
    @staticmethod
    def check_name_list(text: str) -> Tuple[bool, float]:
        """Check if text matches known name lists"""
        text_lower = text.lower()
        words = text_lower.split()
        
        if not words:
            return False, 0.0
        
        # Check first names
        first_name_match = any(word in NameListMatcher.INDIAN_FIRST_NAMES for word in words)
        
        # Check last names
        last_name_match = any(word in NameListMatcher.INDIAN_LAST_NAMES for word in words)
        
        if first_name_match and last_name_match:
            return True, 0.9  # High confidence
        elif first_name_match or last_name_match:
            return True, 0.6  # Medium confidence
        
        return False, 0.0


class PhoneHintExtractor:
    """Extracts country hints from phone numbers"""
    
    COUNTRY_CODES = {
        '+91': 'IN',   # India
        '+1': 'US',    # USA
        '+44': 'GB',   # UK
        '+61': 'AU',   # Australia
        '+81': 'JP',   # Japan
        '+86': 'CN',   # China
    }
    
    @staticmethod
    def extract_country(phone: str) -> Optional[str]:
        """Extract country code from phone number"""
        if not phone:
            return None
        
        phone = phone.strip()
        for code, country in PhoneHintExtractor.COUNTRY_CODES.items():
            if phone.startswith(code):
                return country
        
        return None


class MultiSignalNameDetector:
    """
    Multi-Signal Name Detection Engine
    
    Combines 8 signals to detect person names:
    1. Script signal (Devanagari → likely Indian name)
    2. Phone country code (+91 → India)
    3. NER confidence (if available)
    4. Name-list matching (curated lists)
    5. Honorifics (Mr., Mrs., Shri, etc.)
    6. Token counts (2-3 tokens typical for names)
    7. Context signals (app name, location, group title)
    8. LLM reasoning (for ambiguous cases)
    """
    
    def __init__(self, use_llm_fallback: bool = False):
        self.use_llm_fallback = use_llm_fallback
        self.honorific_detector = HonorificDetector()
        self.script_detector = ScriptDetector()
        self.name_matcher = NameListMatcher()
        self.phone_extractor = PhoneHintExtractor()
    
    def detect_name(
        self,
        text: str,
        phone_context: Optional[str] = None,
        ner_confidence: Optional[float] = None,
        context_signals: Optional[Dict] = None
    ) -> NameDetectionResult:
        """
        Detect if text is a person name using multiple signals
        
        Args:
            text: Text to analyze
            phone_context: Associated phone number for country hint
            ner_confidence: NER confidence if available
            context_signals: Additional context (app_name, location, etc.)
        
        Returns:
            NameDetectionResult with confidence and reasoning
        """
        
        if not text or not text.strip():
            return NameDetectionResult(
                text=text,
                is_person_name=False,
                confidence=0.0,
                reasons=["empty_text"],
                detected_script=None,
                honorific=None,
                token_count=0,
                phone_country_hint=None
            )
        
        text = text.strip()
        reasons = []
        confidence_scores = []
        
        # Signal 1: Script detection
        script = self.script_detector.detect(text)
        reasons.append(f"script={script.value}")
        
        # Script-based confidence
        if script in [UnicodeScript.DEVANAGARI, UnicodeScript.BENGALI, 
                      UnicodeScript.TELUGU, UnicodeScript.TAMIL,
                      UnicodeScript.GUJARATI, UnicodeScript.KANNADA,
                      UnicodeScript.MALAYALAM, UnicodeScript.GURMUKHI]:
            confidence_scores.append(0.7)  # Indian scripts are likely names
            reasons.append(f"indian_script_detected")
        elif script == UnicodeScript.LATIN:
            confidence_scores.append(0.5)  # Could be name or other text
        
        # Signal 2: Phone country code hint
        phone_country_hint = None
        if phone_context:
            phone_country_hint = self.phone_extractor.extract_country(phone_context)
            if phone_country_hint:
                reasons.append(f"phone_country={phone_country_hint}")
                if phone_country_hint == 'IN':
                    confidence_scores.append(0.6)  # India phone suggests Indian name
        
        # Signal 3: NER confidence
        if ner_confidence is not None:
            confidence_scores.append(ner_confidence)
            reasons.append(f"ner_confidence={ner_confidence:.2f}")
        
        # Signal 4: Name list matching
        name_list_match, name_list_score = self.name_matcher.check_name_list(text)
        if name_list_match:
            confidence_scores.append(name_list_score)
            reasons.append(f"name_list_match={name_list_score:.2f}")
        
        # Signal 5: Honorific detection
        honorific = self.honorific_detector.detect(text)
        if honorific:
            confidence_scores.append(0.9)  # Strong signal
            reasons.append(f"honorific={honorific}")
        
        # Signal 6: Token count and patterns
        tokens = text.split()
        token_count = len(tokens)
        reasons.append(f"tokens={token_count}")
        
        # Check for common verbs/non-name patterns
        common_verbs = {'call', 'message', 'sent', 'received', 'send', 'get', 'come', 'go', 'see', 'meet'}
        has_verb = any(token.lower() in common_verbs for token in tokens)
        
        # Check capitalization pattern (Title Case = Name-like)
        # Works for Latin script names
        proper_capitalization = False
        if script == UnicodeScript.LATIN and token_count >= 2:
            # All words should start with capital letter
            proper_capitalization = all(token[0].isupper() for token in tokens if token)
            if proper_capitalization:
                confidence_scores.append(0.75)  # Strong signal for Latin names
                reasons.append("proper_capitalization")
        
        if has_verb:
            confidence_scores.append(0.1)  # Very low - likely not a name
            reasons.append("contains_common_verb")
        elif 2 <= token_count <= 4 and not proper_capitalization:
            confidence_scores.append(0.6)  # Typical name length
            reasons.append("typical_name_length")
        elif token_count == 1:
            confidence_scores.append(0.3)  # Single word - less confident
        elif not proper_capitalization:
            confidence_scores.append(0.2)  # Too many tokens
        
        # Signal 7: Context signals
        if context_signals:
            if context_signals.get('from_contacts_app'):
                confidence_scores.append(0.8)
                reasons.append("context=contacts_app")
            if context_signals.get('has_phone_number'):
                confidence_scores.append(0.7)
                reasons.append("context=has_phone")
        
        # Calculate final confidence
        if confidence_scores:
            # Weighted average (can be tuned)
            final_confidence = sum(confidence_scores) / len(confidence_scores)
        else:
            final_confidence = 0.0
        
        # Signal 8: LLM fallback for ambiguous cases
        if self.use_llm_fallback and 0.5 <= final_confidence < 0.7:
            llm_result = self._llm_name_check(text, script, reasons)
            if llm_result:
                final_confidence = (final_confidence + llm_result['confidence']) / 2
                reasons.append(f"llm_adjusted={llm_result['confidence']:.2f}")
                reasons.extend(llm_result['reasons'])
        
        # Determine if it's a name
        # Require at least one strong signal (honorific, name-list match, or high NER)
        has_strong_signal = (
            honorific is not None or
            name_list_match or
            proper_capitalization or
            (ner_confidence is not None and ner_confidence > 0.8) or
            script in [UnicodeScript.DEVANAGARI, UnicodeScript.BENGALI, 
                      UnicodeScript.TELUGU, UnicodeScript.TAMIL,
                      UnicodeScript.GUJARATI, UnicodeScript.KANNADA,
                      UnicodeScript.MALAYALAM, UnicodeScript.GURMUKHI]
        )
        
        # More conservative threshold without strong signals
        threshold = 0.5 if has_strong_signal else 0.65
        is_person_name = final_confidence >= threshold
        
        return NameDetectionResult(
            text=text,
            is_person_name=is_person_name,
            confidence=final_confidence,
            reasons=reasons,
            detected_script=script,
            honorific=honorific,
            token_count=token_count,
            phone_country_hint=phone_country_hint
        )
    
    def _llm_name_check(
        self,
        text: str,
        script: UnicodeScript,
        existing_reasons: List[str]
    ) -> Optional[Dict]:
        """
        Use LLM to check if text is a person name (fallback for ambiguous cases)
        
        Returns:
            Dict with confidence and reasons, or None if LLM unavailable
        """
        try:
            import requests
            
            prompt = f"""Is this text a person's name? Answer with YES or NO and confidence (0-1).

Text: "{text}"
Script: {script.value}
Existing signals: {', '.join(existing_reasons)}

Consider:
1. Does it look like a person name?
2. Is the format typical for names in this script?
3. Are there any obvious non-name indicators?

Respond in JSON format:
{{"is_name": true/false, "confidence": 0.0-1.0, "reason": "brief explanation"}}
"""
            
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "llama3.2:1b",  # Fast model for quick checks
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 100
                    }
                },
                timeout=10
            )
            
            if response.status_code == 200:
                import json
                result = response.json()
                llm_output = json.loads(result.get('response', '{}'))
                
                return {
                    'confidence': llm_output.get('confidence', 0.5),
                    'reasons': [f"llm_reason: {llm_output.get('reason', 'unknown')}"]
                }
        
        except Exception as e:
            logger.warning(f"LLM fallback failed: {e}")
        
        return None
    
    def detect_names_in_records(
        self,
        records: List[Dict],
        name_field: str = 'name',
        phone_field: Optional[str] = 'phone'
    ) -> List[Dict]:
        """
        Detect names in a list of records
        
        Args:
            records: List of record dictionaries
            name_field: Field containing potential name
            phone_field: Field containing phone number (for country hint)
        
        Returns:
            List of records with name detection results added
        """
        
        results = []
        
        for record in records:
            name_text = record.get(name_field, '')
            phone = record.get(phone_field) if phone_field else None
            
            detection = self.detect_name(
                text=name_text,
                phone_context=phone,
                context_signals={
                    'has_phone_number': bool(phone),
                    'from_contacts_app': True  # Assume contacts
                }
            )
            
            # Add detection results to record
            result_record = record.copy()
            result_record['name_detection'] = {
                'is_person_name': detection.is_person_name,
                'confidence': detection.confidence,
                'confidence_level': self._get_confidence_level(detection.confidence).value,
                'reasons': detection.reasons,
                'detected_script': detection.detected_script.value if detection.detected_script else None,
                'honorific': detection.honorific,
                'token_count': detection.token_count
            }
            
            results.append(result_record)
        
        return results
    
    @staticmethod
    def _get_confidence_level(confidence: float) -> NameConfidence:
        """Convert confidence score to level"""
        if confidence >= 0.9:
            return NameConfidence.HIGH
        elif confidence >= 0.7:
            return NameConfidence.MEDIUM
        elif confidence >= 0.5:
            return NameConfidence.LOW
        else:
            return NameConfidence.NONE


# Singleton instance
_name_detector = None

def get_name_detector(use_llm_fallback: bool = False) -> MultiSignalNameDetector:
    """Get or create name detector singleton"""
    global _name_detector
    
    if _name_detector is None:
        _name_detector = MultiSignalNameDetector(use_llm_fallback=use_llm_fallback)
    
    return _name_detector


# Test function
def test_name_detection():
    """Test name detection with various examples"""
    
    print("\n" + "="*70)
    print("MULTI-SIGNAL NAME DETECTION TEST")
    print("="*70 + "\n")
    
    detector = get_name_detector(use_llm_fallback=False)
    
    # Test cases
    test_cases = [
        # (text, phone, expected_is_name, description)
        ("राज कुमार", "+919876543210", True, "Hindi name with Indian phone"),
        ("Mr. Rajesh Sharma", "+919123456789", True, "English name with honorific"),
        ("Priya", "+919876543210", True, "Single Indian name"),
        ("Message sent", None, False, "Not a name"),
        ("Smt. अनिता वर्मा", "+919123456789", True, "Hindi name with Hindi honorific"),
        ("John Smith", "+14155551234", True, "English name with US phone"),
        ("అనిల్ రెడ్డి", "+919876543210", True, "Telugu name"),
        ("রাজেশ বোস", "+918801234567", True, "Bengali name"),
        ("Call me later", None, False, "Not a name"),
        ("Dr. Suresh Kumar", "+919123456789", True, "Name with Dr. honorific"),
    ]
    
    for text, phone, expected, description in test_cases:
        result = detector.detect_name(
            text=text,
            phone_context=phone,
            context_signals={'from_contacts_app': True}
        )
        
        status = "✅" if result.is_person_name == expected else "❌"
        
        print(f"{status} {description}")
        print(f"   Text: {text}")
        print(f"   Is Name: {result.is_person_name} (Confidence: {result.confidence:.2f})")
        print(f"   Script: {result.detected_script.value if result.detected_script else 'None'}")
        if result.honorific:
            print(f"   Honorific: {result.honorific}")
        print(f"   Reasons: {', '.join(result.reasons[:3])}...")
        print()
    
    print("="*70)


if __name__ == "__main__":
    # Run tests
    logging.basicConfig(level=logging.INFO)
    test_name_detection()
