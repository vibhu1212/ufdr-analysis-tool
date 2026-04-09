"""
Step 4: Record Segmentation & Labeling
Segments extracted text into logical entities (contacts, messages, calls) and labels fields
"""

import re
import uuid
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

try:
    import phonenumbers
    HAS_PHONENUMBERS = True
except ImportError:
    HAS_PHONENUMBERS = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecordType(Enum):
    """Types of records that can be identified"""
    CONTACT = "contact"
    MESSAGE = "message"
    CALL_LOG = "call_log"
    GROUP_META = "group_meta"
    BUSINESS_CARD = "business_card"
    LOCATION = "location"
    DEVICE = "device"
    UNKNOWN = "unknown"


@dataclass
class SegmentedRecord:
    """A segmented and labeled record with extracted fields"""
    record_id: str  # UUID
    type_label: RecordType
    confidence: float  # 0.0-1.0
    fields: Dict[str, Any]  # Extracted structured fields
    raw_text: str  # Original text
    extraction_reasons: List[str]  # Why this classification was made
    provenance: Dict[str, Any]  # Source file, offset, method
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            "record_id": self.record_id,
            "type_label": self.type_label.value,
            "confidence": self.confidence,
            "fields": self.fields,
            "raw_text": self.raw_text,
            "extraction_reasons": self.extraction_reasons,
            "provenance": self.provenance,
            "metadata": self.metadata
        }


class PatternLibrary:
    """
    Centralized pattern library for field extraction
    Supports Indian and international formats
    """
    
    # Phone number patterns
    PHONE_PATTERNS = [
        # Indian numbers: +91XXXXXXXXXX or 91XXXXXXXXXX or XXXXXXXXXX
        r'\+91[\s-]?\d{10}',
        r'91[\s-]?\d{10}',
        r'\b\d{10}\b',
        # International: +CC...
        r'\+\d{1,3}[\s-]?\d{6,14}',
        # With parentheses: (XXX) XXX-XXXX
        r'\(\d{3}\)[\s-]?\d{3}[\s-]?\d{4}',
    ]
    
    # Email patterns
    EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    
    # Timestamp patterns (ISO 8601, common formats)
    TIMESTAMP_PATTERNS = [
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO 8601
        r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}',  # DD/MM/YYYY HH:MM:SS
        r'\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}',  # DD-MM-YYYY HH:MM
    ]
    
    # Duration patterns (for calls)
    DURATION_PATTERN = r'(\d+)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?)'
    
    # Honorifics (Indian and international)
    HONORIFICS = [
        r'\b(Mr|Mrs|Ms|Dr|Prof|Sir|Madam|Shri|Smt|Kumar|ji)\b\.?'
    ]
    
    # Call types
    CALL_TYPES = ['incoming', 'outgoing', 'missed', 'rejected', 'blocked', 'voicemail']
    
    # Message types
    MESSAGE_TYPES = ['sms', 'whatsapp', 'telegram', 'signal', 'email', 'wechat', 'messenger']
    
    @classmethod
    def get_phone_regex(cls) -> re.Pattern:
        """Get compiled phone regex"""
        return re.compile('|'.join(cls.PHONE_PATTERNS), re.IGNORECASE)
    
    @classmethod
    def get_email_regex(cls) -> re.Pattern:
        """Get compiled email regex"""
        return re.compile(cls.EMAIL_PATTERN, re.IGNORECASE)
    
    @classmethod
    def get_timestamp_regex(cls) -> re.Pattern:
        """Get compiled timestamp regex"""
        return re.compile('|'.join(cls.TIMESTAMP_PATTERNS), re.IGNORECASE)


class HeuristicSegmenter:
    """
    Segments records using heuristic pattern matching
    """
    
    def __init__(self):
        self.phone_regex = PatternLibrary.get_phone_regex()
        self.email_regex = PatternLibrary.get_email_regex()
        self.timestamp_regex = PatternLibrary.get_timestamp_regex()
    
    def extract_phones(self, text: str) -> List[str]:
        """Extract phone numbers from text"""
        matches = self.phone_regex.findall(text)
        # Clean and normalize
        phones = []
        for match in matches:
            phone = re.sub(r'[\s-]', '', match)  # Remove spaces and dashes
            phones.append(phone)
        return phones
    
    def normalize_phone(self, phone: str) -> Optional[str]:
        """
        Normalize phone number to E.164 format if possible
        """
        if not HAS_PHONENUMBERS:
            # Fallback normalization
            phone = re.sub(r'[\s\-\(\)]', '', phone)
            if not phone.startswith('+'):
                if phone.startswith('91') and len(phone) == 12:
                    phone = '+' + phone
                elif len(phone) == 10:
                    phone = '+91' + phone  # Assume Indian
            return phone
        
        try:
            # Try to parse with phonenumbers library
            parsed = phonenumbers.parse(phone, "IN")  # Default to India
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except:
            pass
        
        # Fallback
        phone = re.sub(r'[\s-\(\)]', '', phone)
        if not phone.startswith('+'):
            if len(phone) == 10:
                phone = '+91' + phone
        return phone
    
    def extract_emails(self, text: str) -> List[str]:
        """Extract email addresses from text"""
        return self.email_regex.findall(text)
    
    def extract_timestamps(self, text: str) -> List[str]:
        """Extract timestamps from text"""
        return self.timestamp_regex.findall(text)
    
    def extract_duration(self, text: str) -> Optional[int]:
        """Extract call duration in seconds"""
        pattern = re.compile(PatternLibrary.DURATION_PATTERN, re.IGNORECASE)
        match = pattern.search(text)
        
        if match:
            value = int(match.group(1))
            unit = match.group(2).lower()
            
            if 'hour' in unit or 'hr' in unit:
                return value * 3600
            elif 'minute' in unit or 'min' in unit:
                return value * 60
            else:  # seconds
                return value
        
        # Try to extract just a number (assume seconds)
        match = re.search(r'\b(\d{1,5})\s*s\b', text, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        return None


class RecordTypeClassifier:
    """
    Classifies record type based on extracted fields
    """
    
    def __init__(self):
        self.segmenter = HeuristicSegmenter()
    
    def classify(self, text: str, structured_record: Optional[Dict] = None) -> Tuple[RecordType, float, List[str]]:
        """
        Classify record type
        
        Returns:
            (RecordType, confidence, reasons)
        """
        reasons = []
        
        # Extract fields
        phones = self.segmenter.extract_phones(text)
        emails = self.segmenter.extract_emails(text)
        timestamps = self.segmenter.extract_timestamps(text)
        duration = self.segmenter.extract_duration(text)
        
        # Check structured record if available
        has_name = False
        has_message_text = False
        has_call_type = False
        has_sender_receiver = False
        
        if structured_record:
            keys_lower = {k.lower() for k in structured_record.keys()}
            has_name = any(k in keys_lower for k in ['name', 'contact_name', 'display_name'])
            has_message_text = any(k in keys_lower for k in ['message', 'text', 'body', 'content'])
            has_call_type = any(k in keys_lower for k in ['call_type', 'type', 'direction'])
            has_sender_receiver = any(k in keys_lower for k in ['sender', 'receiver', 'from', 'to'])
        
        # Decision tree for classification
        
        # CALL_LOG: has duration or call_type + phones + timestamp
        if (duration is not None) or has_call_type:
            if len(phones) >= 1 and len(timestamps) >= 1:
                reasons.append("has_duration_and_timestamp")
                reasons.append(f"phones_found: {len(phones)}")
                return RecordType.CALL_LOG, 0.95, reasons
            elif len(phones) >= 1:
                reasons.append("has_duration_or_call_type")
                return RecordType.CALL_LOG, 0.85, reasons
        
        # MESSAGE: has sender/receiver + message text + timestamp
        if has_message_text and has_sender_receiver:
            reasons.append("has_sender_receiver_and_message_text")
            if len(timestamps) >= 1:
                reasons.append("has_timestamp")
                return RecordType.MESSAGE, 0.95, reasons
            return RecordType.MESSAGE, 0.85, reasons
        
        # MESSAGE (fallback): has timestamp + phone + looks like message
        if len(timestamps) >= 1 and len(phones) >= 1:
            if 'message' in text.lower() or 'text' in text.lower() or 'chat' in text.lower():
                reasons.append("timestamp_phone_message_keyword")
                return RecordType.MESSAGE, 0.75, reasons
        
        # CONTACT: has name + (phone or email), no timestamp or duration
        if has_name and (len(phones) >= 1 or len(emails) >= 1):
            if len(timestamps) == 0 and duration is None:
                reasons.append("has_name_and_contact_info")
                reasons.append(f"phones: {len(phones)}, emails: {len(emails)}")
                return RecordType.CONTACT, 0.90, reasons
        
        # CONTACT (fallback): just phone or email with no other strong signals
        if (len(phones) >= 1 or len(emails) >= 1) and len(timestamps) == 0:
            reasons.append("has_contact_info_no_timestamp")
            return RecordType.CONTACT, 0.70, reasons
        
        # UNKNOWN
        reasons.append("no_strong_classification_signals")
        return RecordType.UNKNOWN, 0.5, reasons


class FieldExtractor:
    """
    Extracts structured fields from records
    """
    
    def __init__(self):
        self.segmenter = HeuristicSegmenter()
    
    def extract_contact_fields(self, text: str, structured_record: Optional[Dict] = None) -> Dict[str, Any]:
        """Extract fields for contact record"""
        fields = {}
        
        # Extract from structured record first
        if structured_record:
            for key, value in structured_record.items():
                key_lower = key.lower()
                if 'name' in key_lower and 'name' not in fields:
                    fields['name'] = value
                elif 'phone' in key_lower and 'phone' not in fields:
                    fields['phone'] = self.segmenter.normalize_phone(str(value))
                elif 'email' in key_lower and 'email' not in fields:
                    fields['email'] = value
                elif 'address' in key_lower:
                    fields['address'] = value
                elif 'company' in key_lower or 'organization' in key_lower:
                    fields['organization'] = value
        
        # Extract from text if not found
        if 'phone' not in fields:
            phones = self.segmenter.extract_phones(text)
            if phones:
                fields['phone'] = self.segmenter.normalize_phone(phones[0])
                if len(phones) > 1:
                    fields['additional_phones'] = [self.segmenter.normalize_phone(p) for p in phones[1:]]
        
        if 'email' not in fields:
            emails = self.segmenter.extract_emails(text)
            if emails:
                fields['email'] = emails[0]
                if len(emails) > 1:
                    fields['additional_emails'] = emails[1:]
        
        if 'name' not in fields:
            # Try to extract name from text (simple heuristic)
            # Look for "Name: XXX" or "Contact: XXX" pattern
            name_patterns = [
                r'Name:\s*([^\n,]+)',
                r'Contact:\s*([^\n,]+)',
                r'Display Name:\s*([^\n,]+)',
            ]
            for pattern in name_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    fields['name'] = match.group(1).strip()
                    break
        
        return fields
    
    def extract_message_fields(self, text: str, structured_record: Optional[Dict] = None) -> Dict[str, Any]:
        """Extract fields for message record"""
        fields = {}
        
        # Extract from structured record
        if structured_record:
            for key, value in structured_record.items():
                key_lower = key.lower()
                if 'sender' in key_lower or 'from' in key_lower:
                    fields['sender'] = value
                elif 'receiver' in key_lower or 'to' in key_lower:
                    fields['receiver'] = value
                elif 'message' in key_lower or 'text' in key_lower or 'body' in key_lower:
                    fields['message_text'] = value
                elif 'timestamp' in key_lower or 'date' in key_lower or 'time' in key_lower:
                    fields['timestamp'] = value
                elif 'type' in key_lower or 'app' in key_lower:
                    fields['message_type'] = value
        
        # Extract timestamp from text
        if 'timestamp' not in fields:
            timestamps = self.segmenter.extract_timestamps(text)
            if timestamps:
                fields['timestamp'] = timestamps[0]
        
        # Extract phone numbers (sender/receiver)
        if 'sender' not in fields or 'receiver' not in fields:
            phones = self.segmenter.extract_phones(text)
            if len(phones) >= 2:
                if 'sender' not in fields:
                    fields['sender'] = self.segmenter.normalize_phone(phones[0])
                if 'receiver' not in fields:
                    fields['receiver'] = self.segmenter.normalize_phone(phones[1])
            elif len(phones) == 1:
                if 'sender' not in fields:
                    fields['sender'] = self.segmenter.normalize_phone(phones[0])
        
        return fields
    
    def extract_call_fields(self, text: str, structured_record: Optional[Dict] = None) -> Dict[str, Any]:
        """Extract fields for call log record"""
        fields = {}
        
        # Extract from structured record
        if structured_record:
            for key, value in structured_record.items():
                key_lower = key.lower()
                if 'caller' in key_lower or 'from' in key_lower:
                    fields['caller'] = value
                elif 'callee' in key_lower or 'to' in key_lower or 'receiver' in key_lower:
                    fields['callee'] = value
                elif 'duration' in key_lower:
                    fields['duration_seconds'] = value
                elif 'type' in key_lower or 'direction' in key_lower:
                    fields['call_type'] = value
                elif 'timestamp' in key_lower or 'date' in key_lower or 'time' in key_lower:
                    fields['timestamp'] = value
        
        # Extract from text
        if 'duration_seconds' not in fields:
            duration = self.segmenter.extract_duration(text)
            if duration:
                fields['duration_seconds'] = duration
        
        if 'timestamp' not in fields:
            timestamps = self.segmenter.extract_timestamps(text)
            if timestamps:
                fields['timestamp'] = timestamps[0]
        
        # Extract phone numbers
        if 'caller' not in fields or 'callee' not in fields:
            phones = self.segmenter.extract_phones(text)
            if len(phones) >= 2:
                fields['caller'] = self.segmenter.normalize_phone(phones[0])
                fields['callee'] = self.segmenter.normalize_phone(phones[1])
            elif len(phones) == 1:
                fields['caller'] = self.segmenter.normalize_phone(phones[0])
        
        # Extract call type
        if 'call_type' not in fields:
            text_lower = text.lower()
            for call_type in PatternLibrary.CALL_TYPES:
                if call_type in text_lower:
                    fields['call_type'] = call_type
                    break
        
        return fields


class RecordSegmentationEngine:
    """
    Main engine that coordinates segmentation, classification, and field extraction
    """
    
    def __init__(self):
        self.classifier = RecordTypeClassifier()
        self.extractor = FieldExtractor()
    
    def segment_record(self, 
                      text: str,
                      structured_record: Optional[Dict] = None,
                      provenance: Optional[Dict] = None) -> SegmentedRecord:
        """
        Segment a single record
        
        Args:
            text: Raw text of the record
            structured_record: Optional structured data (from JSON/CSV/etc.)
            provenance: Optional provenance information
            
        Returns:
            SegmentedRecord with classification and extracted fields
        """
        # Classify record type
        record_type, confidence, reasons = self.classifier.classify(text, structured_record)
        
        # Extract fields based on type
        if record_type == RecordType.CONTACT:
            fields = self.extractor.extract_contact_fields(text, structured_record)
        elif record_type == RecordType.MESSAGE:
            fields = self.extractor.extract_message_fields(text, structured_record)
        elif record_type == RecordType.CALL_LOG:
            fields = self.extractor.extract_call_fields(text, structured_record)
        else:
            fields = {}
        
        # Create segmented record
        return SegmentedRecord(
            record_id=str(uuid.uuid4()),
            type_label=record_type,
            confidence=confidence,
            fields=fields,
            raw_text=text,
            extraction_reasons=reasons,
            provenance=provenance or {},
            metadata={}
        )
    
    def segment_batch(self,
                     texts: List[str],
                     structured_records: Optional[List[Dict]] = None,
                     provenance_list: Optional[List[Dict]] = None) -> List[SegmentedRecord]:
        """
        Segment multiple records in batch
        
        Args:
            texts: List of raw texts
            structured_records: Optional list of structured data
            provenance_list: Optional list of provenance information
            
        Returns:
            List of SegmentedRecord objects
        """
        results = []
        
        for i, text in enumerate(texts):
            structured = structured_records[i] if structured_records and i < len(structured_records) else None
            prov = provenance_list[i] if provenance_list and i < len(provenance_list) else None
            
            record = self.segment_record(text, structured, prov)
            results.append(record)
        
        logger.info(f"Segmented {len(results)} records")
        return results


if __name__ == "__main__":
    # Test record segmentation
    engine = RecordSegmentationEngine()
    
    # Test contact
    contact_text = "Name: राज कुमार\nPhone: +919876543210\nEmail: raj.kumar@example.com"
    contact_record = engine.segment_record(contact_text)
    
    print("\n" + "="*60)
    print("Test 1: Contact Record")
    print("="*60)
    print(f"Type: {contact_record.type_label.value}")
    print(f"Confidence: {contact_record.confidence:.2%}")
    print(f"Fields: {contact_record.fields}")
    print(f"Reasons: {contact_record.extraction_reasons}")
    
    # Test call log
    call_text = "Caller: +919876543210\nCallee: +918765432109\nDuration: 180 seconds\nType: Outgoing\nTimestamp: 2025-01-02 16:30:00"
    call_record = engine.segment_record(call_text)
    
    print("\n" + "="*60)
    print("Test 2: Call Log Record")
    print("="*60)
    print(f"Type: {call_record.type_label.value}")
    print(f"Confidence: {call_record.confidence:.2%}")
    print(f"Fields: {call_record.fields}")
    print(f"Reasons: {call_record.extraction_reasons}")
    
    # Test message
    message_text = "From: +919876543210\nTo: +918765432109\nText: Hello, how are you?\nTimestamp: 2025-01-02T16:30:00"
    message_record = engine.segment_record(message_text)
    
    print("\n" + "="*60)
    print("Test 3: Message Record")
    print("="*60)
    print(f"Type: {message_record.type_label.value}")
    print(f"Confidence: {message_record.confidence:.2%}")
    print(f"Fields: {message_record.fields}")
    print(f"Reasons: {message_record.extraction_reasons}")
    
    # Test with structured data
    structured_contact = {
        "name": "అనిల్ రెడ్డి",
        "phone": "+919123456789",
        "email": "anil@example.com"
    }
    structured_record = engine.segment_record("", structured_contact)
    
    print("\n" + "="*60)
    print("Test 4: Structured Contact")
    print("="*60)
    print(f"Type: {structured_record.type_label.value}")
    print(f"Confidence: {structured_record.confidence:.2%}")
    print(f"Fields: {structured_record.fields}")
