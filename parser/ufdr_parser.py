"""
UFDR Parser with Streaming XML Support
Handles large UFDR XML reports using SAX/iterparse for memory efficiency
Extracts and normalizes forensic artifacts
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import lxml.etree as ET
import phonenumbers
from dateutil import parser as date_parser
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ArtifactType(Enum):
    """Types of forensic artifacts"""
    MESSAGE = "message"
    CALL = "call"
    CONTACT = "contact"
    DEVICE = "device"
    LOCATION = "location"
    MEDIA = "media"
    APPLICATION = "application"
    BROWSER_HISTORY = "browser_history"
    FILE = "file"
    EMAIL = "email"


@dataclass
class Message:
    """Represents a message artifact"""
    id: str
    sender: Optional[str]
    recipient: Optional[str]
    text: Optional[str]
    timestamp: Optional[str]
    application: Optional[str]
    thread_id: Optional[str]
    attachments: List[str] = None
    metadata: Dict = None
    
    def __post_init__(self):
        if self.attachments is None:
            self.attachments = []
        if self.metadata is None:
            self.metadata = {}


@dataclass
class Call:
    """Represents a call record"""
    id: str
    caller: Optional[str]
    callee: Optional[str]
    timestamp: Optional[str]
    duration: Optional[int]  # in seconds
    call_type: Optional[str]  # incoming/outgoing/missed
    metadata: Dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class Contact:
    """Represents a contact entry"""
    id: str
    name: Optional[str]
    phone_numbers: List[str]
    emails: List[str]
    addresses: List[str]
    metadata: Dict = None
    
    def __post_init__(self):
        if self.phone_numbers is None:
            self.phone_numbers = []
        if self.emails is None:
            self.emails = []
        if self.addresses is None:
            self.addresses = []
        if self.metadata is None:
            self.metadata = {}


class PhoneNumberNormalizer:
    """Normalizes phone numbers to E.164 format"""
    
    @staticmethod
    def normalize(phone: str, default_region: str = "IN") -> Optional[str]:
        """Normalize phone number to E.164 format"""
        if not phone:
            return None
            
        try:
            # Remove common prefixes and clean
            phone = re.sub(r'[^\d+]', '', phone)
            
            # Parse number
            parsed = phonenumbers.parse(phone, default_region)
            
            # Format to E.164
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            else:
                # Return cleaned version if not valid
                return phone
                
        except Exception as e:
            logger.debug(f"Could not normalize phone number {phone}: {e}")
            return phone


class TimestampNormalizer:
    """Normalizes timestamps to ISO8601 UTC"""
    
    @staticmethod
    def normalize(timestamp: Any) -> Optional[str]:
        """Convert various timestamp formats to ISO8601 UTC"""
        if not timestamp:
            return None
            
        try:
            # Handle Unix timestamps
            if isinstance(timestamp, (int, float)):
                dt = datetime.fromtimestamp(timestamp)
            # Handle string timestamps
            else:
                dt = date_parser.parse(str(timestamp))
            
            # Convert to UTC and ISO format
            return dt.isoformat() + "Z"
            
        except Exception as e:
            logger.debug(f"Could not normalize timestamp {timestamp}: {e}")
            return str(timestamp)


class UFDRStreamParser:
    """Streaming XML parser for UFDR files"""
    
    def __init__(self, case_id: str, output_dir: str = "data/parsed"):
        self.case_id = case_id
        self.output_dir = Path(output_dir) / case_id
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.phone_normalizer = PhoneNumberNormalizer()
        self.timestamp_normalizer = TimestampNormalizer()
        
        # Counters for progress tracking
        self.counters = {
            "messages": 0,
            "calls": 0,
            "contacts": 0,
            "media": 0,
            "locations": 0,
            "devices": 0
        }
        
        # Buffers for batch writing
        self.buffer_size = 1000
        self.buffers = {
            "messages": [],
            "calls": [],
            "contacts": [],
            "media": [],
            "locations": [],
            "devices": []
        }
    
    def parse_ufdr_xml(self, xml_path: str) -> Dict:
        """
        Parse UFDR XML using streaming approach
        
        Args:
            xml_path: Path to UFDR XML file
            
        Returns:
            Parsing statistics and metadata
        """
        xml_file = Path(xml_path)
        if not xml_file.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")
        
        logger.info(f"Starting streaming parse of {xml_file}")
        start_time = datetime.now()
        
        # Use iterparse for memory-efficient parsing
        for event, elem in ET.iterparse(xml_file, events=('end',), tag='*'):
            self._process_element(elem)
            
            # Clear element to save memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        # Flush remaining buffers
        self._flush_all_buffers()
        
        # Calculate parsing time
        parse_time = (datetime.now() - start_time).total_seconds()
        
        # Create parsing manifest
        manifest = {
            "case_id": self.case_id,
            "source_file": str(xml_file.absolute()),
            "parse_time": parse_time,
            "parsed_at": datetime.utcnow().isoformat() + "Z",
            "statistics": self.counters,
            "output_directory": str(self.output_dir.absolute())
        }
        
        # Save manifest
        manifest_path = self.output_dir / "parsing_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Parsing complete in {parse_time:.2f} seconds")
        logger.info(f"Statistics: {self.counters}")
        
        return manifest
    
    def _process_element(self, elem):
        """Process individual XML element"""
        tag = elem.tag.lower()
        
        # Route to appropriate handler based on tag
        if 'message' in tag or 'chat' in tag or 'sms' in tag:
            self._process_message(elem)
        elif 'call' in tag:
            self._process_call(elem)
        elif 'contact' in tag:
            self._process_contact(elem)
        elif 'media' in tag or 'attachment' in tag:
            self._process_media(elem)
        elif 'location' in tag or 'gps' in tag:
            self._process_location(elem)
        elif 'device' in tag:
            self._process_device(elem)
    
    def _process_message(self, elem):
        """Extract message artifact"""
        try:
            message = Message(
                id=self._generate_id("msg", elem),
                sender=self._extract_phone(elem, ['from', 'sender', 'source']),
                recipient=self._extract_phone(elem, ['to', 'recipient', 'destination']),
                text=self._extract_text(elem, ['body', 'text', 'content', 'message']),
                timestamp=self._extract_timestamp(elem),
                application=self._extract_text(elem, ['app', 'application', 'source_app']),
                thread_id=self._extract_text(elem, ['thread', 'conversation', 'chat_id']),
                attachments=self._extract_attachments(elem),
                metadata=self._extract_metadata(elem)
            )
            
            self.buffers["messages"].append(asdict(message))
            self.counters["messages"] += 1
            
            # Flush buffer if needed
            if len(self.buffers["messages"]) >= self.buffer_size:
                self._flush_buffer("messages")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _process_call(self, elem):
        """Extract call record"""
        try:
            call = Call(
                id=self._generate_id("call", elem),
                caller=self._extract_phone(elem, ['from', 'caller', 'source']),
                callee=self._extract_phone(elem, ['to', 'callee', 'destination']),
                timestamp=self._extract_timestamp(elem),
                duration=self._extract_duration(elem),
                call_type=self._extract_text(elem, ['type', 'direction', 'call_type']),
                metadata=self._extract_metadata(elem)
            )
            
            self.buffers["calls"].append(asdict(call))
            self.counters["calls"] += 1
            
            if len(self.buffers["calls"]) >= self.buffer_size:
                self._flush_buffer("calls")
                
        except Exception as e:
            logger.error(f"Error processing call: {e}")
    
    def _process_contact(self, elem):
        """Extract contact entry"""
        try:
            contact = Contact(
                id=self._generate_id("contact", elem),
                name=self._extract_text(elem, ['name', 'display_name', 'contact_name']),
                phone_numbers=self._extract_phone_list(elem),
                emails=self._extract_email_list(elem),
                addresses=self._extract_address_list(elem),
                metadata=self._extract_metadata(elem)
            )
            
            self.buffers["contacts"].append(asdict(contact))
            self.counters["contacts"] += 1
            
            if len(self.buffers["contacts"]) >= self.buffer_size:
                self._flush_buffer("contacts")
                
        except Exception as e:
            logger.error(f"Error processing contact: {e}")
    
    def _process_media(self, elem):
        """Extract media metadata"""
        try:
            media = {
                "id": self._generate_id("media", elem),
                "filename": self._extract_text(elem, ['filename', 'name', 'file']),
                "path": self._extract_text(elem, ['path', 'filepath', 'location']),
                "mime_type": self._extract_text(elem, ['mime', 'type', 'content_type']),
                "size": self._extract_number(elem, ['size', 'filesize']),
                "hash": self._extract_text(elem, ['hash', 'md5', 'sha1', 'sha256']),
                "timestamp": self._extract_timestamp(elem),
                "metadata": self._extract_metadata(elem)
            }
            
            self.buffers["media"].append(media)
            self.counters["media"] += 1
            
            if len(self.buffers["media"]) >= self.buffer_size:
                self._flush_buffer("media")
                
        except Exception as e:
            logger.error(f"Error processing media: {e}")
    
    def _process_location(self, elem):
        """Extract location data"""
        try:
            location = {
                "id": self._generate_id("loc", elem),
                "latitude": self._extract_number(elem, ['lat', 'latitude']),
                "longitude": self._extract_number(elem, ['lon', 'long', 'longitude']),
                "accuracy": self._extract_number(elem, ['accuracy', 'precision']),
                "timestamp": self._extract_timestamp(elem),
                "address": self._extract_text(elem, ['address', 'location', 'place']),
                "metadata": self._extract_metadata(elem)
            }
            
            self.buffers["locations"].append(location)
            self.counters["locations"] += 1
            
            if len(self.buffers["locations"]) >= self.buffer_size:
                self._flush_buffer("locations")
                
        except Exception as e:
            logger.error(f"Error processing location: {e}")
    
    def _process_device(self, elem):
        """Extract device information"""
        try:
            device = {
                "id": self._generate_id("dev", elem),
                "imei": self._extract_text(elem, ['imei', 'device_id']),
                "model": self._extract_text(elem, ['model', 'device_model']),
                "manufacturer": self._extract_text(elem, ['manufacturer', 'make']),
                "os": self._extract_text(elem, ['os', 'operating_system']),
                "os_version": self._extract_text(elem, ['version', 'os_version']),
                "metadata": self._extract_metadata(elem)
            }
            
            self.buffers["devices"].append(device)
            self.counters["devices"] += 1
            
            if len(self.buffers["devices"]) >= self.buffer_size:
                self._flush_buffer("devices")
                
        except Exception as e:
            logger.error(f"Error processing device: {e}")
    
    # Helper methods for extraction
    def _extract_text(self, elem, possible_tags: List[str]) -> Optional[str]:
        """Extract text from element or its children"""
        for tag in possible_tags:
            # Check element attributes
            if tag in elem.attrib:
                return elem.attrib[tag]
            
            # Check child elements
            child = elem.find(f".//{tag}")
            if child is not None and child.text:
                return child.text.strip()
        
        # Check element text directly
        if elem.text:
            return elem.text.strip()
        
        return None
    
    def _extract_phone(self, elem, possible_tags: List[str]) -> Optional[str]:
        """Extract and normalize phone number"""
        phone = self._extract_text(elem, possible_tags)
        if phone:
            return self.phone_normalizer.normalize(phone)
        return None
    
    def _extract_timestamp(self, elem) -> Optional[str]:
        """Extract and normalize timestamp"""
        timestamp = self._extract_text(elem, ['timestamp', 'time', 'datetime', 'date'])
        if timestamp:
            return self.timestamp_normalizer.normalize(timestamp)
        return None
    
    def _extract_number(self, elem, possible_tags: List[str]) -> Optional[float]:
        """Extract numeric value"""
        text = self._extract_text(elem, possible_tags)
        if text:
            try:
                return float(text)
            except ValueError:
                return None
        return None
    
    def _extract_duration(self, elem) -> Optional[int]:
        """Extract call duration in seconds"""
        duration = self._extract_text(elem, ['duration', 'length', 'call_duration'])
        if duration:
            try:
                # Handle various duration formats
                if ':' in duration:
                    # Format: HH:MM:SS or MM:SS
                    parts = duration.split(':')
                    if len(parts) == 3:
                        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    elif len(parts) == 2:
                        return int(parts[0]) * 60 + int(parts[1])
                else:
                    # Assume seconds
                    return int(float(duration))
            except:
                return None
        return None
    
    def _extract_phone_list(self, elem) -> List[str]:
        """Extract list of phone numbers"""
        phones = []
        for tag in ['phone', 'number', 'mobile', 'telephone']:
            for child in elem.findall(f".//{tag}"):
                if child.text:
                    normalized = self.phone_normalizer.normalize(child.text.strip())
                    if normalized:
                        phones.append(normalized)
        return phones
    
    def _extract_email_list(self, elem) -> List[str]:
        """Extract list of email addresses"""
        emails = []
        for tag in ['email', 'mail', 'email_address']:
            for child in elem.findall(f".//{tag}"):
                if child.text:
                    emails.append(child.text.strip().lower())
        return emails
    
    def _extract_address_list(self, elem) -> List[str]:
        """Extract list of addresses"""
        addresses = []
        for tag in ['address', 'location', 'street']:
            for child in elem.findall(f".//{tag}"):
                if child.text:
                    addresses.append(child.text.strip())
        return addresses
    
    def _extract_attachments(self, elem) -> List[str]:
        """Extract attachment paths"""
        attachments = []
        for tag in ['attachment', 'media', 'file']:
            for child in elem.findall(f".//{tag}"):
                path = child.get('path') or child.get('href') or child.text
                if path:
                    attachments.append(path.strip())
        return attachments
    
    def _extract_metadata(self, elem) -> Dict:
        """Extract additional metadata as key-value pairs"""
        metadata = {}
        
        # Add all attributes not already processed
        skip_attrs = {'id', 'from', 'to', 'timestamp', 'text', 'body'}
        for key, value in elem.attrib.items():
            if key not in skip_attrs:
                metadata[key] = value
        
        return metadata
    
    def _generate_id(self, prefix: str, elem) -> str:
        """Generate unique ID for artifact"""
        # Try to use existing ID
        existing_id = elem.get('id') or elem.get('uid') or elem.get('uuid')
        if existing_id:
            return f"{prefix}_{existing_id}"
        
        # Generate hash-based ID from content
        content = ET.tostring(elem, encoding='unicode')
        hash_id = hashlib.md5(content.encode()).hexdigest()[:12]
        return f"{prefix}_{hash_id}"
    
    def _flush_buffer(self, buffer_type: str):
        """Write buffer to disk"""
        if not self.buffers[buffer_type]:
            return
        
        # Determine output file
        output_file = self.output_dir / f"{buffer_type}_{self.counters[buffer_type]:06d}.json"
        
        # Write buffer to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.buffers[buffer_type], f, indent=2, ensure_ascii=False)
        
        logger.debug(f"Flushed {len(self.buffers[buffer_type])} {buffer_type} to {output_file}")
        
        # Clear buffer
        self.buffers[buffer_type] = []
    
    def _flush_all_buffers(self):
        """Flush all remaining buffers"""
        for buffer_type in self.buffers:
            if self.buffers[buffer_type]:
                self._flush_buffer(buffer_type)


def main():
    """CLI interface for UFDR parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Parse UFDR XML files")
    parser.add_argument("xml_file", help="Path to UFDR XML file")
    parser.add_argument("--case-id", required=True, help="Case identifier")
    parser.add_argument("--output-dir", default="data/parsed", help="Output directory")
    
    args = parser.parse_args()
    
    parser = UFDRStreamParser(args.case_id, args.output_dir)
    
    try:
        manifest = parser.parse_ufdr_xml(args.xml_file)
        
        print(f"\n✓ Parsing successful!")
        print(f"  Case ID: {manifest['case_id']}")
        print(f"  Parse time: {manifest['parse_time']:.2f} seconds")
        print(f"  Statistics:")
        for key, count in manifest['statistics'].items():
            print(f"    - {key}: {count}")
        print(f"  Output: {manifest['output_directory']}")
        
    except Exception as e:
        print(f"\n✗ Parsing failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())