"""
Advanced UFDR Parser - Production Grade
Supports multiple forensic extraction formats used by law enforcement
Handles Cellebrite, Oxygen Forensics, XRY, MSAB, and generic UFDR formats
"""

import os
import re
import json
import sqlite3
import hashlib
import zipfile
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
from collections import defaultdict
# Optional imports - install if available
try:
    import phonenumbers
except ImportError:
    phonenumbers = None
try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ForensicFormat(Enum):
    """Supported forensic extraction formats"""
    CELLEBRITE_UFED = "cellebrite_ufed"
    CELLEBRITE_PA = "cellebrite_pa"
    OXYGEN = "oxygen"
    XRY = "xry"
    MSAB = "msab"
    AXIOM = "axiom"
    GENERIC_UFDR = "generic_ufdr"
    UNKNOWN = "unknown"


class DataCategory(Enum):
    """Forensic data categories"""
    DEVICE_INFO = "device_info"
    COMMUNICATIONS = "communications"
    CONTACTS = "contacts"
    CALL_LOGS = "call_logs"
    SMS_MMS = "sms_mms"
    INSTANT_MESSAGES = "instant_messages"
    EMAILS = "emails"
    CALENDAR = "calendar"
    NOTES = "notes"
    BROWSER_HISTORY = "browser_history"
    BOOKMARKS = "bookmarks"
    COOKIES = "cookies"
    PASSWORDS = "passwords"
    LOCATIONS = "locations"
    WIFI_NETWORKS = "wifi_networks"
    BLUETOOTH_DEVICES = "bluetooth_devices"
    INSTALLED_APPS = "installed_apps"
    APP_DATA = "app_data"
    SOCIAL_MEDIA = "social_media"
    MEDIA_FILES = "media_files"
    DOCUMENTS = "documents"
    DATABASES = "databases"
    DELETED_DATA = "deleted_data"
    CLOUD_DATA = "cloud_data"
    CRYPTOCURRENCY = "cryptocurrency"
    FINANCIAL = "financial"
    SYSTEM_LOGS = "system_logs"
    REGISTRY = "registry"
    FILE_SYSTEM = "file_system"


@dataclass
class ForensicIntegrity:
    """Forensic integrity and chain of custody information"""
    case_number: str
    evidence_number: str
    examiner_name: str
    examiner_badge: str
    agency: str
    acquisition_date: datetime
    acquisition_tool: str
    acquisition_version: str
    device_seized_date: datetime
    device_seized_location: str
    device_owner: str
    warrant_number: Optional[str] = None
    hash_md5: Optional[str] = None
    hash_sha1: Optional[str] = None
    hash_sha256: Optional[str] = None
    digital_signature: Optional[str] = None
    notes: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage"""
        data = asdict(self)
        # Convert datetime objects to ISO format
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


@dataclass
class DeviceInfo:
    """Comprehensive device information"""
    device_id: str
    manufacturer: str
    model: str
    os_type: str  # iOS, Android, Windows, etc.
    os_version: str
    imei: List[str] = field(default_factory=list)
    serial_number: Optional[str] = None
    phone_number: List[str] = field(default_factory=list)
    iccid: Optional[str] = None
    imsi: Optional[str] = None
    mac_addresses: List[str] = field(default_factory=list)
    bluetooth_mac: Optional[str] = None
    udid: Optional[str] = None  # iOS
    android_id: Optional[str] = None  # Android
    advertising_id: Optional[str] = None
    root_status: bool = False
    jailbreak_status: bool = False
    encryption_status: str = "unknown"
    passcode_type: Optional[str] = None
    acquisition_type: str = "physical"  # physical, logical, cloud
    total_capacity: Optional[int] = None  # in bytes
    used_capacity: Optional[int] = None
    free_capacity: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


class AdvancedUFDRParser:
    """
    Advanced UFDR Parser for production forensic environments
    Handles multiple formats and ensures forensic integrity
    """
    
    def __init__(self, 
                 case_id: str,
                 evidence_number: str,
                 examiner_name: str,
                 agency: str = "Law Enforcement",
                 db_path: str = "data/forensic_evidence.db"):
        """
        Initialize the advanced parser
        
        Args:
            case_id: Official case number
            evidence_number: Evidence tracking number
            examiner_name: Name of forensic examiner
            agency: Law enforcement agency name
            db_path: Path to forensic database
        """
        self.case_id = case_id
        self.evidence_number = evidence_number
        self.examiner_name = examiner_name
        self.agency = agency
        self.db_path = db_path
        
        # Initialize database
        self._init_database()
        
        # Statistics tracking
        self.stats = defaultdict(int)
        
        # Format detection patterns
        self.format_patterns = {
            ForensicFormat.CELLEBRITE_UFED: [
                "ufed_device_info.xml",
                "Extraction Files/",
                "Analytics/"
            ],
            ForensicFormat.CELLEBRITE_PA: [
                "project.pap",
                "Extraction Data/",
                "Analytics Data/"
            ],
            ForensicFormat.OXYGEN: [
                "oxygen.db",
                "OxygenForensicSuite",
                "Plist.xml"
            ],
            ForensicFormat.XRY: [
                "xry_extraction.xml",
                "XRY Files/",
                "Device.xry"
            ],
            ForensicFormat.MSAB: [
                "msab_report.xml",
                "MSAB Extract/",
                "extraction.msab"
            ],
            ForensicFormat.AXIOM: [
                "axiom_case.xml",
                "Artifacts/",
                "Media/"
            ]
        }
        
    def _init_database(self):
        """Initialize forensic database with comprehensive schema"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Cases table with forensic metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                case_id TEXT PRIMARY KEY,
                evidence_number TEXT UNIQUE NOT NULL,
                examiner_name TEXT NOT NULL,
                examiner_badge TEXT,
                agency TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'processing',
                integrity_verified BOOLEAN DEFAULT FALSE,
                court_admissible BOOLEAN DEFAULT FALSE,
                notes TEXT,
                metadata JSON
            )
        """)
        
        # Device information table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                manufacturer TEXT,
                model TEXT,
                os_type TEXT,
                os_version TEXT,
                imei TEXT,
                serial_number TEXT,
                phone_numbers JSON,
                mac_addresses JSON,
                acquisition_type TEXT,
                encryption_status TEXT,
                root_jailbreak_status TEXT,
                capacity_info JSON,
                metadata JSON,
                owner TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id),
                UNIQUE(case_id, device_id)
            )
        """)
        
        # Communications table (unified for all message types)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                comm_type TEXT NOT NULL, -- sms, mms, whatsapp, telegram, etc.
                direction TEXT, -- incoming, outgoing
                sender TEXT,
                sender_name TEXT,
                recipient TEXT,
                recipient_name TEXT,
                timestamp TIMESTAMP,
                text TEXT,
                subject TEXT,
                attachments JSON,
                location JSON,
                read_status BOOLEAN,
                deleted_status BOOLEAN DEFAULT FALSE,
                app_name TEXT,
                thread_id TEXT,
                metadata JSON,
                forensic_tags JSON, -- suspicious, crypto, foreign, etc.
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Call logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS call_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                call_type TEXT, -- incoming, outgoing, missed, video
                caller TEXT,
                caller_name TEXT,
                callee TEXT,
                callee_name TEXT,
                timestamp TIMESTAMP,
                duration INTEGER, -- seconds
                location JSON,
                deleted_status BOOLEAN DEFAULT FALSE,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Contacts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                contact_id TEXT,
                name TEXT,
                phone_numbers JSON,
                emails JSON,
                addresses JSON,
                organizations JSON,
                social_profiles JSON,
                photos JSON,
                notes TEXT,
                groups JSON,
                deleted_status BOOLEAN DEFAULT FALSE,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Locations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                timestamp TIMESTAMP,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                accuracy REAL,
                speed REAL,
                bearing REAL,
                address TEXT,
                place_name TEXT,
                source TEXT, -- GPS, WiFi, Cell Tower
                app_name TEXT,
                deleted_status BOOLEAN DEFAULT FALSE,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Applications table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                app_id TEXT,
                package_name TEXT,
                app_name TEXT,
                version TEXT,
                install_date TIMESTAMP,
                update_date TIMESTAMP,
                last_used TIMESTAMP,
                permissions JSON,
                data_size INTEGER,
                cache_size INTEGER,
                category TEXT,
                deleted_status BOOLEAN DEFAULT FALSE,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Browser history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS browser_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                browser_name TEXT,
                url TEXT,
                title TEXT,
                visit_date TIMESTAMP,
                visit_count INTEGER,
                typed_count INTEGER,
                referrer TEXT,
                download_path TEXT,
                deleted_status BOOLEAN DEFAULT FALSE,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Media files table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                file_path TEXT,
                file_name TEXT,
                file_type TEXT, -- image, video, audio
                mime_type TEXT,
                size INTEGER,
                created_date TIMESTAMP,
                modified_date TIMESTAMP,
                accessed_date TIMESTAMP,
                hash_md5 TEXT,
                hash_sha256 TEXT,
                exif_data JSON,
                location JSON,
                source_app TEXT,
                deleted_status BOOLEAN DEFAULT FALSE,
                recovered_status BOOLEAN DEFAULT FALSE,
                thumbnail BLOB,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Deleted data recovery table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deleted_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                data_type TEXT,
                recovery_method TEXT,
                original_path TEXT,
                deleted_date TIMESTAMP,
                recovered_date TIMESTAMP,
                content TEXT,
                binary_data BLOB,
                confidence_score REAL,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Cloud artifacts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cloud_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                service_name TEXT, -- Google, iCloud, Dropbox, etc.
                account_id TEXT,
                account_email TEXT,
                artifact_type TEXT,
                content JSON,
                sync_date TIMESTAMP,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Cryptocurrency artifacts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                wallet_type TEXT,
                wallet_address TEXT,
                cryptocurrency TEXT,
                balance TEXT,
                transactions JSON,
                keys JSON, -- encrypted
                source_app TEXT,
                discovered_date TIMESTAMP,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Social media artifacts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS social_media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                device_id TEXT,
                platform TEXT, -- Facebook, Instagram, Twitter, etc.
                account_id TEXT,
                username TEXT,
                display_name TEXT,
                profile_data JSON,
                posts JSON,
                messages JSON,
                contacts JSON,
                media JSON,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Forensic tags and patterns table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forensic_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                pattern_type TEXT, -- suspicious, criminal, financial, etc.
                pattern_name TEXT,
                description TEXT,
                severity TEXT, -- low, medium, high, critical
                evidence_items JSON, -- IDs of related evidence
                confidence_score REAL,
                discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Audit trail table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                action TEXT NOT NULL,
                user TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                details JSON,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Chain of custody table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chain_of_custody (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                evidence_number TEXT NOT NULL,
                action TEXT NOT NULL,
                from_person TEXT,
                to_person TEXT,
                location TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hash_verification TEXT,
                notes TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        
        # Create indexes separately
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_comm_timestamp ON communications(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_comm_sender ON communications(sender)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_comm_type ON communications(comm_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_timestamp ON call_logs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contact_name ON contacts(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_timestamp ON locations(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_coords ON locations(latitude, longitude)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_app_name ON applications(app_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_browser_timestamp ON browser_history(visit_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_browser_url ON browser_history(url)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_type ON media_files(file_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_media_hash ON media_files(hash_sha256)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_deleted_type ON deleted_data(data_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cloud_service ON cloud_artifacts(service_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_crypto_address ON crypto_artifacts(wallet_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_social_platform ON social_media(platform)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_social_username ON social_media(username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pattern_type ON forensic_patterns(pattern_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pattern_severity ON forensic_patterns(severity)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_trail(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_custody_timestamp ON chain_of_custody(timestamp)")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Forensic database initialized at {self.db_path}")
    
    def detect_format(self, file_path: str) -> ForensicFormat:
        """
        Detect the forensic extraction format
        
        Args:
            file_path: Path to the forensic extraction file
            
        Returns:
            Detected forensic format
        """
        try:
            if zipfile.is_zipfile(file_path):
                with zipfile.ZipFile(file_path, 'r') as zf:
                    file_list = zf.namelist()
                    
                    for format_type, patterns in self.format_patterns.items():
                        if any(any(pattern in file for pattern in patterns) 
                               for file in file_list):
                            logger.info(f"Detected format: {format_type.value}")
                            return format_type
            
            elif tarfile.is_tarfile(file_path):
                with tarfile.open(file_path, 'r') as tf:
                    file_list = tf.getnames()
                    
                    for format_type, patterns in self.format_patterns.items():
                        if any(any(pattern in file for pattern in patterns) 
                               for file in file_list):
                            logger.info(f"Detected format: {format_type.value}")
                            return format_type
            
            # Check for raw XML or other formats
            with open(file_path, 'rb') as f:
                header = f.read(1024)
                if b'<?xml' in header:
                    return ForensicFormat.GENERIC_UFDR
                    
        except Exception as e:
            logger.error(f"Error detecting format: {e}")
        
        return ForensicFormat.UNKNOWN
    
    def calculate_hashes(self, file_path: str) -> Tuple[str, str, str]:
        """
        Calculate forensic hashes for integrity verification
        
        Args:
            file_path: Path to file
            
        Returns:
            Tuple of (MD5, SHA1, SHA256) hashes
        """
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                md5.update(chunk)
                sha1.update(chunk)
                sha256.update(chunk)
        
        return md5.hexdigest(), sha1.hexdigest(), sha256.hexdigest()
    
    def parse_cellebrite(self, extraction_path: str) -> Dict[str, Any]:
        """Parse Cellebrite UFED/PA format"""
        data = defaultdict(list)
        
        try:
            # Parse device information
            device_info_path = Path(extraction_path) / "ufed_device_info.xml"
            if device_info_path.exists():
                tree = ET.parse(device_info_path)
                root = tree.getroot()
                
                device = DeviceInfo(
                    device_id=root.findtext(".//DeviceID", ""),
                    manufacturer=root.findtext(".//Manufacturer", ""),
                    model=root.findtext(".//Model", ""),
                    os_type=root.findtext(".//OSType", ""),
                    os_version=root.findtext(".//OSVersion", ""),
                    imei=[root.findtext(".//IMEI", "")],
                    serial_number=root.findtext(".//SerialNumber", "")
                )
                data['device_info'] = device.to_dict()
            
            # Parse messages
            messages_path = Path(extraction_path) / "Chats"
            if messages_path.exists():
                for chat_file in messages_path.glob("*.xml"):
                    tree = ET.parse(chat_file)
                    for msg in tree.findall(".//Message"):
                        data['communications'].append({
                            'comm_type': 'instant_message',
                            'sender': msg.findtext("From", ""),
                            'recipient': msg.findtext("To", ""),
                            'text': msg.findtext("Body", ""),
                            'timestamp': msg.findtext("TimeStamp", ""),
                            'app_name': msg.findtext("Source", "WhatsApp")
                        })
            
            # Parse call logs
            calls_path = Path(extraction_path) / "CallLog.xml"
            if calls_path.exists():
                tree = ET.parse(calls_path)
                for call in tree.findall(".//Call"):
                    data['call_logs'].append({
                        'call_type': call.findtext("Type", ""),
                        'caller': call.findtext("From", ""),
                        'callee': call.findtext("To", ""),
                        'timestamp': call.findtext("TimeStamp", ""),
                        'duration': int(call.findtext("Duration", "0"))
                    })
            
        except Exception as e:
            logger.error(f"Error parsing Cellebrite format: {e}")
        
        return dict(data)
    
    def parse_oxygen(self, extraction_path: str) -> Dict[str, Any]:
        """Parse Oxygen Forensics format"""
        data = defaultdict(list)
        
        try:
            # Look for Oxygen database
            db_path = Path(extraction_path) / "oxygen.db"
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Extract messages
                cursor.execute("""
                    SELECT sender, recipient, message, timestamp, app_name
                    FROM messages
                """)
                for row in cursor.fetchall():
                    data['communications'].append({
                        'comm_type': 'message',
                        'sender': row[0],
                        'recipient': row[1],
                        'text': row[2],
                        'timestamp': row[3],
                        'app_name': row[4]
                    })
                
                conn.close()
                
        except Exception as e:
            logger.error(f"Error parsing Oxygen format: {e}")
        
        return dict(data)
    
    def parse_generic_ufdr(self, file_path: str) -> Dict[str, Any]:
        """Parse generic UFDR XML format"""
        data = defaultdict(list)
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Parse device info
            device_elem = root.find(".//device")
            if device_elem is not None:
                device = DeviceInfo(
                    device_id=device_elem.get('id', ''),
                    manufacturer=device_elem.findtext('manufacturer', ''),
                    model=device_elem.findtext('model', ''),
                    os_type=device_elem.findtext('os', ''),
                    os_version=device_elem.findtext('os_version', ''),
                    imei=[device_elem.findtext('imei', '')],
                    serial_number=device_elem.findtext('serial_number', '')
                )
                data['device_info'] = device.to_dict()
            
            # Parse messages
            for msg in root.findall(".//message"):
                data['communications'].append({
                    'comm_type': 'sms',
                    'sender': msg.findtext('sender', ''),
                    'recipient': msg.findtext('recipient', ''),
                    'text': msg.findtext('text', ''),
                    'timestamp': msg.findtext('timestamp', ''),
                    'app_name': msg.findtext('application', 'SMS')
                })
            
            # Parse calls
            for call in root.findall(".//call"):
                data['call_logs'].append({
                    'call_type': call.findtext('type', 'unknown'),
                    'caller': call.findtext('caller', ''),
                    'callee': call.findtext('callee', ''),
                    'timestamp': call.findtext('timestamp', ''),
                    'duration': int(call.findtext('duration', '0'))
                })
            
            # Parse contacts - handle both lowercase and uppercase tags
            contact_elements = (
                root.findall(".//contact") + 
                root.findall(".//Contact")
            )
            
            for contact in contact_elements:
                # Handle both tag variations
                contact_id = (
                    contact.findtext('id') or 
                    contact.findtext('Id') or 
                    contact.get('id', '')
                )
                
                name = (
                    contact.findtext('name') or 
                    contact.findtext('Name') or 
                    contact.findtext('display_name') or 
                    contact.findtext('DisplayName') or ''
                )
                
                # Get phone numbers
                phones = []
                phone_elements = (
                    contact.findall('.//phone') + 
                    contact.findall('.//Phone')
                )
                for p in phone_elements:
                    if p.text:
                        phones.append(p.text)
                
                # Also check for direct phone text
                phone_text = contact.findtext('phone') or contact.findtext('Phone')
                if phone_text and phone_text not in phones:
                    phones.append(phone_text)
                
                # Get emails
                emails = []
                email_elements = (
                    contact.findall('.//email') + 
                    contact.findall('.//Email')
                )
                for e in email_elements:
                    if e.text:
                        emails.append(e.text)
                
                # Also check for direct email text
                email_text = contact.findtext('email') or contact.findtext('Email')
                if email_text and email_text not in emails:
                    emails.append(email_text)
                
                # Only add if we have a name or phone
                if name or phones:
                    data['contacts'].append({
                        'contact_id': contact_id,
                        'name': name,
                        'phone_numbers': phones,
                        'emails': emails
                    })
            
            # Parse locations
            for loc in root.findall(".//location"):
                data['locations'].append({
                    'timestamp': loc.findtext('timestamp', ''),
                    'latitude': float(loc.findtext('latitude', '0')),
                    'longitude': float(loc.findtext('longitude', '0')),
                    'accuracy': float(loc.findtext('accuracy', '0')),
                    'address': loc.findtext('address', '')
                })
            
        except Exception as e:
            logger.error(f"Error parsing generic UFDR: {e}")
        
        return dict(data)
    
    def detect_suspicious_patterns(self, data: Dict[str, Any]) -> List[Dict]:
        """
        Detect suspicious patterns in forensic data
        
        Args:
            data: Parsed forensic data
            
        Returns:
            List of detected patterns
        """
        patterns = []
        
        # Check for cryptocurrency addresses
        crypto_pattern = re.compile(r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b|0x[a-fA-F0-9]{40}')
        
        # Check for foreign phone numbers
        foreign_pattern = re.compile(r'\+(?!91)[0-9]{1,3}[0-9\s-]+')
        
        # Check for suspicious keywords
        suspicious_keywords = [
            'bomb', 'explosive', 'drug', 'cocaine', 'heroin', 'weapon',
            'murder', 'kill', 'ransom', 'bitcoin', 'crypto', 'hawala',
            'terror', 'jihad', 'smuggle', 'launder', 'bribe'
        ]
        
        # Analyze communications
        for comm in data.get('communications', []):
            text = comm.get('text', '').lower()
            
            # Check for crypto addresses
            if crypto_pattern.search(text):
                patterns.append({
                    'pattern_type': 'cryptocurrency',
                    'pattern_name': 'Crypto Address Detected',
                    'description': f"Cryptocurrency address found in message",
                    'severity': 'high',
                    'confidence_score': 0.9,
                    'evidence_items': [comm]
                })
            
            # Check for suspicious keywords
            for keyword in suspicious_keywords:
                if keyword in text:
                    patterns.append({
                        'pattern_type': 'suspicious_content',
                        'pattern_name': f'Keyword: {keyword}',
                        'description': f"Suspicious keyword '{keyword}' detected",
                        'severity': 'medium',
                        'confidence_score': 0.7,
                        'evidence_items': [comm]
                    })
            
            # Check for foreign contacts
            sender = comm.get('sender', '')
            if foreign_pattern.match(sender):
                patterns.append({
                    'pattern_type': 'foreign_contact',
                    'pattern_name': 'International Communication',
                    'description': f"Communication with foreign number: {sender}",
                    'severity': 'low',
                    'confidence_score': 0.8,
                    'evidence_items': [comm]
                })
        
        return patterns
    
    def store_data(self, data: Dict[str, Any], integrity: ForensicIntegrity) -> bool:
        """
        Store parsed data in forensic database
        
        Args:
            data: Parsed forensic data
            integrity: Forensic integrity information
            
        Returns:
            Success status
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Start transaction
            cursor.execute("BEGIN TRANSACTION")
            
            # Insert case record
            cursor.execute("""
                INSERT OR REPLACE INTO cases 
                (case_id, evidence_number, examiner_name, agency, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self.case_id,
                self.evidence_number,
                self.examiner_name,
                self.agency,
                'completed',
                json.dumps(integrity.to_dict())
            ))
            
            # Store device info (with conflict handling)
            device_info = data.get('device_info', {})
            if device_info:
                cursor.execute("""
                    INSERT OR REPLACE INTO devices 
                    (case_id, device_id, manufacturer, model, os_type, os_version,
                     imei, serial_number, phone_numbers, metadata, owner)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    device_info.get('device_id', 'unknown'),
                    device_info.get('manufacturer'),
                    device_info.get('model'),
                    device_info.get('os_type'),
                    device_info.get('os_version'),
                    json.dumps(device_info.get('imei', [])),
                    device_info.get('serial_number'),
                    json.dumps(device_info.get('phone_numbers', [])),
                    json.dumps(device_info),
                    device_info.get('owner')
                ))
            
            # Store communications
            for comm in data.get('communications', []):
                cursor.execute("""
                    INSERT INTO communications
                    (case_id, comm_type, sender, recipient, text, timestamp,
                     app_name, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    comm.get('comm_type', 'unknown'),
                    comm.get('sender'),
                    comm.get('recipient'),
                    comm.get('text'),
                    comm.get('timestamp'),
                    comm.get('app_name'),
                    json.dumps(comm)
                ))
                self.stats['communications'] += 1
            
            # Store call logs
            for call in data.get('call_logs', []):
                cursor.execute("""
                    INSERT INTO call_logs
                    (case_id, call_type, caller, callee, timestamp, duration, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    call.get('call_type'),
                    call.get('caller'),
                    call.get('callee'),
                    call.get('timestamp'),
                    call.get('duration', 0),
                    json.dumps(call)
                ))
                self.stats['call_logs'] += 1
            
            # Store contacts
            for contact in data.get('contacts', []):
                cursor.execute("""
                    INSERT INTO contacts
                    (case_id, contact_id, name, phone_numbers, emails, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    contact.get('contact_id'),
                    contact.get('name'),
                    json.dumps(contact.get('phone_numbers', [])),
                    json.dumps(contact.get('emails', [])),
                    json.dumps(contact)
                ))
                self.stats['contacts'] += 1
            
            # Store locations
            for location in data.get('locations', []):
                cursor.execute("""
                    INSERT INTO locations
                    (case_id, timestamp, latitude, longitude, accuracy, address, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    location.get('timestamp'),
                    location.get('latitude'),
                    location.get('longitude'),
                    location.get('accuracy'),
                    location.get('address'),
                    json.dumps(location)
                ))
                self.stats['locations'] += 1
            
            # Store detected patterns
            patterns = self.detect_suspicious_patterns(data)
            for pattern in patterns:
                cursor.execute("""
                    INSERT INTO forensic_patterns
                    (case_id, pattern_type, pattern_name, description, severity,
                     confidence_score, evidence_items, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.case_id,
                    pattern['pattern_type'],
                    pattern['pattern_name'],
                    pattern['description'],
                    pattern['severity'],
                    pattern['confidence_score'],
                    json.dumps(pattern.get('evidence_items', [])),
                    json.dumps(pattern)
                ))
                self.stats['patterns'] += 1
            
            # Add audit trail entry
            cursor.execute("""
                INSERT INTO audit_trail (case_id, action, user, details)
                VALUES (?, ?, ?, ?)
            """, (
                self.case_id,
                'data_ingestion',
                self.examiner_name,
                json.dumps({
                    'statistics': dict(self.stats),
                    'timestamp': datetime.now().isoformat()
                })
            ))
            
            # Commit transaction
            cursor.execute("COMMIT")
            
            logger.info(f"Successfully stored data for case {self.case_id}")
            logger.info(f"Statistics: {dict(self.stats)}")
            
            return True
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Error storing data: {e}")
            raise
            
        finally:
            conn.close()
    
    def process_ufdr(self, file_path: str, warrant_number: str = None) -> Dict:
        """
        Main processing method for UFDR files
        
        Args:
            file_path: Path to UFDR file
            warrant_number: Optional warrant number for legal compliance
            
        Returns:
            Processing results and statistics
        """
        logger.info(f"Processing UFDR file: {file_path}")
        logger.info(f"Case ID: {self.case_id}")
        logger.info(f"Evidence Number: {self.evidence_number}")
        
        # Calculate hashes for integrity
        md5, sha1, sha256 = self.calculate_hashes(file_path)
        
        # Create forensic integrity record
        integrity = ForensicIntegrity(
            case_number=self.case_id,
            evidence_number=self.evidence_number,
            examiner_name=self.examiner_name,
            examiner_badge="",
            agency=self.agency,
            acquisition_date=datetime.now(timezone.utc),
            acquisition_tool="Advanced UFDR Parser",
            acquisition_version="2.0",
            device_seized_date=datetime.now(timezone.utc),
            device_seized_location="Evidence Room",
            device_owner="Under Investigation",
            warrant_number=warrant_number,
            hash_md5=md5,
            hash_sha1=sha1,
            hash_sha256=sha256
        )
        
        # Create extraction directory
        extract_dir = Path(f"temp/{self.case_id}")
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Detect format
            format_type = self.detect_format(file_path)
            
            # Extract if needed
            if zipfile.is_zipfile(file_path):
                with zipfile.ZipFile(file_path, 'r') as zf:
                    zf.extractall(extract_dir)
                extraction_path = str(extract_dir)
            elif tarfile.is_tarfile(file_path):
                with tarfile.open(file_path, 'r') as tf:
                    tf.extractall(extract_dir)
                extraction_path = str(extract_dir)
            else:
                extraction_path = file_path
            
            # Parse based on format
            if format_type == ForensicFormat.CELLEBRITE_UFED:
                data = self.parse_cellebrite(extraction_path)
            elif format_type == ForensicFormat.CELLEBRITE_PA:
                data = self.parse_cellebrite(extraction_path)
            elif format_type == ForensicFormat.OXYGEN:
                data = self.parse_oxygen(extraction_path)
            elif format_type == ForensicFormat.GENERIC_UFDR:
                # Find XML file if extracted
                if extraction_path != file_path:
                    xml_files = list(Path(extraction_path).glob("*.xml"))
                    if xml_files:
                        data = self.parse_generic_ufdr(str(xml_files[0]))
                    else:
                        data = {}
                else:
                    data = self.parse_generic_ufdr(extraction_path)
            else:
                logger.warning(f"Unsupported format: {format_type}")
                # Try generic parsing anyway
                data = self.parse_generic_ufdr(extraction_path)
            
            # Store data in database
            self.store_data(data, integrity)
            
            # Save parsed data as JSON for other components
            output_dir = Path(f"data/parsed/{self.case_id}")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for category, items in data.items():
                if items:
                    output_file = output_dir / f"{category}.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(items, f, indent=2, ensure_ascii=False)
            
            # Create processing manifest
            manifest = {
                'case_id': self.case_id,
                'evidence_number': self.evidence_number,
                'examiner': self.examiner_name,
                'agency': self.agency,
                'processing_date': datetime.now().isoformat(),
                'format_detected': format_type.value,
                'integrity': integrity.to_dict(),
                'statistics': dict(self.stats),
                'status': 'success'
            }
            
            # Save manifest
            manifest_path = output_dir / "manifest.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            logger.info("UFDR processing completed successfully")
            return manifest
            
        except Exception as e:
            logger.error(f"Error processing UFDR: {e}")
            raise
            
        finally:
            # Cleanup
            import shutil
            if extract_dir.exists():
                shutil.rmtree(extract_dir)


if __name__ == "__main__":
    # Test the advanced parser
    parser = AdvancedUFDRParser(
        case_id="CASE_2025_001",
        evidence_number="EVD_001",
        examiner_name="Inspector Kumar",
        agency="Cyber Crime Division"
    )
    
    # Test with a sample file
    test_files = list(Path("data/ufdr_files").glob("*.ufdr"))
    if test_files:
        result = parser.process_ufdr(
            str(test_files[0]),
            warrant_number="WRT_2025_001"
        )
        print(f"Processing result: {json.dumps(result, indent=2)}")
    else:
        print("No test files found")