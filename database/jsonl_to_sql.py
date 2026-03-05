"""
JSONL to SQL Ingestion Pipeline
Converts canonical JSONL files to indexed SQL database for fast querying
"""

import json
import logging
from pathlib import Path
from datetime import datetime
import uuid

from database.schema import (
    DatabaseManager, Case, Device, Contact, Message, Call, Media, Location,
    normalize_phone_to_digits
)

logger = logging.getLogger(__name__)


class JSONLToSQLIngester:
    """Ingests canonical JSONL files into SQL database"""
    
    def __init__(self, db_path: str = "data/forensics.db"):
        """
        Initialize ingester
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_manager = DatabaseManager(db_path)
        self.db_manager.create_schema()
        self.stats = {
            'cases': 0,
            'devices': 0,
            'contacts': 0,
            'messages': 0,
            'calls': 0,
            'media': 0,
            'locations': 0,
        }
        
    def ingest_case(self, 
                    case_id: str,
                    canonical_dir: Path,
                    examiner: str = "",
                    agency: str = "",
                    source_file: str = "") -> None:
        """
        Ingest a complete case from canonical JSONL files
        
        Args:
            case_id: Unique case identifier
            canonical_dir: Directory containing JSONL files
            examiner: Forensic examiner name
            agency: Law enforcement agency
            source_file: Original UFDR file name
        """
        session = self.db_manager.get_session()
        
        try:
            # Create case record
            case = Case(
                case_id=case_id,
                ingest_time=datetime.utcnow(),
                source_file=source_file,
                examiner=examiner,
                agency=agency
            )
            session.add(case)
            session.commit()
            self.stats['cases'] += 1
            logger.info(f"Created case: {case_id}")
            
            # Ingest each data type
            self._ingest_devices(session, canonical_dir / "devices.jsonl", case_id)
            self._ingest_contacts(session, canonical_dir / "contacts.jsonl", case_id)
            self._ingest_messages(session, canonical_dir / "messages.jsonl", case_id)
            self._ingest_calls(session, canonical_dir / "calls.jsonl", case_id)
            self._ingest_media(session, canonical_dir / "media.jsonl", case_id)
            self._ingest_locations(session, canonical_dir / "locations.jsonl", case_id)
            
            logger.info(f"[OK] Case {case_id} ingestion complete")
            self.print_stats()
            
        except Exception as e:
            session.rollback()
            logger.error(f"[ERROR] Error ingesting case {case_id}: {e}")
            raise
        finally:
            session.close()
    
    def _ingest_devices(self, session, jsonl_path: Path, case_id: str):
        """Ingest devices from JSON/JSONL"""
        # Try .jsonl first, then .json
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Devices file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                # Read line by line
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_device(session, data, case_id)
            else:
                # Read as JSON array
                devices_list = json.load(f)
                if not isinstance(devices_list, list):
                    devices_list = [devices_list]
                for data in devices_list:
                    self._add_device(session, data, case_id)
        
        session.commit()
        logger.info(f"  Ingested {self.stats['devices']} devices")
    
    def _add_device(self, session, data: dict, case_id: str):
        """Add a single device to database"""
        device = Device(
            device_id=data.get('device_id', str(uuid.uuid4())),
            case_id=case_id,
            imei=data.get('imei'),
            serial_number=data.get('serial_number'),
            manufacturer=data.get('manufacturer'),
            model=data.get('model'),
            os_type=data.get('os_type'),
            os_version=data.get('os_version'),
            owner=data.get('owner')
        )
        session.add(device)
        self.stats['devices'] += 1
    
    def _ingest_contacts(self, session, jsonl_path: Path, case_id: str):
        """Ingest contacts from JSON/JSONL"""
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Contacts file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_contact(session, data, case_id)
            else:
                contacts_list = json.load(f)
                if not isinstance(contacts_list, list):
                    contacts_list = [contacts_list]
                for data in contacts_list:
                    self._add_contact(session, data, case_id)
        
        session.commit()
        logger.info(f"  Ingested {self.stats['contacts']} contacts")
    
    def _add_contact(self, session, data: dict, case_id: str):
        """Add a single contact to database"""
        phone_raw = data.get('phone') or data.get('phone_number')
        phone_digits = normalize_phone_to_digits(phone_raw) if phone_raw else ''
        
        contact = Contact(
            contact_id=data.get('contact_id', str(uuid.uuid4())),
            case_id=case_id,
            name=data.get('name'),
            phone_raw=phone_raw,
            phone_digits=phone_digits,
            phone_e164=data.get('phone_e164'),
            email=data.get('email')
        )
        session.add(contact)
        self.stats['contacts'] += 1
    
    def _ingest_messages(self, session, jsonl_path: Path, case_id: str):
        """Ingest messages from JSON/JSONL"""
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Messages file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_message(session, data, case_id)
                        if self.stats['messages'] % 1000 == 0:
                            session.commit()
                            logger.info(f"  Ingested {self.stats['messages']} messages...")
            else:
                messages_list = json.load(f)
                if not isinstance(messages_list, list):
                    messages_list = [messages_list]
                for data in messages_list:
                    self._add_message(session, data, case_id)
                    if self.stats['messages'] % 1000 == 0:
                        session.commit()
                        logger.info(f"  Ingested {self.stats['messages']} messages...")
        
        session.commit()
        logger.info(f"  [OK] Ingested {self.stats['messages']} messages")
    
    def _add_message(self, session, data: dict, case_id: str):
        """Add a single message to database"""
        # Normalize phone numbers
        sender_raw = data.get('from_person') or data.get('from') or data.get('sender')
        receiver_raw = data.get('to_person') or data.get('to') or data.get('receiver')
        
        sender_digits = normalize_phone_to_digits(sender_raw) if sender_raw else ''
        receiver_digits = normalize_phone_to_digits(receiver_raw) if receiver_raw else ''
        
        # Parse timestamp
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = None
        
        message = Message(
            msg_id=data.get('id', str(uuid.uuid4())),
            case_id=case_id,
            device_id=data.get('device_id'),
            app=data.get('app', 'unknown'),
            sender_raw=sender_raw,
            sender_digits=sender_digits,
            receiver_raw=receiver_raw,
            receiver_digits=receiver_digits,
            text=data.get('text'),
            message_type=data.get('message_type', 'text'),
            timestamp=timestamp,
            encrypted=data.get('encrypted', False),
            is_deleted=data.get('is_deleted', False),
            source_path=data.get('source_path')
        )
        session.add(message)
        self.stats['messages'] += 1
    
    def _ingest_calls(self, session, jsonl_path: Path, case_id: str):
        """Ingest calls from JSON/JSONL"""
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Calls file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_call(session, data, case_id)
            else:
                calls_list = json.load(f)
                if not isinstance(calls_list, list):
                    calls_list = [calls_list]
                for data in calls_list:
                    self._add_call(session, data, case_id)
        
        session.commit()
        logger.info(f"  Ingested {self.stats['calls']} calls")
    
    def _add_call(self, session, data: dict, case_id: str):
        """Add a single call to database"""
        # Normalize phone numbers  
        caller_raw = data.get('from_person') or data.get('caller')
        receiver_raw = data.get('to_person') or data.get('receiver')
        
        caller_digits = normalize_phone_to_digits(caller_raw) if caller_raw else ''
        receiver_digits = normalize_phone_to_digits(receiver_raw) if receiver_raw else ''
        
        # Parse timestamp
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = None
        
        call = Call(
            call_id=data.get('id', str(uuid.uuid4())),
            case_id=case_id,
            device_id=data.get('device_id'),
            caller_raw=caller_raw,
            caller_digits=caller_digits,
            receiver_raw=receiver_raw,
            receiver_digits=receiver_digits,
            timestamp=timestamp,
            duration_seconds=data.get('duration_seconds'),
            direction=data.get('direction', 'unknown'),
            source_path=data.get('source_path')
        )
        session.add(call)
        self.stats['calls'] += 1
    
    def _ingest_media(self, session, jsonl_path: Path, case_id: str):
        """Ingest media from JSON/JSONL"""
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Media file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_media(session, data, case_id)
            else:
                media_list = json.load(f)
                if not isinstance(media_list, list):
                    media_list = [media_list]
                for data in media_list:
                    self._add_media(session, data, case_id)
        
        session.commit()
        logger.info(f"  Ingested {self.stats['media']} media items")
    
    def _add_media(self, session, data: dict, case_id: str):
        """Add a single media item to database"""
        sha256_hash = data.get('sha256')
        
        # Check if media with this SHA256 already exists
        if sha256_hash:
            existing_media = session.query(Media).filter_by(sha256=sha256_hash).first()
            if existing_media:
                logger.debug(f"Media with SHA256 {sha256_hash[:16]}... already exists, skipping")
                return  # Skip duplicate
        
        # Parse timestamp
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = None
        
        media = Media(
            media_id=data.get('id', str(uuid.uuid4())),
            case_id=case_id,
            device_id=data.get('device_id'),
            filename=data.get('filename'),
            media_type=data.get('media_type'),
            sha256=sha256_hash,
            phash=data.get('phash'),
            ocr_text=data.get('ocr_text'),
            caption=data.get('caption'),
            timestamp=timestamp,
            file_size=data.get('file_size'),
            source_path=data.get('source_path')
        )
        session.add(media)
        self.stats['media'] += 1
    
    def _ingest_locations(self, session, jsonl_path: Path, case_id: str):
        """Ingest locations from JSON/JSONL"""
        json_path = jsonl_path.parent / jsonl_path.name.replace('.jsonl', '.json')
        
        if jsonl_path.exists():
            path_to_use = jsonl_path
            is_jsonl = True
        elif json_path.exists():
            path_to_use = json_path
            is_jsonl = False
        else:
            logger.warning(f"Locations file not found: {jsonl_path} or {json_path}")
            return
            
        with open(path_to_use, 'r', encoding='utf-8') as f:
            if is_jsonl:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        self._add_location(session, data, case_id)
            else:
                locations_list = json.load(f)
                if not isinstance(locations_list, list):
                    locations_list = [locations_list]
                for data in locations_list:
                    self._add_location(session, data, case_id)
        
        session.commit()
        logger.info(f"  Ingested {self.stats['locations']} locations")
    
    def _add_location(self, session, data: dict, case_id: str):
        """Add a single location to database"""
        # Parse timestamp
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = None
        
        location = Location(
            location_id=data.get('id', str(uuid.uuid4())),
            case_id=case_id,
            device_id=data.get('device_id'),
            latitude=data.get('latitude'),
            longitude=data.get('longitude'),
            accuracy=data.get('accuracy'),
            altitude=data.get('altitude'),
            timestamp=timestamp,
            source_path=data.get('source_path')
        )
        session.add(location)
        self.stats['locations'] += 1
    
    def print_stats(self):
        """Print ingestion statistics"""
        print("\n" + "="*60)
        print(" Ingestion Statistics")
        print("="*60)
        for key, value in self.stats.items():
            print(f"  {key.capitalize()}: {value:,}")
        print("="*60 + "\n")


# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest canonical JSONL into SQL database")
    parser.add_argument("case_id", help="Case ID")
    parser.add_argument("canonical_dir", help="Path to canonical JSONL directory")
    parser.add_argument("--db", default="data/forensics.db", help="Database path")
    parser.add_argument("--examiner", default="", help="Examiner name")
    parser.add_argument("--agency", default="", help="Agency name")
    parser.add_argument("--source", default="", help="Source UFDR file")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    ingester = JSONLToSQLIngester(args.db)
    ingester.ingest_case(
        case_id=args.case_id,
        canonical_dir=Path(args.canonical_dir),
        examiner=args.examiner,
        agency=args.agency,
        source_file=args.source
    )