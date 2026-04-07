"""
UFDR Ingestor Module
Handles complete ingestion pipeline for UFDR files
"""

import os
import json
import sqlite3
import hashlib
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UFDRIngestor:
    """Main class for ingesting UFDR files"""

    def __init__(self, db_path: str = "data/ufdr_analysis.db"):
        """Initialize the ingestor with database connection"""
        self.db_path = db_path
        self._ensure_database()

    def _ensure_database(self):
        """Create database tables if they don't exist"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                case_id TEXT PRIMARY KEY,
                operator TEXT,
                created_at TEXT,
                source_file TEXT,
                file_hash TEXT,
                status TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT,
                message_id TEXT,
                sender TEXT,
                recipient TEXT,
                text TEXT,
                timestamp TEXT,
                application TEXT,
                thread_id TEXT,
                attachments TEXT,
                metadata TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT,
                call_id TEXT,
                caller TEXT,
                callee TEXT,
                timestamp TEXT,
                duration INTEGER,
                call_type TEXT,
                metadata TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT,
                contact_id TEXT,
                name TEXT,
                phone_numbers TEXT,
                emails TEXT,
                addresses TEXT,
                metadata TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT,
                location_id TEXT,
                timestamp TEXT,
                latitude REAL,
                longitude REAL,
                accuracy REAL,
                address TEXT,
                metadata TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT,
                device_id TEXT,
                manufacturer TEXT,
                model TEXT,
                os TEXT,
                imei TEXT,
                serial_number TEXT,
                metadata TEXT,
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)

        # Create indexes for better query performance
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_case_id ON messages(case_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_calls_case_id ON calls(case_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_contacts_case_id ON contacts(case_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_locations_case_id ON locations(case_id)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_calls_timestamp ON calls(timestamp)")

        conn.commit()
        conn.close()

        logger.info(f"Database initialized at {self.db_path}")

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _extract_ufdr(self, file_path: str, extract_dir: str) -> str:
        """Extract UFDR (ZIP) file and return path to XML"""
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Find the main XML file
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.xml'):
                    return os.path.join(root, file)

        raise ValueError("No XML file found in UFDR archive")

    def _parse_xml_data(self, xml_path: str) -> Dict:
        """Parse XML data from UFDR file"""
        tree = ET.parse(xml_path)
        root = tree.getroot()

        data = {
            'messages': [],
            'calls': [],
            'contacts': [],
            'locations': [],
            'devices': []
        }

        # Parse messages
        for msg in root.findall('.//message'):
            message_data = {
                'message_id': msg.get('id', ''),
                'sender': msg.findtext('sender', ''),
                'recipient': msg.findtext('recipient', ''),
                'text': msg.findtext('text', ''),
                'timestamp': msg.findtext('timestamp', ''),
                'application': msg.findtext('application', ''),
                'thread_id': msg.findtext('thread_id', ''),
                'attachments': [],
                'metadata': {}
            }

            # Get attachments
            for att in msg.findall('.//attachment'):
                message_data['attachments'].append(att.text)

            # Extract metadata
            for meta in msg.findall('.//metadata/*'):
                message_data['metadata'][meta.tag] = meta.text

            data['messages'].append(message_data)

        # Parse calls
        for call in root.findall('.//call'):
            call_data = {
                'call_id': call.get('id', ''),
                'caller': call.findtext('caller', ''),
                'callee': call.findtext('callee', ''),
                'timestamp': call.findtext('timestamp', ''),
                'duration': int(call.findtext('duration', '0')),
                'call_type': call.findtext('type', 'unknown'),
                'metadata': {}
            }

            # Extract metadata
            for meta in call.findall('.//metadata/*'):
                call_data['metadata'][meta.tag] = meta.text

            data['calls'].append(call_data)

        # Parse contacts - handle both lowercase and uppercase tags
        contact_elements = (
            root.findall('.//contact') +
            root.findall('.//Contact')
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

            contact_data = {
                'contact_id': contact_id,
                'name': name,
                'phone_numbers': [],
                'emails': [],
                'addresses': [],
                'metadata': {}
            }

            # Get phone numbers - handle multiple tag variations
            phone_elements = (
                contact.findall('.//phone') +
                contact.findall('.//Phone') +
                contact.findall('.//number') +
                contact.findall('.//Number')
            )

            for phone_elem in phone_elements:
                if phone_elem.text and phone_elem.text.strip():
                    contact_data['phone_numbers'].append(
                        phone_elem.text.strip())

            # Also check for direct phone text (our XML structure)
            phone_text = contact.findtext('phone') or contact.findtext('Phone')
            if phone_text and phone_text not in contact_data['phone_numbers']:
                contact_data['phone_numbers'].append(phone_text)

            # Get emails - handle multiple tag variations
            email_elements = (
                contact.findall('.//email') +
                contact.findall('.//Email') +
                contact.findall('.//mail') +
                contact.findall('.//Mail')
            )

            for email_elem in email_elements:
                if email_elem.text and email_elem.text.strip():
                    contact_data['emails'].append(email_elem.text.strip())

            # Also check for direct email text
            email_text = contact.findtext('email') or contact.findtext('Email')
            if email_text and email_text not in contact_data['emails']:
                contact_data['emails'].append(email_text)

            # Get addresses
            addr_elements = (
                contact.findall('.//address') +
                contact.findall('.//Address')
            )

            for addr_elem in addr_elements:
                if addr_elem.text and addr_elem.text.strip():
                    contact_data['addresses'].append(addr_elem.text.strip())

            # Get company info as metadata
            company = contact.findtext(
                'company') or contact.findtext('Company')
            if company:
                contact_data['metadata']['company'] = company

            # Extract other metadata
            for meta in contact.findall('.//metadata/*'):
                contact_data['metadata'][meta.tag] = meta.text

            # Only add if we have a name or phone number
            if contact_data['name'] or contact_data['phone_numbers']:
                data['contacts'].append(contact_data)

        # Parse locations
        for loc in root.findall('.//location'):
            location_data = {
                'location_id': loc.get('id', ''),
                'timestamp': loc.findtext('timestamp', ''),
                'latitude': float(loc.findtext('latitude', '0')),
                'longitude': float(loc.findtext('longitude', '0')),
                'accuracy': float(loc.findtext('accuracy', '0')),
                'address': loc.findtext('address', ''),
                'metadata': {}
            }

            # Extract metadata
            for meta in loc.findall('.//metadata/*'):
                location_data['metadata'][meta.tag] = meta.text

            data['locations'].append(location_data)

        # Parse device info
        for device in root.findall('.//device'):
            device_data = {
                'device_id': device.get('id', ''),
                'manufacturer': device.findtext('manufacturer', ''),
                'model': device.findtext('model', ''),
                'os': device.findtext('os', ''),
                'imei': device.findtext('imei', ''),
                'serial_number': device.findtext('serial_number', ''),
                'metadata': {}
            }

            # Extract metadata
            for meta in device.findall('.//metadata/*'):
                device_data['metadata'][meta.tag] = meta.text

            data['devices'].append(device_data)

        return data

    def _store_data(self, case_id: str, data: Dict) -> Dict:
        """Store parsed data in SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        statistics = {
            'messages': 0,
            'calls': 0,
            'contacts': 0,
            'locations': 0,
            'devices': 0
        }

        try:
            # ⚡ BOLT OPTIMIZATION: Replaced N+1 row-by-row inserts with executemany
            # Expected impact: Dramatically faster database insertions (approx 10x-50x speedup)
            # by avoiding individual statement parsing and context switching
            # overhead.

            # Store messages
            if data['messages']:
                cursor.executemany("""
                    INSERT INTO messages (case_id, message_id, sender, recipient,
                                        text, timestamp, application, thread_id,
                                        attachments, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [(
                    case_id,
                    msg['message_id'],
                    msg['sender'],
                    msg['recipient'],
                    msg['text'],
                    msg['timestamp'],
                    msg['application'],
                    msg['thread_id'],
                    json.dumps(msg['attachments']),
                    json.dumps(msg['metadata'])
                ) for msg in data['messages']])
                statistics['messages'] += len(data['messages'])

            # Store calls
            if data['calls']:
                cursor.executemany("""
                    INSERT INTO calls (case_id, call_id, caller, callee, timestamp,
                                     duration, call_type, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [(
                    case_id,
                    call['call_id'],
                    call['caller'],
                    call['callee'],
                    call['timestamp'],
                    call['duration'],
                    call['call_type'],
                    json.dumps(call['metadata'])
                ) for call in data['calls']])
                statistics['calls'] += len(data['calls'])

            # Store contacts
            if data['contacts']:
                cursor.executemany("""
                    INSERT INTO contacts (case_id, contact_id, name, phone_numbers,
                                        emails, addresses, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [(
                    case_id,
                    contact['contact_id'],
                    contact['name'],
                    json.dumps(contact['phone_numbers']),
                    json.dumps(contact['emails']),
                    json.dumps(contact['addresses']),
                    json.dumps(contact['metadata'])
                ) for contact in data['contacts']])
                statistics['contacts'] += len(data['contacts'])

            # Store locations
            if data['locations']:
                cursor.executemany("""
                    INSERT INTO locations (case_id, location_id, timestamp, latitude,
                                         longitude, accuracy, address, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [(
                    case_id,
                    loc['location_id'],
                    loc['timestamp'],
                    loc['latitude'],
                    loc['longitude'],
                    loc['accuracy'],
                    loc['address'],
                    json.dumps(loc['metadata'])
                ) for loc in data['locations']])
                statistics['locations'] += len(data['locations'])

            # Store devices
            if data['devices']:
                cursor.executemany("""
                    INSERT INTO devices (case_id, device_id, manufacturer, model,
                                       os, imei, serial_number, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [(
                    case_id,
                    device['device_id'],
                    device['manufacturer'],
                    device['model'],
                    device['os'],
                    device['imei'],
                    device['serial_number'],
                    json.dumps(device['metadata'])
                ) for device in data['devices']])
                statistics['devices'] += len(data['devices'])

            conn.commit()
            logger.info(f"Stored data for case {case_id}: {statistics}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error storing data: {e}")
            raise
        finally:
            conn.close()

        return statistics

    def _save_parsed_data(self, case_id: str, data: Dict):
        """Save parsed data as JSON files for vector indexing"""
        output_dir = Path("data/parsed") / case_id
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save each data type
        for data_type, items in data.items():
            if items:
                file_path = output_dir / f"{data_type}.json"
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(items, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(items)} {data_type} to {file_path}")

    def ingest(
            self,
            file_path: str,
            case_id: str = None,
            operator: str = None) -> Dict:
        """
        Main ingestion method

        Args:
            file_path: Path to UFDR file
            case_id: Case identifier
            operator: Operator name

        Returns:
            Ingestion manifest with statistics
        """
        # Generate case ID if not provided
        if not case_id:
            case_id = f"case_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Starting ingestion for case: {case_id}")

        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path)

        # Create temporary directory for extraction
        temp_dir = Path("temp") / case_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Extract UFDR file
            logger.info("Extracting UFDR file...")
            xml_path = self._extract_ufdr(file_path, str(temp_dir))

            # Parse XML data
            logger.info("Parsing XML data...")
            data = self._parse_xml_data(xml_path)

            # Store in database
            logger.info("Storing data in database...")
            statistics = self._store_data(case_id, data)

            # Save parsed data for vector indexing
            logger.info("Saving parsed data...")
            self._save_parsed_data(case_id, data)

            # Update case record
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cases (case_id, operator, created_at,
                                            source_file, file_hash, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                case_id,
                operator or "Unknown",
                datetime.now().isoformat(),
                os.path.basename(file_path),
                file_hash,
                "completed"
            ))
            conn.commit()
            conn.close()

            # Create manifest
            manifest = {
                'case_id': case_id,
                'operator': operator,
                'timestamp': datetime.now().isoformat(),
                'source_file': {
                    'name': os.path.basename(file_path),
                    'size': os.path.getsize(file_path),
                    'sha256': file_hash
                },
                'parsing': {
                    'status': 'success',
                    'statistics': statistics
                }
            }

            # Save manifest
            manifest_path = Path("data/parsed") / case_id / "manifest.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)

            logger.info(
                f"Ingestion completed successfully for case: {case_id}")
            return manifest

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            # Update case status
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE cases SET status = 'failed' WHERE case_id = ?
            """, (case_id,))
            conn.commit()
            conn.close()
            raise

        finally:
            # Cleanup temporary files
            import shutil
            if temp_dir.exists():
                shutil.rmtree(temp_dir)


if __name__ == "__main__":
    # Test the ingestor
    ingestor = UFDRIngestor()

    # Test with a sample UFDR file
    ufdr_files = list(Path("data/ufdr_files").glob("*.ufdr"))
    if ufdr_files:
        test_file = ufdr_files[0]
        print(f"Testing with: {test_file}")

        manifest = ingestor.ingest(
            str(test_file),
            case_id=f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            operator="Test Operator"
        )

        print("Ingestion successful!")
        print(f"Statistics: {manifest['parsing']['statistics']}")
    else:
        print("No UFDR files found for testing")
