"""
Canonical JSONL Ingestion Pipeline
Transforms parsed UFDR data into canonical format with:
- JSONL storage for efficient streaming
- SHA256 integrity verification
- Perceptual hashing for media
- Normalized timestamps and phone numbers
"""

import os
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Generator
import logging

# Import canonical models
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from models.canonical_models import (
    Case, Device, Person, Message, Call, Media, Location, IngestManifest,
    MessageType, CallDirection, MediaType,
    normalize_timestamp, normalize_phone_number
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import name detector (Step 7)
NAME_DETECTOR_AVAILABLE = False
try:
    # Import directly from file to avoid circular dependencies
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "name_detector",
        Path(__file__).parent.parent / "ingest" / "name_detector.py"
    )
    if spec and spec.loader:
        name_detector_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(name_detector_module)
        get_name_detector = name_detector_module.get_name_detector
        NAME_DETECTOR_AVAILABLE = True
        logger.info("Name detector (Step 7) loaded successfully")
except Exception as e:
    logger.warning(f"Name detector (Step 7) not available: {e}")
    get_name_detector = None

# Import location enricher (Step 10)
LOCATION_ENRICHER_AVAILABLE = False
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "location_enricher",
        Path(__file__).parent.parent / "ingest" / "location_enricher.py"
    )
    if spec and spec.loader:
        location_enricher_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(location_enricher_module)
        get_location_enricher = location_enricher_module.get_location_enricher
        LOCATION_ENRICHER_AVAILABLE = True
        logger.info("Location enricher (Step 10) loaded successfully")
except Exception as e:
    logger.warning(f"Location enricher (Step 10) not available: {e}")
    get_location_enricher = None

# Import media enhancer (Step 12)
MEDIA_ENHANCER_AVAILABLE = False
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "media_enhancer",
        Path(__file__).parent.parent / "ingest" / "media_enhancer.py"
    )
    if spec and spec.loader:
        media_enhancer_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(media_enhancer_module)
        get_media_enhancer = media_enhancer_module.get_media_enhancer
        MEDIA_ENHANCER_AVAILABLE = True
        logger.info("Media enhancer (Step 12) loaded successfully")
except Exception as e:
    logger.warning(f"Media enhancer (Step 12) not available: {e}")
    get_media_enhancer = None


class CanonicalIngestionPipeline:
    """
    Processes parsed UFDR data and outputs canonical JSONL format
    Ensures forensic integrity and normalized data representation
    """
    
    def __init__(self, 
                 case_id: str,
                 examiner: str,
                 agency: str,
                 output_dir: str = "data/canonical"):
        """
        Initialize the canonical ingestion pipeline
        
        Args:
            case_id: Unique case identifier
            examiner: Name of forensic examiner
            agency: Law enforcement agency
            output_dir: Output directory for canonical JSONL files
        """
        self.case_id = case_id
        self.examiner = examiner
        self.agency = agency
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create case-specific subdirectory
        self.case_dir = self.output_dir / case_id
        self.case_dir.mkdir(exist_ok=True)
        
        # Statistics
        self.stats = {
            'messages': 0,
            'calls': 0,
            'contacts': 0,
            'media': 0,
            'locations': 0,
            'names_detected': 0,
            'names_high_confidence': 0,
            'names_needs_review': 0,
            'locations_enriched': 0,
            'locations_with_coordinates': 0,
            'locations_with_addresses': 0,
            'location_clusters': 0,
            'media_enhanced': 0,
            'ocr_processed': 0,
            'stt_processed': 0,
            'entities_extracted': 0,
        }
        
        # Initialize name detector (Step 7)
        self.name_detector = None
        if NAME_DETECTOR_AVAILABLE:
            try:
                self.name_detector = get_name_detector(use_llm_fallback=False)
                logger.info("Name detector (Step 7) initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize name detector: {e}")
        else:
            logger.info("Name detector not available - contacts will be processed without name validation")
        
        # Initialize location enricher (Step 10)
        self.location_enricher = None
        if LOCATION_ENRICHER_AVAILABLE:
            try:
                from ingest.location_enricher import LocationEnricher
                self.location_enricher = LocationEnricher()
                logger.info("Location enricher (Step 10) initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize location enricher: {e}")
        else:
            logger.info("Location enricher not available - locations will be processed without enrichment")
        
        # Initialize media enhancer (Step 12)
        self.media_enhancer = None
        if MEDIA_ENHANCER_AVAILABLE:
            try:
                from ingest.media_enhancer import MediaEnhancer
                db_path = Path("data/ufdr_analysis.db")
                self.media_enhancer = MediaEnhancer(
                    ocr_enabled=True,
                    stt_enabled=True,
                    db_path=str(db_path)
                )
                logger.info("Media enhancer (Step 12) initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize media enhancer: {e}")
        else:
            logger.info("Media enhancer not available - media will be processed without OCR/STT")
        
        # Open JSONL files for streaming writes
        self.file_handles = {}
        self._open_jsonl_files()
        
        logger.info(f"Initialized canonical ingestion for case: {case_id}")
    
    def _open_jsonl_files(self):
        """Open JSONL file handles for each data type"""
        data_types = ['messages', 'calls', 'contacts', 'media', 'locations', 'devices']
        
        for dtype in data_types:
            file_path = self.case_dir / f"{dtype}.jsonl"
            self.file_handles[dtype] = open(file_path, 'w', encoding='utf-8')
    
    def close(self):
        """Close all file handles"""
        for handle in self.file_handles.values():
            handle.close()
        logger.info("Closed all JSONL file handles")
    
    def _write_jsonl(self, data_type: str, record: Dict):
        """Write a record to JSONL file"""
        json_line = json.dumps(record, ensure_ascii=False)
        self.file_handles[data_type].write(json_line + '\n')
        self.file_handles[data_type].flush()
    
    def ingest_device(self, device_data: Dict) -> Device:
        """
        Ingest device information
        
        Args:
            device_data: Raw device data dictionary
            
        Returns:
            Canonical Device object
        """
        device = Device(
            device_id=device_data.get('device_id', str(uuid.uuid4())),
            case_id=self.case_id,
            imei=device_data.get('imei'),
            serial_number=device_data.get('serial_number'),
            manufacturer=device_data.get('manufacturer'),
            model=device_data.get('model'),
            os_type=device_data.get('os_type'),
            os_version=device_data.get('os_version'),
            phone_numbers=[
                normalize_phone_number(p) or p 
                for p in device_data.get('phone_numbers', [])
            ],
            mac_addresses=device_data.get('mac_addresses', []),
            acquisition_date=normalize_timestamp(device_data.get('acquisition_date')),
            metadata=device_data.get('metadata', {})
        )
        
        self._write_jsonl('devices', device.to_dict())
        return device
    
    def ingest_message(self, message_data: Dict, device_id: str) -> Message:
        """
        Ingest a message with normalization
        
        Args:
            message_data: Raw message data
            device_id: Associated device ID
            
        Returns:
            Canonical Message object
        """
        # Determine message type
        msg_type = MessageType.TEXT
        if message_data.get('has_attachment'):
            attachment_type = message_data.get('attachment_type', '').lower()
            if 'image' in attachment_type:
                msg_type = MessageType.IMAGE
            elif 'video' in attachment_type:
                msg_type = MessageType.VIDEO
            elif 'audio' in attachment_type or 'voice' in attachment_type:
                msg_type = MessageType.AUDIO
        
        # Normalize phone numbers
        from_person = message_data.get('from')
        to_person = message_data.get('to')
        
        if from_person and from_person.startswith('+'):
            from_person = normalize_phone_number(from_person) or from_person
        if to_person and to_person.startswith('+'):
            to_person = normalize_phone_number(to_person) or to_person
        
        message = Message(
            id=message_data.get('id', str(uuid.uuid4())),
            case_id=self.case_id,
            device_id=device_id,
            app=Message.standardize_app_name(message_data.get('app', 'unknown')),
            from_person=from_person,
            to_person=to_person,
            participants=message_data.get('participants', []),
            text=message_data.get('text'),
            message_type=msg_type,
            timestamp=normalize_timestamp(message_data.get('timestamp')),
            is_deleted=message_data.get('is_deleted', False),
            source_path=message_data.get('source_path'),
            media_references=message_data.get('media_references', []),
            metadata=message_data.get('metadata', {})
        )
        
        self._write_jsonl('messages', message.to_dict())
        self.stats['messages'] += 1
        return message
    
    def ingest_call(self, call_data: Dict, device_id: str) -> Call:
        """
        Ingest a call record with normalization
        
        Args:
            call_data: Raw call data
            device_id: Associated device ID
            
        Returns:
            Canonical Call object
        """
        # Determine call direction
        direction_map = {
            'incoming': CallDirection.INCOMING,
            'outgoing': CallDirection.OUTGOING,
            'missed': CallDirection.MISSED,
            'rejected': CallDirection.REJECTED,
            'blocked': CallDirection.BLOCKED,
        }
        direction = direction_map.get(
            call_data.get('direction', 'incoming').lower(),
            CallDirection.INCOMING
        )
        
        # Normalize phone number
        number = call_data.get('number', '')
        normalized_number = normalize_phone_number(number) or number
        
        call = Call(
            id=call_data.get('id', str(uuid.uuid4())),
            case_id=self.case_id,
            device_id=device_id,
            number=normalized_number,
            direction=direction,
            duration=int(call_data.get('duration', 0)),
            timestamp=normalize_timestamp(call_data.get('timestamp')) or datetime.now(),
            contact_name=call_data.get('contact_name'),
            is_deleted=call_data.get('is_deleted', False),
            source_path=call_data.get('source_path'),
            metadata=call_data.get('metadata', {})
        )
        
        self._write_jsonl('calls', call.to_dict())
        self.stats['calls'] += 1
        return call
    
    def ingest_media(self, media_data: Dict, device_id: str, 
                     media_storage_path: Optional[str] = None) -> Media:
        """
        Ingest media file with SHA256 and perceptual hashing
        
        Args:
            media_data: Raw media data
            device_id: Associated device ID
            media_storage_path: Path to media storage directory
            
        Returns:
            Canonical Media object
        """
        # Determine media type
        mime_type = media_data.get('mime_type', '').lower()
        if 'image' in mime_type:
            media_type = MediaType.IMAGE
        elif 'video' in mime_type:
            media_type = MediaType.VIDEO
        elif 'audio' in mime_type:
            media_type = MediaType.AUDIO
        elif 'document' in mime_type or 'pdf' in mime_type:
            media_type = MediaType.DOCUMENT
        else:
            media_type = MediaType.UNKNOWN
        
        original_path = media_data.get('original_path', '')
        stored_path = media_data.get('stored_path', original_path)
        
        # Compute SHA256 hash
        sha256_hash = media_data.get('sha256')
        if not sha256_hash and os.path.exists(stored_path):
            sha256_hash = Media.compute_sha256(stored_path)
        
        # Compute perceptual hash for images
        phash = None
        if media_type == MediaType.IMAGE and os.path.exists(stored_path):
            phash = Media.compute_perceptual_hash(stored_path)
        
        # Get file size
        file_size = media_data.get('file_size', 0)
        if not file_size and os.path.exists(stored_path):
            file_size = os.path.getsize(stored_path)
        
        media = Media(
            id=media_data.get('id', str(uuid.uuid4())),
            case_id=self.case_id,
            device_id=device_id,
            type=media_type,
            original_path=original_path,
            stored_path=stored_path,
            sha256=sha256_hash or '',
            phash=phash,
            file_size=file_size,
            mime_type=mime_type,
            width=media_data.get('width'),
            height=media_data.get('height'),
            duration=media_data.get('duration'),
            created_date=normalize_timestamp(media_data.get('created_date')),
            modified_date=normalize_timestamp(media_data.get('modified_date')),
            ocr_text=media_data.get('ocr_text'),
            caption=media_data.get('caption'),
            embeddings=media_data.get('embeddings', []),
            tags=media_data.get('tags', []),
            metadata=media_data.get('metadata', {})
        )
        
        self._write_jsonl('media', media.to_dict())
        self.stats['media'] += 1
        return media
    
    def ingest_location(self, location_data: Dict, device_id: str) -> Location:
        """
        Ingest location data with enrichment (Step 10)
        
        Args:
            location_data: Raw location data
            device_id: Associated device ID
            
        Returns:
            Canonical Location object with enrichment metadata
        """
        # Initialize enrichment metadata
        enrichment_metadata = {
            'enriched': False,
            'coordinate_normalized': False,
            'address_parsed': False,
            'location_type': None,
            'enricher_version': '1.0'
        }
        
        # Try to enrich location if enricher is available
        if self.location_enricher and (location_data.get('latitude') or location_data.get('address')):
            try:
                # Prepare coordinate input
                coordinate_input = None
                if location_data.get('latitude') and location_data.get('longitude'):
                    coordinate_input = {
                        'latitude': location_data['latitude'],
                        'longitude': location_data['longitude'],
                        'accuracy': location_data.get('accuracy'),
                        'altitude': location_data.get('altitude')
                    }
                
                # Prepare address input
                address_input = location_data.get('address')
                
                # Create enriched location
                enriched_loc = self.location_enricher.create_location(
                    coordinate=coordinate_input,
                    address=address_input,
                    name=location_data.get('name'),
                    timestamp=normalize_timestamp(location_data.get('timestamp')),
                    metadata=location_data.get('metadata', {})
                )
                
                if enriched_loc:
                    # Update enrichment metadata
                    enrichment_metadata['enriched'] = True
                    enrichment_metadata['coordinate_normalized'] = enriched_loc.coordinate is not None
                    enrichment_metadata['address_parsed'] = enriched_loc.address is not None
                    enrichment_metadata['location_type'] = enriched_loc.location_type
                    
                    if enriched_loc.coordinate:
                        enrichment_metadata['coordinate_format'] = enriched_loc.coordinate.source_format
                        enrichment_metadata['coordinate_dms'] = enriched_loc.coordinate.to_dms()
                    
                    if enriched_loc.address:
                        enrichment_metadata['address_confidence'] = enriched_loc.address.confidence
                        enrichment_metadata['standardized_address'] = enriched_loc.address.standardized
                        enrichment_metadata['country'] = enriched_loc.address.country
                    
                    # Update statistics
                    self.stats['locations_enriched'] += 1
                    if enriched_loc.coordinate:
                        self.stats['locations_with_coordinates'] += 1
                    if enriched_loc.address:
                        self.stats['locations_with_addresses'] += 1
                    
                    logger.debug(f"Location enriched: type={enriched_loc.location_type}, "
                               f"visits={enriched_loc.visit_count}")
                    
            except Exception as e:
                logger.warning(f"Location enrichment failed: {e}")
                enrichment_metadata['error'] = str(e)
        
        # Merge enrichment metadata with existing metadata
        metadata = location_data.get('metadata', {})
        metadata['enrichment'] = enrichment_metadata
        
        location = Location(
            id=location_data.get('id', str(uuid.uuid4())),
            case_id=self.case_id,
            device_id=device_id,
            latitude=float(location_data.get('latitude', 0)),
            longitude=float(location_data.get('longitude', 0)),
            altitude=location_data.get('altitude'),
            accuracy=location_data.get('accuracy'),
            timestamp=normalize_timestamp(location_data.get('timestamp')) or datetime.now(),
            source_app=location_data.get('source_app'),
            source_path=location_data.get('source_path'),
            address=location_data.get('address'),
            metadata=metadata
        )
        
        self._write_jsonl('locations', location.to_dict())
        self.stats['locations'] += 1
        return location
    
    def ingest_contact(self, contact_data: Dict) -> Person:
        """
        Ingest contact as a Person entity with name detection (Step 7)
        
        Args:
            contact_data: Raw contact data
            
        Returns:
            Canonical Person object with name validation metadata
        """
        # Normalize all phone numbers
        phone_numbers = []
        for phone in contact_data.get('phone_numbers', []):
            normalized = normalize_phone_number(phone)
            if normalized:
                phone_numbers.append(normalized)
        
        # Get contact name
        contact_name = contact_data.get('name', '')
        
        # Initialize name detection metadata
        name_detection_metadata = {
            'is_person_name': None,
            'name_confidence': None,
            'name_confidence_level': None,
            'detected_script': None,
            'honorific': None,
            'detection_reasons': [],
            'needs_review': False,
            'detector_version': '1.0'
        }
        
        # Run name detection if available
        if self.name_detector and contact_name:
            try:
                # Get phone context (prefer first phone number)
                phone_context = phone_numbers[0] if phone_numbers else None
                
                # Run name detection
                detection = self.name_detector.detect_name(
                    text=contact_name,
                    phone_context=phone_context,
                    context_signals={
                        'from_contacts_app': True,
                        'has_phone_number': bool(phone_numbers)
                    }
                )
                
                # Update metadata
                name_detection_metadata.update({
                    'is_person_name': detection.is_person_name,
                    'name_confidence': detection.confidence,
                    'name_confidence_level': self._get_confidence_level_name(detection.confidence),
                    'detected_script': detection.detected_script.value if detection.detected_script else None,
                    'honorific': detection.honorific,
                    'detection_reasons': detection.reasons[:5],  # Limit to top 5 reasons
                    'needs_review': detection.confidence < 0.7,
                })
                
                # Update statistics
                self.stats['names_detected'] += 1
                if detection.confidence >= 0.9:
                    self.stats['names_high_confidence'] += 1
                if detection.confidence < 0.7:
                    self.stats['names_needs_review'] += 1
                    
                logger.debug(f"Name detection: '{contact_name}' -> {detection.is_person_name} "
                           f"(confidence: {detection.confidence:.2f})")
                
            except Exception as e:
                logger.warning(f"Name detection failed for '{contact_name}': {e}")
                name_detection_metadata['error'] = str(e)
        
        # Merge name detection metadata with existing metadata
        metadata = contact_data.get('metadata', {})
        metadata['name_detection'] = name_detection_metadata
        
        person = Person(
            person_id=contact_data.get('id', str(uuid.uuid4())),
            case_id=self.case_id,
            name=contact_name,
            phone_numbers=phone_numbers,
            emails=contact_data.get('emails', []),
            usernames=contact_data.get('usernames', []),
            addresses=contact_data.get('addresses', []),
            metadata=metadata
        )
        
        self._write_jsonl('contacts', person.to_dict())
        self.stats['contacts'] += 1
        return person
    
    def _get_confidence_level_name(self, confidence: float) -> str:
        """Convert confidence score to human-readable level"""
        if confidence >= 0.9:
            return "high"
        elif confidence >= 0.7:
            return "medium"
        elif confidence >= 0.5:
            return "low"
        else:
            return "none"
    
    def resolve_entities(self, min_merge_confidence: float = 0.8) -> Dict:
        """
        Resolve duplicate entities using Step 8 (Semantic Deduplication)
        
        Args:
            min_merge_confidence: Minimum confidence to auto-merge entities (default: 0.8)
            
        Returns:
            Dictionary with resolution results including:
            - entities: List of resolved entities
            - matches: List of entity matches found
            - merge_count: Number of duplicates merged
            - statistics: Detailed statistics
        """
        logger.info(f"Starting entity resolution for case: {self.case_id}")
        
        try:
            # Import entity resolver (Step 8)
            from ingest.entity_resolver import get_entity_resolver
            resolver = get_entity_resolver()
            
            # Load contacts from JSONL
            contacts_file = self.case_dir / "contacts.jsonl"
            if not contacts_file.exists():
                logger.warning(f"No contacts file found: {contacts_file}")
                return {
                    'success': False,
                    'error': 'No contacts file found',
                    'entities': [],
                    'matches': [],
                    'merge_count': 0
                }
            
            # Read contacts
            contacts = []
            with open(contacts_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        contact = json.loads(line)
                        # Convert to format expected by entity resolver
                        contacts.append({
                            'id': contact.get('person_id', str(uuid.uuid4())),
                            'name': contact.get('name', ''),
                            'phone': contact.get('phone_numbers', [None])[0] if contact.get('phone_numbers') else None,
                            'email': contact.get('emails', [None])[0] if contact.get('emails') else None,
                            'case_id': contact.get('case_id', self.case_id),
                            'metadata': contact.get('metadata', {})
                        })
            
            if not contacts:
                logger.info("No contacts to resolve")
                return {
                    'success': True,
                    'entities': [],
                    'matches': [],
                    'merge_count': 0
                }
            
            logger.info(f"Resolving {len(contacts)} contacts...")
            
            # Resolve entities
            results = resolver.resolve_entities(
                contacts,
                min_merge_confidence=min_merge_confidence
            )
            
            # Write resolved entities to new file
            entities_file = self.case_dir / "resolved_entities.jsonl"
            with open(entities_file, 'w', encoding='utf-8') as f:
                for entity in results['entities']:
                    json_line = json.dumps(entity, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            # Write entity matches (duplicates)
            if results['matches']:
                matches_file = self.case_dir / "entity_matches.jsonl"
                with open(matches_file, 'w', encoding='utf-8') as f:
                    for match in results['matches']:
                        match_dict = {
                            'entity_1_id': match.entity_1_id,
                            'entity_2_id': match.entity_2_id,
                            'confidence': match.confidence,
                            'confidence_level': match.get_confidence_level().value,
                            'phone_match': match.phone_match,
                            'name_similarity': match.name_similarity,
                            'email_match': match.email_match,
                            'reasons': match.match_reasons,
                            'case_id': self.case_id
                        }
                        json_line = json.dumps(match_dict, ensure_ascii=False)
                        f.write(json_line + '\n')
            
            logger.info(f"Entity resolution complete: {results['merge_count']} duplicates merged")
            logger.info(f"  Total entities: {len(contacts)} → {results['total_entities']}")
            logger.info(f"  Reduction: {len(contacts) - results['total_entities']} duplicates removed")
            logger.info(f"  High confidence matches: {results['high_confidence_matches']}")
            logger.info(f"  Medium confidence matches: {results['medium_confidence_matches']}")
            logger.info(f"  Low confidence matches: {results['low_confidence_matches']}")
            
            return {
                'success': True,
                'entities': results['entities'],
                'matches': results['matches'],
                'merge_count': results['merge_count'],
                'total_entities': results['total_entities'],
                'total_matches': results['total_matches'],
                'high_confidence_matches': results['high_confidence_matches'],
                'medium_confidence_matches': results['medium_confidence_matches'],
                'low_confidence_matches': results['low_confidence_matches'],
                'original_count': len(contacts),
                'reduction_percentage': ((len(contacts) - results['total_entities']) / len(contacts) * 100) if contacts else 0
            }
            
        except Exception as e:
            logger.error(f"Entity resolution failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'entities': [],
                'matches': [],
                'merge_count': 0
            }
    
    def cluster_locations(self, radius_meters: float = 100) -> Dict:
        """
        Cluster locations using Step 10 (Location Enrichment)
        
        Args:
            radius_meters: Maximum distance for locations to be in same cluster (default: 100m)
            
        Returns:
            Dictionary with clustering results including:
            - clusters: List of location clusters
            - statistics: Detailed statistics
        """
        logger.info(f"Starting location clustering for case: {self.case_id}")
        
        if not self.location_enricher:
            logger.warning("Location enricher not available")
            return {
                'success': False,
                'error': 'Location enricher not available',
                'clusters': [],
                'total_clusters': 0
            }
        
        try:
            # Perform clustering
            clusters = self.location_enricher.cluster_locations(radius_meters=radius_meters)
            
            if not clusters:
                logger.info("No location clusters found")
                return {
                    'success': True,
                    'clusters': [],
                    'total_clusters': 0
                }
            
            # Write clusters to file
            clusters_file = self.case_dir / "location_clusters.jsonl"
            with open(clusters_file, 'w', encoding='utf-8') as f:
                for cluster in clusters:
                    json_line = json.dumps(cluster.to_dict(), ensure_ascii=False)
                    f.write(json_line + '\n')
            
            # Update statistics
            self.stats['location_clusters'] = len(clusters)
            
            # Get enricher statistics
            enricher_stats = self.location_enricher.get_statistics()
            
            logger.info(f"Location clustering complete: {len(clusters)} clusters created")
            logger.info(f"  Total locations: {enricher_stats['total_locations']}")
            logger.info(f"  With coordinates: {enricher_stats['with_coordinates']}")
            logger.info(f"  With addresses: {enricher_stats['with_addresses']}")
            logger.info(f"  Total visits: {enricher_stats['total_visits']}")
            
            return {
                'success': True,
                'clusters': [c.to_dict() for c in clusters],
                'total_clusters': len(clusters),
                'total_locations': enricher_stats['total_locations'],
                'with_coordinates': enricher_stats['with_coordinates'],
                'with_addresses': enricher_stats['with_addresses'],
                'total_visits': enricher_stats['total_visits']
            }
            
        except Exception as e:
            logger.error(f"Location clustering failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'clusters': [],
                'total_clusters': 0
            }
    
    def enhance_media(self, media_dir: Optional[Path] = None) -> Dict:
        """
        Enhance media files using Step 12 (OCR for images, STT for audio)
        
        Args:
            media_dir: Optional directory containing media files.
                      If None, searches for media in extracted UFDR directory
            
        Returns:
            Dictionary with enhancement results including:
            - success: Boolean indicating success
            - results: List of MediaEnhancementResult objects
            - statistics: Enhancement statistics
        """
        logger.info(f"Starting media enhancement for case: {self.case_id}")
        
        if not self.media_enhancer:
            logger.warning("Media enhancer not available")
            return {
                'success': False,
                'error': 'Media enhancer not available',
                'results': [],
                'statistics': {}
            }
        
        try:
            # If no media directory specified, try to find one
            if media_dir is None:
                # Look for common media directories
                possible_dirs = [
                    Path(f"data/parsed/{self.case_id}/media"),
                    Path(f"data/extracted/{self.case_id}/media"),
                    Path(f"uploads/ufdr/{self.case_id}/media"),
                ]
                
                for dir_path in possible_dirs:
                    if dir_path.exists():
                        media_dir = dir_path
                        logger.info(f"Found media directory: {media_dir}")
                        break
                
                if media_dir is None:
                    logger.info("No media directory found to enhance")
                    return {
                        'success': True,
                        'results': [],
                        'statistics': {'message': 'No media files found'}
                    }
            
            media_dir = Path(media_dir)
            
            if not media_dir.exists():
                logger.warning(f"Media directory does not exist: {media_dir}")
                return {
                    'success': False,
                    'error': f'Media directory not found: {media_dir}',
                    'results': [],
                    'statistics': {}
                }
            
            # Process media directory
            logger.info(f"Processing media files in: {media_dir}")
            results = self.media_enhancer.process_media_directory(
                media_dir=media_dir,
                case_id=self.case_id,
                progress_callback=None  # Can add progress callback for UI
            )
            
            # Update statistics
            for result in results:
                if result.success:
                    self.stats['media_enhanced'] += 1
                    if result.media_type == 'image':
                        self.stats['ocr_processed'] += 1
                    elif result.media_type == 'audio':
                        self.stats['stt_processed'] += 1
                    self.stats['entities_extracted'] += sum(len(v) for v in result.entities.values())
            
            # Get enhancement statistics
            enhancement_stats = self.media_enhancer.get_enhancement_stats(self.case_id)
            
            logger.info(f"Media enhancement complete: {len(results)} files processed")
            logger.info(f"  OCR processed: {self.stats['ocr_processed']}")
            logger.info(f"  STT processed: {self.stats['stt_processed']}")
            logger.info(f"  Entities extracted: {self.stats['entities_extracted']}")
            
            return {
                'success': True,
                'results': [r.to_dict() for r in results],
                'total_processed': len(results),
                'ocr_processed': self.stats['ocr_processed'],
                'stt_processed': self.stats['stt_processed'],
                'entities_extracted': self.stats['entities_extracted'],
                'statistics': enhancement_stats
            }
            
        except Exception as e:
            logger.error(f"Media enhancement failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'results': [],
                'statistics': {}
            }
    
    def generate_manifest(self, 
                         original_file_path: str,
                         extraction_tool: str = "UFDR Parser",
                         extraction_version: str = "1.0") -> IngestManifest:
        """
        Generate ingestion manifest for chain of custody
        
        Args:
            original_file_path: Path to original UFDR file
            extraction_tool: Name of extraction tool
            extraction_version: Version of extraction tool
            
        Returns:
            IngestManifest object
        """
        # Compute file hash
        file_hash = Case.generate_manifest_hash(original_file_path)
        file_size = os.path.getsize(original_file_path)
        
        manifest = IngestManifest(
            manifest_id=str(uuid.uuid4()),
            case_id=self.case_id,
            file_name=os.path.basename(original_file_path),
            file_size=file_size,
            file_hash_sha256=file_hash,
            ingest_timestamp=datetime.now(),
            examiner=self.examiner,
            agency=self.agency,
            extraction_tool=extraction_tool,
            extraction_version=extraction_version,
            total_messages=self.stats['messages'],
            total_calls=self.stats['calls'],
            total_contacts=self.stats['contacts'],
            total_media=self.stats['media'],
            total_locations=self.stats['locations']
        )
        
        # Save manifest
        manifest_path = self.case_dir / "ingest_manifest.json"
        manifest.to_json(str(manifest_path))
        
        logger.info(f"Generated manifest: {manifest_path}")
        return manifest
    
    def get_stats(self) -> Dict:
        """Get ingestion statistics"""
        return self.stats.copy()
    
    def ingest_ufdr(self, ufdr_path: str) -> Dict:
        """
        Ingest a complete UFDR file (ZIP archive containing SQLite database)
        
        Args:
            ufdr_path: Path to UFDR file (.ufdr or .zip)
            
        Returns:
            Dictionary with 'statistics', 'manifest', and 'case_dir' keys
        """
        import zipfile
        import sqlite3
        import shutil
        import tempfile
        
        logger.info(f"Starting UFDR ingestion: {ufdr_path}")
        
        # Create temporary extraction directory
        extract_dir = Path(tempfile.mkdtemp(prefix=f"ufdr_extract_{self.case_id}_"))
        
        try:
            # Step 1: Extract ZIP archive
            logger.info("Extracting UFDR archive...")
            with zipfile.ZipFile(ufdr_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Step 2: Find SQLite database
            db_path = None
            for file in extract_dir.rglob("*.db"):
                db_path = file
                break
            
            if not db_path:
                # Try to find any database file
                for file in extract_dir.rglob("*"):
                    if file.is_file() and file.suffix.lower() in ['.db', '.sqlite', '.sqlite3']:
                        db_path = file
                        break
            
            if not db_path:
                logger.error("No SQLite database found in UFDR archive")
                raise ValueError("No SQLite database found in UFDR file")
            
            logger.info(f"Found database: {db_path}")
            
            # Step 3: Connect to database and extract data
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # Access columns by name
            cursor = conn.cursor()
            
            # Get available tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Found tables: {tables}")
            
            # Step 4: Ingest devices
            if 'devices' in tables:
                cursor.execute("SELECT * FROM devices")
                for row in cursor.fetchall():
                    device_data = dict(row)
                    self.ingest_device(device_data)
            
            # Get a default device_id for records without device association
            device_id = 'default_device'
            
            # Step 5: Ingest contacts
            if 'contacts' in tables:
                cursor.execute("SELECT * FROM contacts")
                for row in cursor.fetchall():
                    contact_data = dict(row)
                    self.ingest_contact(contact_data)
            
            # Step 6: Ingest messages
            if 'messages' in tables:
                cursor.execute("SELECT * FROM messages")
                for row in cursor.fetchall():
                    message_data = dict(row)
                    # Map column names if needed
                    if 'sender' in message_data:
                        message_data['from'] = message_data.get('sender')
                    if 'receiver' in message_data:
                        message_data['to'] = message_data.get('receiver')
                    self.ingest_message(message_data, device_id)
            
            # Step 7: Ingest calls
            if 'calls' in tables:
                cursor.execute("SELECT * FROM calls")
                for row in cursor.fetchall():
                    call_data = dict(row)
                    self.ingest_call(call_data, device_id)
            
            # Step 8: Ingest media
            if 'media' in tables:
                cursor.execute("SELECT * FROM media")
                for row in cursor.fetchall():
                    media_data = dict(row)
                    self.ingest_media(media_data, device_id)
            
            # Step 9: Ingest locations
            if 'locations' in tables:
                cursor.execute("SELECT * FROM locations")
                for row in cursor.fetchall():
                    location_data = dict(row)
                    self.ingest_location(location_data, device_id)
            
            conn.close()
            
            # Step 10: Generate manifest
            logger.info("Generating manifest...")
            manifest = self.generate_manifest(ufdr_path)
            
            # Step 11: Export to parsed JSON format for compatibility
            logger.info("Exporting to legacy JSON format...")
            self._export_to_json()
            
            logger.info(f"UFDR ingestion complete. Stats: {self.stats}")
            
            return {
                'statistics': self.get_stats(),
                'manifest': manifest.to_dict(),
                'case_dir': str(self.case_dir),
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error during UFDR ingestion: {e}", exc_info=True)
            raise
        finally:
            # Cleanup temporary extraction directory
            if extract_dir.exists():
                shutil.rmtree(extract_dir, ignore_errors=True)
                logger.info(f"Cleaned up extraction directory: {extract_dir}")
    
    def _export_to_json(self):
        """
        Export JSONL files to legacy JSON format for compatibility
        Creates files in data/parsed/<case_id>/ directory
        """
        parsed_dir = Path("data/parsed") / self.case_id
        parsed_dir.mkdir(parents=True, exist_ok=True)
        
        # Close file handles to ensure all data is written
        for handle in self.file_handles.values():
            handle.flush()
        
        # Convert each JSONL file to JSON array
        for data_type in ['messages', 'calls', 'contacts', 'media', 'locations', 'devices']:
            jsonl_path = self.case_dir / f"{data_type}.jsonl"
            json_path = parsed_dir / f"{data_type}.json"
            
            if jsonl_path.exists():
                records = []
                for record in read_jsonl(str(jsonl_path)):
                    records.append(record)
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(records, f, indent=2, ensure_ascii=False, default=str)
                
                logger.info(f"Exported {len(records)} records to {json_path}")


def read_jsonl(file_path: str) -> Generator[Dict, None, None]:
    """
    Read JSONL file line by line
    
    Args:
        file_path: Path to JSONL file
        
    Yields:
        Dictionary for each line
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = CanonicalIngestionPipeline(
        case_id="CASE_2024_001",
        examiner="Inspector Kumar",
        agency="NSG Cyber Division"
    )
    
    # Example device ingestion
    device_data = {
        'device_id': 'DEV001',
        'manufacturer': 'Samsung',
        'model': 'Galaxy S21',
        'os_type': 'Android',
        'os_version': '12',
        'imei': '123456789012345',
        'phone_numbers': ['+919876543210']
    }
    device = pipeline.ingest_device(device_data)
    
    # Example message ingestion
    message_data = {
        'id': 'MSG001',
        'app': 'WhatsApp',
        'from': '+919876543210',
        'to': '+911234567890',
        'text': 'Test message with crypto address: 1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa',
        'timestamp': '2024-01-15 10:30:00'
    }
    message = pipeline.ingest_message(message_data, device.device_id)
    
    # Generate manifest
    # manifest = pipeline.generate_manifest('/path/to/original.ufdr')
    
    # Close pipeline
    pipeline.close()
    
    print(f"Ingestion complete: {pipeline.get_stats()}")