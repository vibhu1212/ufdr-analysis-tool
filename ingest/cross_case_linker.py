"""
Step 11: Cross-Case Linking Module

This module provides cross-case entity matching, timeline correlation, and relationship
mapping capabilities for the UFDR ingestion pipeline. It handles:

- Entity matching across multiple cases
- Timeline correlation and event sequencing
- Relationship network analysis
- Cross-case contact linkage
- Communication pattern detection
- Location overlap analysis
- Shared media detection

All operations are designed for forensic use with full provenance tracking.

Author: UFDR Analysis Tool
Date: October 2025
"""

import hashlib
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass, asdict
from datetime import datetime
from collections import defaultdict


@dataclass
class EntityMatch:
    """Represents a matched entity across cases"""
    entity_id: str
    case_ids: List[str]
    match_type: str  # person, location, phone, email, device
    confidence: float
    match_attributes: Dict[str, Any]
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class CaseLink:
    """Represents a link between two cases"""
    link_id: str
    case_1_id: str
    case_2_id: str
    link_type: str  # shared_contact, shared_location, shared_media, communication
    strength: float  # 0.0 to 1.0
    evidence: List[Dict[str, Any]]
    detected_at: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class Timeline:
    """Represents a cross-case timeline"""
    timeline_id: str
    case_ids: List[str]
    events: List[Dict[str, Any]]
    start_time: str
    end_time: str
    duration_seconds: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class RelationshipNetwork:
    """Represents relationships across cases"""
    network_id: str
    entities: List[str]  # entity IDs
    relationships: List[Dict[str, Any]]  # edges between entities
    case_ids: List[str]
    network_type: str  # communication, location, social
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


class CrossCaseLinker:
    """
    Links entities, events, and relationships across multiple UFDR cases.
    
    This class handles all cross-case analysis operations:
    - Entity matching across cases
    - Timeline correlation
    - Relationship network construction
    - Communication pattern analysis
    - Location overlap detection
    """
    
    def __init__(self):
        """Initialize the cross-case linker"""
        self.cases: Dict[str, Dict[str, Any]] = {}
        self.entity_matches: List[EntityMatch] = []
        self.case_links: List[CaseLink] = []
        self.timelines: List[Timeline] = []
        self.networks: List[RelationshipNetwork] = []
        
        # Index structures for fast lookup
        self.phone_index: Dict[str, Set[str]] = defaultdict(set)  # phone -> case_ids
        self.email_index: Dict[str, Set[str]] = defaultdict(set)  # email -> case_ids
        self.name_index: Dict[str, Set[str]] = defaultdict(set)   # name -> case_ids
        self.location_index: Dict[Tuple[float, float], Set[str]] = defaultdict(set)  # (lat,lon) -> case_ids
        self.device_index: Dict[str, Set[str]] = defaultdict(set)  # device_id -> case_ids
    
    def load_case(self, case_id: str, case_data: Dict[str, Any]):
        """
        Load a case into the cross-case linker.
        
        Args:
            case_id: Unique case identifier
            case_data: Dictionary containing case data with keys:
                - contacts: List of contact records
                - messages: List of message records
                - calls: List of call records
                - locations: List of location records
                - devices: List of device records
                - media: List of media records
        """
        self.cases[case_id] = case_data
        
        # Index contacts
        for contact in case_data.get('contacts', []):
            # Index phones
            for phone in contact.get('phone_numbers', []):
                if phone:
                    self.phone_index[phone].add(case_id)
            
            # Index emails
            for email in contact.get('emails', []):
                if email:
                    self.email_index[email.lower()].add(case_id)
            
            # Index names
            name = contact.get('name', '').strip()
            if name:
                self.name_index[name.lower()].add(case_id)
        
        # Index locations (rounded to ~11m precision)
        for location in case_data.get('locations', []):
            lat = location.get('latitude')
            lon = location.get('longitude')
            if lat is not None and lon is not None:
                lat_rounded = round(lat, 5)
                lon_rounded = round(lon, 5)
                self.location_index[(lat_rounded, lon_rounded)].add(case_id)
        
        # Index devices
        for device in case_data.get('devices', []):
            device_id = device.get('device_id')
            if device_id:
                self.device_index[device_id].add(case_id)
            
            # Index device phone numbers
            for phone in device.get('phone_numbers', []):
                if phone:
                    self.phone_index[phone].add(case_id)
    
    def find_shared_contacts(self, min_confidence: float = 0.7) -> List[EntityMatch]:
        """
        Find contacts that appear in multiple cases.
        
        Args:
            min_confidence: Minimum confidence threshold for matches
            
        Returns:
            List of EntityMatch objects for shared contacts
        """
        matches = []
        
        # Find shared phones
        for phone, case_ids in self.phone_index.items():
            if len(case_ids) > 1:
                match = EntityMatch(
                    entity_id=self._generate_entity_id('phone', phone),
                    case_ids=list(case_ids),
                    match_type='phone',
                    confidence=1.0,  # Exact phone match
                    match_attributes={'phone': phone}
                )
                matches.append(match)
        
        # Find shared emails
        for email, case_ids in self.email_index.items():
            if len(case_ids) > 1:
                match = EntityMatch(
                    entity_id=self._generate_entity_id('email', email),
                    case_ids=list(case_ids),
                    match_type='email',
                    confidence=1.0,  # Exact email match
                    match_attributes={'email': email}
                )
                matches.append(match)
        
        # Find shared names (with lower confidence)
        for name, case_ids in self.name_index.items():
            if len(case_ids) > 1:
                # Only include if name is reasonably specific (>5 chars)
                if len(name) > 5:
                    match = EntityMatch(
                        entity_id=self._generate_entity_id('name', name),
                        case_ids=list(case_ids),
                        match_type='name',
                        confidence=0.75,  # Lower confidence for name-only match
                        match_attributes={'name': name}
                    )
                    if match.confidence >= min_confidence:
                        matches.append(match)
        
        self.entity_matches.extend(matches)
        return matches
    
    def find_shared_locations(self, radius_meters: float = 100) -> List[EntityMatch]:
        """
        Find locations that appear in multiple cases.
        
        Args:
            radius_meters: Radius for location matching (not currently used)
            
        Returns:
            List of EntityMatch objects for shared locations
        """
        matches = []
        
        # Find exact location matches (within ~11m precision)
        for location, case_ids in self.location_index.items():
            if len(case_ids) > 1:
                lat, lon = location
                match = EntityMatch(
                    entity_id=self._generate_entity_id('location', f"{lat},{lon}"),
                    case_ids=list(case_ids),
                    match_type='location',
                    confidence=0.95,  # High confidence for location match
                    match_attributes={'latitude': lat, 'longitude': lon}
                )
                matches.append(match)
        
        self.entity_matches.extend(matches)
        return matches
    
    def find_shared_devices(self) -> List[EntityMatch]:
        """
        Find devices that appear in multiple cases.
        
        Returns:
            List of EntityMatch objects for shared devices
        """
        matches = []
        
        for device_id, case_ids in self.device_index.items():
            if len(case_ids) > 1:
                match = EntityMatch(
                    entity_id=self._generate_entity_id('device', device_id),
                    case_ids=list(case_ids),
                    match_type='device',
                    confidence=1.0,  # Exact device match
                    match_attributes={'device_id': device_id}
                )
                matches.append(match)
        
        self.entity_matches.extend(matches)
        return matches
    
    def create_case_links(self, min_strength: float = 0.5) -> List[CaseLink]:
        """
        Create links between cases based on shared entities.
        
        Args:
            min_strength: Minimum link strength threshold
            
        Returns:
            List of CaseLink objects
        """
        # Group matches by case pairs
        case_pair_evidence: Dict[Tuple[str, str], List[EntityMatch]] = defaultdict(list)
        
        for match in self.entity_matches:
            # Create all case pairs for this match
            cases = sorted(match.case_ids)
            for i in range(len(cases)):
                for j in range(i + 1, len(cases)):
                    case_pair = (cases[i], cases[j])
                    case_pair_evidence[case_pair].append(match)
        
        # Create links
        links = []
        for (case_1_id, case_2_id), evidence in case_pair_evidence.items():
            # Calculate link strength based on evidence
            strength = self._calculate_link_strength(evidence)
            
            if strength >= min_strength:
                # Determine link type
                link_types = set(match.match_type for match in evidence)
                if len(link_types) == 1:
                    link_type = f"shared_{list(link_types)[0]}"
                else:
                    link_type = "multiple_shared_entities"
                
                link = CaseLink(
                    link_id=self._generate_link_id(case_1_id, case_2_id),
                    case_1_id=case_1_id,
                    case_2_id=case_2_id,
                    link_type=link_type,
                    strength=strength,
                    evidence=[match.to_dict() for match in evidence],
                    detected_at=datetime.now().isoformat()
                )
                links.append(link)
        
        self.case_links = links
        return links
    
    def create_cross_case_timeline(self, case_ids: Optional[List[str]] = None) -> Timeline:
        """
        Create a unified timeline across multiple cases.
        
        Args:
            case_ids: List of case IDs to include (None = all cases)
            
        Returns:
            Timeline object
        """
        if case_ids is None:
            case_ids = list(self.cases.keys())
        
        # Collect all timestamped events
        events = []
        
        for case_id in case_ids:
            case_data = self.cases.get(case_id, {})
            
            # Add messages
            for msg in case_data.get('messages', []):
                if msg.get('timestamp'):
                    events.append({
                        'type': 'message',
                        'case_id': case_id,
                        'timestamp': msg.get('timestamp'),
                        'from': msg.get('from_person'),
                        'to': msg.get('to_person'),
                        'text': msg.get('text', '')[:100]  # Truncate for summary
                    })
            
            # Add calls
            for call in case_data.get('calls', []):
                if call.get('timestamp'):
                    events.append({
                        'type': 'call',
                        'case_id': case_id,
                        'timestamp': call.get('timestamp'),
                        'from': call.get('from_number'),
                        'to': call.get('to_number'),
                        'duration': call.get('duration')
                    })
            
            # Add locations
            for loc in case_data.get('locations', []):
                if loc.get('timestamp'):
                    events.append({
                        'type': 'location',
                        'case_id': case_id,
                        'timestamp': loc.get('timestamp'),
                        'latitude': loc.get('latitude'),
                        'longitude': loc.get('longitude')
                    })
        
        # Sort events by timestamp (handle both datetime and string)
        def get_sortable_timestamp(event):
            ts = event.get('timestamp', '')
            if isinstance(ts, datetime):
                return ts.isoformat()
            return ts if isinstance(ts, str) else ''
        
        events.sort(key=get_sortable_timestamp)
        
        # Calculate timeline bounds
        if events:
            start_time = events[0].get('timestamp', '')
            end_time = events[-1].get('timestamp', '')
        else:
            start_time = datetime.now().isoformat()
            end_time = start_time
        
        timeline = Timeline(
            timeline_id=self._generate_timeline_id(case_ids),
            case_ids=case_ids,
            events=events,
            start_time=start_time,
            end_time=end_time
        )
        
        self.timelines.append(timeline)
        return timeline
    
    def create_communication_network(self, case_ids: Optional[List[str]] = None) -> RelationshipNetwork:
        """
        Create a communication network graph across cases.
        
        Args:
            case_ids: List of case IDs to include (None = all cases)
            
        Returns:
            RelationshipNetwork object
        """
        if case_ids is None:
            case_ids = list(self.cases.keys())
        
        # Build network of communications
        entities = set()
        relationships = []
        
        for case_id in case_ids:
            case_data = self.cases.get(case_id, {})
            
            # Add message relationships
            for msg in case_data.get('messages', []):
                from_person = msg.get('from_person')
                to_person = msg.get('to_person')
                if from_person and to_person:
                    entities.add(from_person)
                    entities.add(to_person)
                    relationships.append({
                        'from': from_person,
                        'to': to_person,
                        'type': 'message',
                        'case_id': case_id,
                        'timestamp': msg.get('timestamp')
                    })
            
            # Add call relationships
            for call in case_data.get('calls', []):
                from_number = call.get('from_number')
                to_number = call.get('to_number')
                if from_number and to_number:
                    entities.add(from_number)
                    entities.add(to_number)
                    relationships.append({
                        'from': from_number,
                        'to': to_number,
                        'type': 'call',
                        'case_id': case_id,
                        'timestamp': call.get('timestamp'),
                        'duration': call.get('duration')
                    })
        
        network = RelationshipNetwork(
            network_id=self._generate_network_id(case_ids),
            entities=list(entities),
            relationships=relationships,
            case_ids=case_ids,
            network_type='communication'
        )
        
        self.networks.append(network)
        return network
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about cross-case analysis"""
        return {
            'total_cases': len(self.cases),
            'total_entity_matches': len(self.entity_matches),
            'entity_matches_by_type': self._count_by_type(self.entity_matches, 'match_type'),
            'total_case_links': len(self.case_links),
            'case_links_by_type': self._count_by_type(self.case_links, 'link_type'),
            'total_timelines': len(self.timelines),
            'total_networks': len(self.networks),
            'indexed_phones': len(self.phone_index),
            'indexed_emails': len(self.email_index),
            'indexed_names': len(self.name_index),
            'indexed_locations': len(self.location_index),
            'indexed_devices': len(self.device_index)
        }
    
    def _generate_entity_id(self, entity_type: str, key: str) -> str:
        """Generate unique entity ID"""
        id_str = f"{entity_type}:{key}"
        return hashlib.md5(id_str.encode()).hexdigest()[:16]
    
    def _generate_link_id(self, case_1_id: str, case_2_id: str) -> str:
        """Generate unique link ID"""
        cases = sorted([case_1_id, case_2_id])
        id_str = f"link:{cases[0]}:{cases[1]}"
        return hashlib.md5(id_str.encode()).hexdigest()[:16]
    
    def _generate_timeline_id(self, case_ids: List[str]) -> str:
        """Generate unique timeline ID"""
        cases = sorted(case_ids)
        id_str = f"timeline:{':'.join(cases)}"
        return hashlib.md5(id_str.encode()).hexdigest()[:16]
    
    def _generate_network_id(self, case_ids: List[str]) -> str:
        """Generate unique network ID"""
        cases = sorted(case_ids)
        id_str = f"network:{':'.join(cases)}"
        return hashlib.md5(id_str.encode()).hexdigest()[:16]
    
    def _calculate_link_strength(self, evidence: List[EntityMatch]) -> float:
        """Calculate link strength based on evidence"""
        if not evidence:
            return 0.0
        
        # Weight different types of evidence
        type_weights = {
            'phone': 0.4,
            'email': 0.3,
            'device': 0.5,
            'location': 0.2,
            'name': 0.1
        }
        
        total_weight = 0.0
        for match in evidence:
            weight = type_weights.get(match.match_type, 0.1)
            total_weight += weight * match.confidence
        
        # Normalize to 0-1 range (max weight per type)
        max_possible = sum(type_weights.values())
        strength = min(total_weight / max_possible, 1.0)
        
        return strength
    
    def _count_by_type(self, items: List, type_field: str) -> Dict[str, int]:
        """Count items by type field"""
        counts = defaultdict(int)
        for item in items:
            item_type = getattr(item, type_field, 'unknown')
            counts[item_type] += 1
        return dict(counts)
    
    def reset(self):
        """Reset all stored data"""
        self.cases.clear()
        self.entity_matches.clear()
        self.case_links.clear()
        self.timelines.clear()
        self.networks.clear()
        self.phone_index.clear()
        self.email_index.clear()
        self.name_index.clear()
        self.location_index.clear()
        self.device_index.clear()


# Singleton instance
_cross_case_linker_instance = None


def get_cross_case_linker() -> CrossCaseLinker:
    """Get singleton CrossCaseLinker instance"""
    global _cross_case_linker_instance
    if _cross_case_linker_instance is None:
        _cross_case_linker_instance = CrossCaseLinker()
    return _cross_case_linker_instance


# Test function
def _test_cross_case_linker():
    """Test the cross-case linker with sample data"""
    print("=" * 70)
    print("Testing Step 11: Cross-Case Linking")
    print("=" * 70)
    
    linker = CrossCaseLinker()  # Fresh instance for testing
    
    # Create sample case data
    case1_data = {
        'contacts': [
            {'name': 'John Doe', 'phone_numbers': ['+1234567890'], 'emails': ['john@example.com']},
            {'name': 'Jane Smith', 'phone_numbers': ['+9876543210'], 'emails': ['jane@example.com']},
            {'name': 'Bob Wilson', 'phone_numbers': ['+1111111111'], 'emails': []}
        ],
        'messages': [
            {'from_person': '+1234567890', 'to_person': '+9876543210', 'timestamp': '2025-01-01T10:00:00Z', 'text': 'Hello'}
        ],
        'calls': [
            {'from_number': '+1234567890', 'to_number': '+9876543210', 'timestamp': '2025-01-01T11:00:00Z', 'duration': 300}
        ],
        'locations': [
            {'latitude': 40.7128, 'longitude': -74.0060, 'timestamp': '2025-01-01T09:00:00Z'}
        ],
        'devices': [
            {'device_id': 'device_001', 'phone_numbers': ['+1234567890']}
        ],
        'media': []
    }
    
    case2_data = {
        'contacts': [
            {'name': 'John Doe', 'phone_numbers': ['+1234567890'], 'emails': ['john@example.com']},  # Shared contact
            {'name': 'Alice Brown', 'phone_numbers': ['+2222222222'], 'emails': ['alice@example.com']},
            {'name': 'Jane Smith', 'phone_numbers': ['+9876543210'], 'emails': ['jane@example.com']}  # Shared contact
        ],
        'messages': [
            {'from_person': '+1234567890', 'to_person': '+2222222222', 'timestamp': '2025-01-02T10:00:00Z', 'text': 'Hi Alice'}
        ],
        'calls': [
            {'from_number': '+2222222222', 'to_number': '+9876543210', 'timestamp': '2025-01-02T11:00:00Z', 'duration': 600}
        ],
        'locations': [
            {'latitude': 40.7128, 'longitude': -74.0060, 'timestamp': '2025-01-02T09:00:00Z'}  # Shared location
        ],
        'devices': [
            {'device_id': 'device_002', 'phone_numbers': ['+2222222222']}
        ],
        'media': []
    }
    
    # Load cases
    print("\n📥 Loading cases...")
    linker.load_case('CASE_001', case1_data)
    linker.load_case('CASE_002', case2_data)
    print("   ✅ Loaded 2 cases")
    
    # Find shared contacts
    print("\n👥 Finding shared contacts...")
    shared_contacts = linker.find_shared_contacts()
    print(f"   ✅ Found {len(shared_contacts)} shared contacts")
    for match in shared_contacts:
        print(f"      - {match.match_type}: {match.match_attributes} (confidence: {match.confidence:.2f})")
    
    # Find shared locations
    print("\n📍 Finding shared locations...")
    shared_locations = linker.find_shared_locations()
    print(f"   ✅ Found {len(shared_locations)} shared locations")
    for match in shared_locations:
        print(f"      - Location: ({match.match_attributes['latitude']}, {match.match_attributes['longitude']})")
    
    # Find shared devices
    print("\n📱 Finding shared devices...")
    shared_devices = linker.find_shared_devices()
    print(f"   ✅ Found {len(shared_devices)} shared devices")
    
    # Create case links
    print("\n🔗 Creating case links...")
    links = linker.create_case_links()
    print(f"   ✅ Created {len(links)} case links")
    for link in links:
        print(f"      - {link.case_1_id} ↔ {link.case_2_id}: {link.link_type} (strength: {link.strength:.2f})")
    
    # Create timeline
    print("\n📅 Creating cross-case timeline...")
    timeline = linker.create_cross_case_timeline()
    print(f"   ✅ Created timeline with {len(timeline.events)} events")
    print(f"      Time range: {timeline.start_time} to {timeline.end_time}")
    
    # Create communication network
    print("\n🌐 Creating communication network...")
    network = linker.create_communication_network()
    print(f"   ✅ Created network with {len(network.entities)} entities and {len(network.relationships)} relationships")
    
    # Get statistics
    stats = linker.get_statistics()
    print("\n📊 Statistics:")
    print(f"   Total cases: {stats['total_cases']}")
    print(f"   Total entity matches: {stats['total_entity_matches']}")
    print(f"   Matches by type: {stats['entity_matches_by_type']}")
    print(f"   Total case links: {stats['total_case_links']}")
    print(f"   Indexed phones: {stats['indexed_phones']}")
    print(f"   Indexed emails: {stats['indexed_emails']}")
    
    # Test results
    print("\n" + "=" * 70)
    expected_shared_contacts = 4  # 2 phones + 2 emails
    expected_shared_locations = 1
    expected_links = 1
    
    passed = 0
    failed = 0
    
    if len(shared_contacts) >= expected_shared_contacts:
        print("✅ Shared contacts test PASSED")
        passed += 1
    else:
        print(f"❌ Shared contacts test FAILED (expected >= {expected_shared_contacts}, got {len(shared_contacts)})")
        failed += 1
    
    if len(shared_locations) >= expected_shared_locations:
        print("✅ Shared locations test PASSED")
        passed += 1
    else:
        print(f"❌ Shared locations test FAILED (expected >= {expected_shared_locations}, got {len(shared_locations)})")
        failed += 1
    
    if len(links) >= expected_links:
        print("✅ Case links test PASSED")
        passed += 1
    else:
        print(f"❌ Case links test FAILED (expected >= {expected_links}, got {len(links)})")
        failed += 1
    
    if len(timeline.events) > 0:
        print("✅ Timeline test PASSED")
        passed += 1
    else:
        print("❌ Timeline test FAILED")
        failed += 1
    
    if len(network.entities) > 0:
        print("✅ Network test PASSED")
        passed += 1
    else:
        print("❌ Network test FAILED")
        failed += 1
    
    print("=" * 70)
    print(f"✅ Tests Passed: {passed}")
    print(f"❌ Tests Failed: {failed}")
    print(f"📊 Success Rate: {passed}/{passed+failed} ({100*passed/(passed+failed):.0f}%)")
    print("=" * 70)
    
    return passed, failed


if __name__ == '__main__':
    _test_cross_case_linker()
