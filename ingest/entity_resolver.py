"""
Step 8: Semantic Deduplication & Entity Resolution
Detects duplicate contacts across cases using:
- Fuzzy name matching (Levenshtein distance)
- Phone number normalization
- Cross-case entity linking
- Confidence-based merging
"""

import re
import unicodedata
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MatchConfidence(Enum):
    """Confidence levels for entity matching"""
    HIGH = "high"      # 0.9+ - Very confident match
    MEDIUM = "medium"  # 0.7-0.9 - Likely match
    LOW = "low"        # 0.5-0.7 - Possible match
    NONE = "none"      # < 0.5 - Not a match


@dataclass
class EntityMatch:
    """Result of entity matching"""
    entity_1_id: str
    entity_2_id: str
    confidence: float
    match_reasons: List[str]
    phone_match: bool
    name_similarity: float
    email_match: bool
    
    def get_confidence_level(self) -> MatchConfidence:
        """Get confidence level from score"""
        if self.confidence >= 0.9:
            return MatchConfidence.HIGH
        elif self.confidence >= 0.7:
            return MatchConfidence.MEDIUM
        elif self.confidence >= 0.5:
            return MatchConfidence.LOW
        return MatchConfidence.NONE


@dataclass
class Entity:
    """Canonical entity representation"""
    entity_id: str
    names: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)
    emails: List[str] = field(default_factory=list)
    case_ids: List[str] = field(default_factory=list)
    source_ids: List[str] = field(default_factory=list)  # Original contact IDs
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'entity_id': self.entity_id,
            'names': self.names,
            'phones': self.phones,
            'emails': self.emails,
            'case_ids': self.case_ids,
            'source_ids': self.source_ids,
            'metadata': self.metadata
        }


class PhoneNormalizer:
    """Normalizes phone numbers to E.164 format"""
    
    # Country code mappings
    COUNTRY_CODES = {
        'IN': '+91',   # India
        'US': '+1',    # USA
        'GB': '+44',   # UK
        'AU': '+61',   # Australia
        'JP': '+81',   # Japan
        'CN': '+86',   # China
    }
    
    @staticmethod
    def normalize(phone: str, default_country: str = 'IN') -> Optional[str]:
        """
        Normalize phone number to E.164 format
        
        Args:
            phone: Raw phone number
            default_country: Default country code if not specified
            
        Returns:
            Normalized phone in E.164 format (+CCXXXXXXXXXX)
        """
        if not phone:
            return None
        
        # Remove all non-digit characters except leading +
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        if not cleaned:
            return None
        
        # Handle different formats
        if cleaned.startswith('+'):
            # Already has country code
            return cleaned
        
        elif cleaned.startswith('00'):
            # International format (00XX...)
            return '+' + cleaned[2:]
        
        elif cleaned.startswith('0') and len(cleaned) == 11:
            # Indian format (0XXXXXXXXXX) → +91XXXXXXXXXX
            if default_country == 'IN':
                return '+91' + cleaned[1:]
            return '+' + PhoneNormalizer.COUNTRY_CODES.get(default_country, '+91').lstrip('+') + cleaned[1:]
        
        elif len(cleaned) == 10:
            # Local format (XXXXXXXXXX) → +91XXXXXXXXXX
            country_code = PhoneNormalizer.COUNTRY_CODES.get(default_country, '+91')
            return country_code + cleaned
        
        elif len(cleaned) > 10:
            # Assume it already has country code without +
            return '+' + cleaned
        
        # Fallback: assume local number
        country_code = PhoneNormalizer.COUNTRY_CODES.get(default_country, '+91')
        return country_code + cleaned
    
    @staticmethod
    def extract_variations(phone: str) -> Set[str]:
        """
        Extract all possible variations of a phone number
        
        Returns:
            Set of phone number variations
        """
        if not phone:
            return set()
        
        variations = {phone}
        
        # Try normalizing with different country codes
        for country in ['IN', 'US', 'GB']:
            normalized = PhoneNormalizer.normalize(phone, default_country=country)
            if normalized:
                variations.add(normalized)
        
        # Add without country code
        digits_only = re.sub(r'[^\d]', '', phone)
        if len(digits_only) >= 10:
            variations.add(digits_only[-10:])  # Last 10 digits
        
        return variations


class FuzzyNameMatcher:
    """Fuzzy name matching using Levenshtein distance and Unicode normalization"""
    
    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """
        Calculate Levenshtein distance between two strings
        
        Args:
            s1: First string
            s2: Second string
            
        Returns:
            Edit distance
        """
        if len(s1) < len(s2):
            return FuzzyNameMatcher.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                # Cost of insertions, deletions, or substitutions
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """
        Normalize name for comparison
        
        Args:
            name: Raw name
            
        Returns:
            Normalized name (lowercase, no punctuation, normalized unicode)
        """
        if not name:
            return ""
        
        # Unicode normalization (NFD)
        normalized = unicodedata.normalize('NFD', name)
        
        # Lowercase
        normalized = normalized.lower()
        
        # Remove punctuation and extra spaces
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    @staticmethod
    def similarity_score(name1: str, name2: str) -> float:
        """
        Calculate similarity score between two names
        
        Args:
            name1: First name
            name2: Second name
            
        Returns:
            Similarity score (0.0 - 1.0)
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        norm1 = FuzzyNameMatcher.normalize_name(name1)
        norm2 = FuzzyNameMatcher.normalize_name(name2)
        
        if not norm1 or not norm2:
            return 0.0
        
        # Exact match
        if norm1 == norm2:
            return 1.0
        
        # Token-based matching (handles reordering)
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())
        
        if tokens1 == tokens2:
            return 0.95  # Same tokens, different order
        
        # Jaccard similarity for token sets
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        token_similarity = len(intersection) / len(union) if union else 0.0
        
        # Levenshtein similarity
        max_len = max(len(norm1), len(norm2))
        if max_len == 0:
            return 0.0
        
        distance = FuzzyNameMatcher.levenshtein_distance(norm1, norm2)
        levenshtein_similarity = 1.0 - (distance / max_len)
        
        # Combine scores (weighted average)
        final_score = (token_similarity * 0.6) + (levenshtein_similarity * 0.4)
        
        return final_score
    
    @staticmethod
    def is_likely_same_person(name1: str, name2: str, threshold: float = 0.7) -> Tuple[bool, float]:
        """
        Check if two names likely refer to the same person
        
        Args:
            name1: First name
            name2: Second name
            threshold: Similarity threshold
            
        Returns:
            (is_match, similarity_score)
        """
        similarity = FuzzyNameMatcher.similarity_score(name1, name2)
        return (similarity >= threshold, similarity)


class EntityGraph:
    """Graph structure for managing entity relationships"""
    
    def __init__(self):
        self.entities: Dict[str, Entity] = {}
        self.matches: List[EntityMatch] = []
    
    def add_entity(self, entity: Entity):
        """Add entity to graph"""
        self.entities[entity.entity_id] = entity
    
    def add_match(self, match: EntityMatch):
        """Add entity match"""
        self.matches.append(match)
    
    def get_entity(self, entity_id: str) -> Optional[Entity]:
        """Get entity by ID"""
        return self.entities.get(entity_id)
    
    def get_matches_for_entity(self, entity_id: str, min_confidence: float = 0.7) -> List[EntityMatch]:
        """Get all matches for an entity above confidence threshold"""
        matches = []
        for match in self.matches:
            if (match.entity_1_id == entity_id or match.entity_2_id == entity_id) and \
               match.confidence >= min_confidence:
                matches.append(match)
        return matches
    
    def merge_entities(self, entity_1_id: str, entity_2_id: str) -> Optional[Entity]:
        """
        Merge two entities into one
        
        Args:
            entity_1_id: First entity ID (canonical)
            entity_2_id: Second entity ID (to be merged)
            
        Returns:
            Merged entity
        """
        entity_1 = self.get_entity(entity_1_id)
        entity_2 = self.get_entity(entity_2_id)
        
        if not entity_1 or not entity_2:
            return None
        
        # Merge data into entity_1
        entity_1.names = list(set(entity_1.names + entity_2.names))
        entity_1.phones = list(set(entity_1.phones + entity_2.phones))
        entity_1.emails = list(set(entity_1.emails + entity_2.emails))
        entity_1.case_ids = list(set(entity_1.case_ids + entity_2.case_ids))
        entity_1.source_ids = list(set(entity_1.source_ids + entity_2.source_ids))
        
        # Remove entity_2
        del self.entities[entity_2_id]
        
        logger.info(f"Merged entities: {entity_1_id} ← {entity_2_id}")
        
        return entity_1


class DuplicateDetector:
    """Main duplicate detection engine"""
    
    def __init__(self, phone_match_weight: float = 0.6, name_match_weight: float = 0.3,
                 email_match_weight: float = 0.1):
        """
        Initialize duplicate detector
        
        Args:
            phone_match_weight: Weight for phone number matching
            name_match_weight: Weight for name matching
            email_match_weight: Weight for email matching
        """
        self.phone_normalizer = PhoneNormalizer()
        self.name_matcher = FuzzyNameMatcher()
        
        # Matching weights
        self.phone_weight = phone_match_weight
        self.name_weight = name_match_weight
        self.email_weight = email_match_weight
    
    def compare_contacts(self, contact1: Dict, contact2: Dict) -> EntityMatch:
        """
        Compare two contacts and calculate match confidence
        
        Args:
            contact1: First contact dict with 'id', 'name', 'phone', 'email'
            contact2: Second contact dict
            
        Returns:
            EntityMatch result
        """
        reasons = []
        scores = {}
        
        # Phone matching
        phone1 = contact1.get('phone', '')
        phone2 = contact2.get('phone', '')
        
        phone_match = False
        if phone1 and phone2:
            # Normalize phones
            norm_phone1 = self.phone_normalizer.normalize(phone1)
            norm_phone2 = self.phone_normalizer.normalize(phone2)
            
            if norm_phone1 and norm_phone2:
                # Check exact match
                if norm_phone1 == norm_phone2:
                    phone_match = True
                    reasons.append(f"phone_exact_match: {norm_phone1}")
                    scores['phone'] = 1.0
                else:
                    # Check variations
                    vars1 = self.phone_normalizer.extract_variations(phone1)
                    vars2 = self.phone_normalizer.extract_variations(phone2)
                    
                    if vars1 & vars2:  # Intersection
                        phone_match = True
                        reasons.append(f"phone_variation_match: {vars1 & vars2}")
                        scores['phone'] = 0.9
        
        if not phone_match:
            scores['phone'] = 0.0
        
        # Name matching
        name1 = contact1.get('name', '')
        name2 = contact2.get('name', '')
        
        name_similarity = 0.0
        if name1 and name2:
            is_match, similarity = self.name_matcher.is_likely_same_person(name1, name2)
            name_similarity = similarity
            scores['name'] = similarity
            
            if similarity > 0.9:
                reasons.append(f"name_exact_match: similarity={similarity:.2f}")
            elif similarity > 0.7:
                reasons.append(f"name_fuzzy_match: similarity={similarity:.2f}")
            elif similarity > 0.5:
                reasons.append(f"name_possible_match: similarity={similarity:.2f}")
        else:
            scores['name'] = 0.0
        
        # Email matching
        email1 = (contact1.get('email') or '').lower().strip() if contact1.get('email') else ''
        email2 = (contact2.get('email') or '').lower().strip() if contact2.get('email') else ''
        
        email_match = False
        if email1 and email2 and email1 == email2:
            email_match = True
            reasons.append(f"email_match: {email1}")
            scores['email'] = 1.0
        else:
            scores['email'] = 0.0
        
        # Calculate weighted confidence
        confidence = (
            scores['phone'] * self.phone_weight +
            scores['name'] * self.name_weight +
            scores['email'] * self.email_weight
        )
        
        # Create match result
        match = EntityMatch(
            entity_1_id=contact1.get('id', ''),
            entity_2_id=contact2.get('id', ''),
            confidence=confidence,
            match_reasons=reasons,
            phone_match=phone_match,
            name_similarity=name_similarity,
            email_match=email_match
        )
        
        return match
    
    def detect_duplicates(self, contacts: List[Dict], 
                         min_confidence: float = 0.7) -> List[EntityMatch]:
        """
        Detect duplicate contacts in a list
        
        Args:
            contacts: List of contact dicts
            min_confidence: Minimum confidence threshold for matches
            
        Returns:
            List of entity matches above threshold
        """
        matches = []
        
        # Compare all pairs
        for i in range(len(contacts)):
            for j in range(i + 1, len(contacts)):
                match = self.compare_contacts(contacts[i], contacts[j])
                
                if match.confidence >= min_confidence:
                    matches.append(match)
                    logger.info(f"Found duplicate: {contacts[i].get('name')} ≈ {contacts[j].get('name')} "
                              f"(confidence: {match.confidence:.2f})")
        
        return matches
    
    def resolve_entities(self, contacts: List[Dict], 
                        min_merge_confidence: float = 0.8) -> Dict:
        """
        Resolve entities by merging duplicates
        
        Args:
            contacts: List of contact dicts
            min_merge_confidence: Minimum confidence to merge entities
            
        Returns:
            Dict with 'entities', 'matches', 'merge_count'
        """
        # Create fresh entity graph for this resolution
        entity_graph = EntityGraph()
        
        # Create initial entities
        for contact in contacts:
            entity = Entity(
                entity_id=contact.get('id', str(id(contact))),
                names=[contact.get('name', '')] if contact.get('name') else [],
                phones=[contact.get('phone', '')] if contact.get('phone') else [],
                emails=[contact.get('email', '')] if contact.get('email') else [],
                case_ids=[contact.get('case_id', 'unknown')],
                source_ids=[contact.get('id', str(id(contact)))],
                metadata=contact.get('metadata', {})
            )
            entity_graph.add_entity(entity)
        
        # Detect duplicates
        matches = self.detect_duplicates(contacts, min_confidence=0.5)
        
        for match in matches:
            entity_graph.add_match(match)
        
        # Merge high-confidence matches
        merge_count = 0
        processed_ids = set()
        
        for match in sorted(matches, key=lambda m: m.confidence, reverse=True):
            if match.confidence >= min_merge_confidence:
                # Check if already merged
                if match.entity_1_id in processed_ids or match.entity_2_id in processed_ids:
                    continue
                
                merged = entity_graph.merge_entities(match.entity_1_id, match.entity_2_id)
                if merged:
                    merge_count += 1
                    processed_ids.add(match.entity_2_id)
        
        # Return results
        result = {
            'entities': [e.to_dict() for e in entity_graph.entities.values()],
            'matches': matches,
            'merge_count': merge_count,
            'total_entities': len(entity_graph.entities),
            'total_matches': len(matches),
            'high_confidence_matches': sum(1 for m in matches if m.confidence >= 0.9),
            'medium_confidence_matches': sum(1 for m in matches if 0.7 <= m.confidence < 0.9),
            'low_confidence_matches': sum(1 for m in matches if 0.5 <= m.confidence < 0.7)
        }
        
        return result


# Singleton instance
_entity_resolver = None

def get_entity_resolver() -> DuplicateDetector:
    """Get singleton entity resolver instance"""
    global _entity_resolver
    if _entity_resolver is None:
        _entity_resolver = DuplicateDetector()
    return _entity_resolver


# ============================================================
# TESTING CODE
# ============================================================

def test_phone_normalization():
    """Test phone number normalization"""
    print("\n" + "="*70)
    print("TEST 1: Phone Number Normalization")
    print("="*70)
    
    normalizer = PhoneNormalizer()
    
    test_cases = [
        ("+919876543210", "+919876543210"),
        ("919876543210", "+919876543210"),
        ("09876543210", "+919876543210"),
        ("9876543210", "+919876543210"),
        ("+1-415-555-1234", "+14155551234"),
        ("00919876543210", "+919876543210"),
    ]
    
    for input_phone, expected in test_cases:
        result = normalizer.normalize(input_phone)
        status = "✅" if result == expected else "❌"
        print(f"{status} {input_phone:20} → {result:20} (expected: {expected})")


def test_fuzzy_name_matching():
    """Test fuzzy name matching"""
    print("\n" + "="*70)
    print("TEST 2: Fuzzy Name Matching")
    print("="*70)
    
    matcher = FuzzyNameMatcher()
    
    test_cases = [
        ("Rajesh Sharma", "Rajesh Sharma", True, 1.0),
        ("Rajesh Sharma", "Sharma Rajesh", True, 0.95),
        ("राज कुमार", "राज कुमार", True, 1.0),
        ("Rajesh", "Rajeesh", True, 0.8),  # Typo
        ("John Smith", "Jane Smith", False, 0.5),  # Different person
        ("Mr. Rajesh Sharma", "Rajesh Sharma", True, 0.85),  # Honorific
    ]
    
    for name1, name2, expected_match, min_similarity in test_cases:
        is_match, similarity = matcher.is_likely_same_person(name1, name2, threshold=0.7)
        status = "✅" if is_match == expected_match else "❌"
        print(f"{status} '{name1}' ≈ '{name2}': similarity={similarity:.2f} (match={is_match})")


def test_duplicate_detection():
    """Test complete duplicate detection"""
    print("\n" + "="*70)
    print("TEST 3: Duplicate Detection")
    print("="*70)
    
    contacts = [
        {'id': 'c1', 'name': 'Rajesh Sharma', 'phone': '+919876543210', 'email': 'rajesh@example.com', 'case_id': 'case1'},
        {'id': 'c2', 'name': 'Rajesh Sharma', 'phone': '9876543210', 'email': '', 'case_id': 'case2'},  # Duplicate
        {'id': 'c3', 'name': 'Sharma Rajesh', 'phone': '+919876543210', 'email': 'rajesh@example.com', 'case_id': 'case3'},  # Duplicate (reordered)
        {'id': 'c4', 'name': 'Priya Verma', 'phone': '+918765432109', 'email': 'priya@example.com', 'case_id': 'case1'},
        {'id': 'c5', 'name': 'Priya', 'phone': '8765432109', 'email': '', 'case_id': 'case2'},  # Partial match
        {'id': 'c6', 'name': 'John Smith', 'phone': '+14155551234', 'email': 'john@example.com', 'case_id': 'case1'},
    ]
    
    detector = get_entity_resolver()
    
    # Detect duplicates
    results = detector.resolve_entities(contacts, min_merge_confidence=0.8)
    
    print(f"\n📊 Results:")
    print(f"   Total Contacts: {len(contacts)}")
    print(f"   Unique Entities: {results['total_entities']}")
    print(f"   Duplicates Merged: {results['merge_count']}")
    print(f"   Total Matches Found: {results['total_matches']}")
    print(f"   High Confidence: {results['high_confidence_matches']}")
    print(f"   Medium Confidence: {results['medium_confidence_matches']}")
    print(f"   Low Confidence: {results['low_confidence_matches']}")
    
    print(f"\n🔗 Matches Found:")
    for match in results['matches']:
        c1 = next(c for c in contacts if c['id'] == match.entity_1_id)
        c2 = next(c for c in contacts if c['id'] == match.entity_2_id)
        print(f"\n   {match.get_confidence_level().value.upper()} (confidence: {match.confidence:.2f})")
        print(f"   '{c1['name']}' ≈ '{c2['name']}'")
        print(f"   Reasons: {', '.join(match.match_reasons)}")
    
    print(f"\n✅ Entity Resolution Complete!")
    print(f"   Original: {len(contacts)} contacts")
    print(f"   Resolved: {results['total_entities']} unique entities")
    print(f"   Reduction: {len(contacts) - results['total_entities']} duplicates removed")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("🔍 STEP 8: SEMANTIC DEDUPLICATION & ENTITY RESOLUTION")
    print("="*70)
    
    # Run tests
    test_phone_normalization()
    test_fuzzy_name_matching()
    test_duplicate_detection()
    
    print("\n" + "="*70)
    print("✅ All tests complete!")
    print("="*70)
