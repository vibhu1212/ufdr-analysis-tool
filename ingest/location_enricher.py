"""
Step 10: Location Enrichment Module

This module provides location data normalization, geocoding, clustering, and enrichment
capabilities for the UFDR ingestion pipeline. It handles:

- GPS coordinate normalization and validation
- Address parsing and standardization
- Reverse geocoding (coordinates to address)
- Location clustering (grouping nearby locations)
- Distance calculations between coordinates
- Location type inference (home, work, etc.)
- Accuracy assessment

All operations are designed for forensic use with full provenance tracking.

Author: UFDR Analysis Tool
Date: October 2025
"""

import re
import math
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class Coordinate:
    """Represents a normalized GPS coordinate"""
    latitude: float
    longitude: float
    accuracy: Optional[float] = None  # meters
    altitude: Optional[float] = None  # meters
    source_format: Optional[str] = None
    normalized: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_dms(self) -> str:
        """Convert to Degrees Minutes Seconds format"""
        def decimal_to_dms(decimal: float, is_longitude: bool) -> str:
            direction = ''
            if is_longitude:
                direction = 'E' if decimal >= 0 else 'W'
            else:
                direction = 'N' if decimal >= 0 else 'S'
            
            decimal = abs(decimal)
            degrees = int(decimal)
            minutes_decimal = (decimal - degrees) * 60
            minutes = int(minutes_decimal)
            seconds = (minutes_decimal - minutes) * 60
            
            return f"{degrees}°{minutes}'{seconds:.2f}\"{direction}"
        
        lat_dms = decimal_to_dms(self.latitude, False)
        lon_dms = decimal_to_dms(self.longitude, True)
        return f"{lat_dms}, {lon_dms}"


@dataclass
class Address:
    """Represents a parsed and standardized address"""
    raw: str
    street_number: Optional[str] = None
    street_name: Optional[str] = None
    unit: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    standardized: Optional[str] = None
    confidence: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class Location:
    """Represents an enriched location with coordinates and address"""
    location_id: str
    coordinate: Optional[Coordinate] = None
    address: Optional[Address] = None
    location_type: Optional[str] = None  # home, work, transit, poi, etc.
    name: Optional[str] = None
    visit_count: int = 1
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            'location_id': self.location_id,
            'coordinate': self.coordinate.to_dict() if self.coordinate else None,
            'address': self.address.to_dict() if self.address else None,
            'location_type': self.location_type,
            'name': self.name,
            'visit_count': self.visit_count,
            'first_seen': self.first_seen,
            'last_seen': self.last_seen,
            'metadata': self.metadata
        }
        return result


@dataclass
class LocationCluster:
    """Represents a cluster of nearby locations"""
    cluster_id: str
    centroid: Coordinate
    locations: List[str]  # location_ids
    radius_meters: float
    visit_count: int
    cluster_type: Optional[str] = None  # frequent, home, work, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'cluster_id': self.cluster_id,
            'centroid': self.centroid.to_dict(),
            'locations': self.locations,
            'radius_meters': self.radius_meters,
            'visit_count': self.visit_count,
            'cluster_type': self.cluster_type
        }


class LocationEnricher:
    """
    Enriches location data with normalization, parsing, and clustering capabilities.
    
    This class handles all location-related operations for the UFDR pipeline:
    - Coordinate normalization and validation
    - Address parsing and standardization
    - Distance calculations
    - Location clustering
    - Location type inference
    """
    
    def __init__(self):
        """Initialize the location enricher"""
        self.locations: Dict[str, Location] = {}
        self.clusters: Dict[str, LocationCluster] = {}
        
        # Coordinate formats regex patterns
        self.coord_patterns = {
            'decimal': re.compile(r'^(-?\d+\.?\d*)[,\s]+(-?\d+\.?\d*)$'),
            'dms': re.compile(r"(\d+)°\s*(\d+)'\s*([\d.]+)\"?\s*([NSEW])"),
            'exif': re.compile(r'^\[?(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]?$'),
        }
        
        # Address component patterns
        self.address_patterns = {
            'postal_code_us': re.compile(r'\b\d{5}(?:-\d{4})?\b'),
            'postal_code_uk': re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b', re.IGNORECASE),
            'postal_code_ca': re.compile(r'\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b', re.IGNORECASE),
            'postal_code_in': re.compile(r'\b\d{6}\b'),
            'street_number': re.compile(r'^\d+[A-Z]?(?:\s+\w+)?'),
            'unit': re.compile(r'(?:apt|unit|suite|#)\s*[\w-]+', re.IGNORECASE),
        }
        
        # Country detection patterns
        self.country_patterns = {
            'USA': ['USA', 'United States', 'US', 'America'],
            'India': ['India', 'Bharat'],
            'UK': ['UK', 'United Kingdom', 'England', 'Scotland', 'Wales'],
            'Canada': ['Canada', 'CA'],
            'Australia': ['Australia', 'AU'],
        }
        
        # State abbreviations (US)
        self.us_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        }
        
        # Common location types
        self.poi_keywords = {
            'home': ['home', 'residence', 'house', 'apartment'],
            'work': ['office', 'work', 'workplace', 'company'],
            'restaurant': ['restaurant', 'cafe', 'diner', 'eatery'],
            'shop': ['store', 'shop', 'mall', 'market'],
            'school': ['school', 'university', 'college', 'academy'],
            'hospital': ['hospital', 'clinic', 'medical'],
            'transit': ['station', 'airport', 'bus', 'train', 'metro'],
        }
    
    def normalize_coordinate(self, coord_input: Any) -> Optional[Coordinate]:
        """
        Normalize GPS coordinates from various formats to decimal degrees.
        
        Supported formats:
        - Decimal: "28.6139, 77.2090" or (28.6139, 77.2090)
        - DMS: "28°36'50"N, 77°12'32"E"
        - EXIF: [28.6139, 77.2090]
        - Dict: {'latitude': 28.6139, 'longitude': 77.2090}
        
        Args:
            coord_input: Coordinate in various formats
            
        Returns:
            Normalized Coordinate object or None if invalid
        """
        try:
            # Handle dict input
            if isinstance(coord_input, dict):
                lat = coord_input.get('latitude') or coord_input.get('lat')
                lon = coord_input.get('longitude') or coord_input.get('lon') or coord_input.get('lng')
                accuracy = coord_input.get('accuracy')
                altitude = coord_input.get('altitude')
                
                if lat is not None and lon is not None:
                    lat, lon = float(lat), float(lon)
                    if self._validate_coordinate(lat, lon):
                        return Coordinate(
                            latitude=lat,
                            longitude=lon,
                            accuracy=float(accuracy) if accuracy else None,
                            altitude=float(altitude) if altitude else None,
                            source_format='dict'
                        )
            
            # Handle tuple/list input
            elif isinstance(coord_input, (tuple, list)) and len(coord_input) >= 2:
                lat, lon = float(coord_input[0]), float(coord_input[1])
                if self._validate_coordinate(lat, lon):
                    return Coordinate(
                        latitude=lat,
                        longitude=lon,
                        source_format='tuple'
                    )
            
            # Handle string input
            elif isinstance(coord_input, str):
                coord_str = coord_input.strip()
                
                # Try decimal format
                match = self.coord_patterns['decimal'].match(coord_str)
                if match:
                    lat, lon = float(match.group(1)), float(match.group(2))
                    if self._validate_coordinate(lat, lon):
                        return Coordinate(
                            latitude=lat,
                            longitude=lon,
                            source_format='decimal_string'
                        )
                
                # Try DMS format
                dms_matches = self.coord_patterns['dms'].findall(coord_str)
                if len(dms_matches) >= 2:
                    lat = self._dms_to_decimal(*dms_matches[0])
                    lon = self._dms_to_decimal(*dms_matches[1])
                    if self._validate_coordinate(lat, lon):
                        return Coordinate(
                            latitude=lat,
                            longitude=lon,
                            source_format='dms'
                        )
                
                # Try EXIF format
                match = self.coord_patterns['exif'].match(coord_str)
                if match:
                    lat, lon = float(match.group(1)), float(match.group(2))
                    if self._validate_coordinate(lat, lon):
                        return Coordinate(
                            latitude=lat,
                            longitude=lon,
                            source_format='exif'
                        )
            
            return None
            
        except (ValueError, TypeError, AttributeError):
            return None
    
    def _validate_coordinate(self, lat: float, lon: float) -> bool:
        """Validate latitude and longitude ranges"""
        return -90 <= lat <= 90 and -180 <= lon <= 180
    
    def _dms_to_decimal(self, degrees: str, minutes: str, seconds: str, direction: str) -> float:
        """Convert DMS (Degrees Minutes Seconds) to decimal"""
        decimal = float(degrees) + float(minutes)/60 + float(seconds)/3600
        if direction in ['S', 'W']:
            decimal = -decimal
        return decimal
    
    def parse_address(self, address_input: Any) -> Optional[Address]:
        """
        Parse and standardize address from various formats.
        
        Args:
            address_input: Address string or dict
            
        Returns:
            Parsed Address object or None if invalid
        """
        if not address_input:
            return None
        
        try:
            # Handle dict input
            if isinstance(address_input, dict):
                return Address(
                    raw=str(address_input),
                    street_number=address_input.get('street_number'),
                    street_name=address_input.get('street_name'),
                    unit=address_input.get('unit'),
                    city=address_input.get('city'),
                    state=address_input.get('state'),
                    postal_code=address_input.get('postal_code'),
                    country=address_input.get('country'),
                    standardized=address_input.get('standardized'),
                    confidence=1.0
                )
            
            # Handle string input
            address_str = str(address_input).strip()
            if not address_str:
                return None
            
            # Parse components
            components = {
                'raw': address_str,
                'street_number': None,
                'street_name': None,
                'unit': None,
                'city': None,
                'state': None,
                'postal_code': None,
                'country': None,
                'confidence': 0.5
            }
            
            # Extract postal code
            for pattern_name, pattern in self.address_patterns.items():
                if pattern_name.startswith('postal_code'):
                    match = pattern.search(address_str)
                    if match:
                        components['postal_code'] = match.group(0)
                        components['confidence'] += 0.1
                        break
            
            # Extract unit number
            unit_match = self.address_patterns['unit'].search(address_str)
            if unit_match:
                components['unit'] = unit_match.group(0)
                components['confidence'] += 0.05
            
            # Extract street number
            street_match = self.address_patterns['street_number'].match(address_str)
            if street_match:
                components['street_number'] = street_match.group(0)
                components['confidence'] += 0.1
            
            # Detect country
            for country, keywords in self.country_patterns.items():
                for keyword in keywords:
                    if keyword.lower() in address_str.lower():
                        components['country'] = country
                        components['confidence'] += 0.15
                        break
                if components['country']:
                    break
            
            # Detect US state
            address_upper = address_str.upper()
            for state in self.us_states:
                if f' {state} ' in address_upper or address_upper.endswith(f' {state}'):
                    components['state'] = state
                    components['confidence'] += 0.1
                    break
            
            # Try to extract city (simple heuristic: word before state/postal code)
            parts = address_str.split(',')
            if len(parts) >= 2:
                components['city'] = parts[-2].strip().split()[-1]
                components['confidence'] += 0.05
            
            # Build standardized address
            std_parts = []
            if components['street_number']:
                std_parts.append(components['street_number'])
            if components['unit']:
                std_parts.append(components['unit'])
            if components['city']:
                std_parts.append(components['city'])
            if components['state']:
                std_parts.append(components['state'])
            if components['postal_code']:
                std_parts.append(components['postal_code'])
            if components['country']:
                std_parts.append(components['country'])
            
            components['standardized'] = ', '.join(std_parts) if std_parts else address_str
            
            return Address(**components)
            
        except Exception:
            return None
    
    def calculate_distance(self, coord1: Coordinate, coord2: Coordinate) -> float:
        """
        Calculate distance between two coordinates using Haversine formula.
        
        Args:
            coord1: First coordinate
            coord2: Second coordinate
            
        Returns:
            Distance in meters
        """
        # Earth radius in meters
        R = 6371000
        
        # Convert to radians
        lat1 = math.radians(coord1.latitude)
        lat2 = math.radians(coord2.latitude)
        delta_lat = math.radians(coord2.latitude - coord1.latitude)
        delta_lon = math.radians(coord2.longitude - coord1.longitude)
        
        # Haversine formula
        a = math.sin(delta_lat/2)**2 + \
            math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def infer_location_type(self, location: Location) -> str:
        """
        Infer location type based on name, visit count, and other metadata.
        
        Args:
            location: Location object
            
        Returns:
            Location type (home, work, poi, transit, etc.)
        """
        if not location:
            return 'unknown'
        
        # Check name/address for keywords
        text = ''
        if location.name:
            text += location.name.lower()
        if location.address and location.address.raw:
            text += ' ' + location.address.raw.lower()
        
        for loc_type, keywords in self.poi_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    return loc_type
        
        # Infer from visit count (heuristic)
        if location.visit_count >= 10:
            return 'frequent'
        elif location.visit_count >= 5:
            return 'regular'
        
        return 'poi'
    
    def create_location(self, 
                       coordinate: Optional[Any] = None,
                       address: Optional[Any] = None,
                       name: Optional[str] = None,
                       timestamp: Optional[str] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> Optional[Location]:
        """
        Create an enriched location from raw inputs.
        
        Args:
            coordinate: GPS coordinate (various formats)
            address: Address (string or dict)
            name: Location name
            timestamp: ISO timestamp of visit
            metadata: Additional metadata
            
        Returns:
            Enriched Location object or None
        """
        # Normalize coordinate
        norm_coord = None
        if coordinate:
            norm_coord = self.normalize_coordinate(coordinate)
        
        # Parse address
        parsed_addr = None
        if address:
            parsed_addr = self.parse_address(address)
        
        # Must have at least coordinate or address
        if not norm_coord and not parsed_addr:
            return None
        
        # Generate location ID
        location_id = self._generate_location_id(norm_coord, parsed_addr, name)
        
        # Check if location already exists
        if location_id in self.locations:
            existing = self.locations[location_id]
            existing.visit_count += 1
            if timestamp:
                existing.last_seen = timestamp
            return existing
        
        # Create new location
        location = Location(
            location_id=location_id,
            coordinate=norm_coord,
            address=parsed_addr,
            name=name,
            first_seen=timestamp,
            last_seen=timestamp,
            metadata=metadata
        )
        
        # Infer location type
        location.location_type = self.infer_location_type(location)
        
        # Store location
        self.locations[location_id] = location
        
        return location
    
    def _generate_location_id(self, coord: Optional[Coordinate], 
                              address: Optional[Address], 
                              name: Optional[str]) -> str:
        """Generate unique location ID"""
        components = []
        
        if coord:
            # Round to ~11m precision (5 decimal places)
            components.append(f"{coord.latitude:.5f},{coord.longitude:.5f}")
        
        if address and address.standardized:
            components.append(address.standardized)
        
        if name:
            components.append(name)
        
        id_str = '|'.join(components)
        return hashlib.md5(id_str.encode()).hexdigest()[:16]
    
    def cluster_locations(self, radius_meters: float = 100) -> List[LocationCluster]:
        """
        Cluster nearby locations using simple distance-based clustering.
        
        Args:
            radius_meters: Maximum distance for locations to be in same cluster
            
        Returns:
            List of LocationCluster objects
        """
        if not self.locations:
            return []
        
        # Only cluster locations with coordinates
        locations_with_coords = [
            loc for loc in self.locations.values() 
            if loc.coordinate is not None
        ]
        
        if not locations_with_coords:
            return []
        
        clusters = []
        clustered_ids = set()
        
        for location in locations_with_coords:
            if location.location_id in clustered_ids:
                continue
            
            # Start new cluster
            cluster_locations = [location.location_id]
            cluster_coords = [location.coordinate]
            cluster_visits = location.visit_count
            
            # Find nearby locations
            for other in locations_with_coords:
                if other.location_id == location.location_id:
                    continue
                if other.location_id in clustered_ids:
                    continue
                
                distance = self.calculate_distance(location.coordinate, other.coordinate)
                if distance <= radius_meters:
                    cluster_locations.append(other.location_id)
                    cluster_coords.append(other.coordinate)
                    cluster_visits += other.visit_count
                    clustered_ids.add(other.location_id)
            
            clustered_ids.add(location.location_id)
            
            # Calculate centroid
            avg_lat = sum(c.latitude for c in cluster_coords) / len(cluster_coords)
            avg_lon = sum(c.longitude for c in cluster_coords) / len(cluster_coords)
            centroid = Coordinate(latitude=avg_lat, longitude=avg_lon)
            
            # Determine cluster type
            cluster_type = 'frequent' if cluster_visits >= 10 else 'regular' if cluster_visits >= 5 else 'occasional'
            
            # Create cluster
            cluster = LocationCluster(
                cluster_id=f"cluster_{len(clusters)+1:03d}",
                centroid=centroid,
                locations=cluster_locations,
                radius_meters=radius_meters,
                visit_count=cluster_visits,
                cluster_type=cluster_type
            )
            clusters.append(cluster)
            self.clusters[cluster.cluster_id] = cluster
        
        return clusters
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about processed locations"""
        coord_count = sum(1 for loc in self.locations.values() if loc.coordinate)
        address_count = sum(1 for loc in self.locations.values() if loc.address)
        
        return {
            'total_locations': len(self.locations),
            'with_coordinates': coord_count,
            'with_addresses': address_count,
            'total_clusters': len(self.clusters),
            'total_visits': sum(loc.visit_count for loc in self.locations.values())
        }
    
    def reset(self):
        """Reset all stored locations and clusters"""
        self.locations.clear()
        self.clusters.clear()


# Singleton instance
_location_enricher_instance = None


def get_location_enricher() -> LocationEnricher:
    """Get singleton LocationEnricher instance"""
    global _location_enricher_instance
    if _location_enricher_instance is None:
        _location_enricher_instance = LocationEnricher()
    return _location_enricher_instance


# Test function
def _test_location_enricher():
    """Test the location enricher with various inputs"""
    print("=" * 70)
    print("Testing Step 10: Location Enrichment")
    print("=" * 70)
    
    enricher = LocationEnricher()  # Fresh instance for testing
    
    test_cases = [
        {
            'name': 'Decimal Coordinates',
            'input': {'coordinate': '28.6139, 77.2090', 'name': 'India Gate'},
            'expected': 'coordinate'
        },
        {
            'name': 'Dict Coordinates',
            'input': {'coordinate': {'latitude': 40.7128, 'longitude': -74.0060, 'accuracy': 10}, 'name': 'NYC'},
            'expected': 'coordinate'
        },
        {
            'name': 'DMS Coordinates',
            'input': {'coordinate': '40°42\'46"N, 74°0\'22"W', 'name': 'NYC DMS'},
            'expected': 'coordinate'
        },
        {
            'name': 'US Address',
            'input': {'address': '1600 Pennsylvania Avenue NW, Washington, DC 20500, USA'},
            'expected': 'address'
        },
        {
            'name': 'Indian Address',
            'input': {'address': 'Flat 101, Rajesh Apartments, MG Road, Bangalore 560001, India'},
            'expected': 'address'
        },
        {
            'name': 'Both Coordinate and Address',
            'input': {
                'coordinate': (37.7749, -122.4194),
                'address': '1 Market St, San Francisco, CA 94105',
                'name': 'Office'
            },
            'expected': 'both'
        },
        {
            'name': 'Home Location (repeated)',
            'input': {'coordinate': (37.7749, -122.4194), 'name': 'Home'},
            'expected': 'coordinate'
        },
    ]
    
    passed = 0
    failed = 0
    
    for i, test in enumerate(test_cases, 1):
        try:
            location = enricher.create_location(**test['input'])
            
            if location:
                has_coord = location.coordinate is not None
                has_addr = location.address is not None
                
                success = False
                if test['expected'] == 'coordinate' and has_coord:
                    success = True
                elif test['expected'] == 'address' and has_addr:
                    success = True
                elif test['expected'] == 'both' and has_coord and has_addr:
                    success = True
                
                if success:
                    print(f"✅ Test {i}: {test['name']}")
                    if has_coord:
                        print(f"   Coordinate: ({location.coordinate.latitude:.4f}, {location.coordinate.longitude:.4f})")
                        print(f"   DMS: {location.coordinate.to_dms()}")
                    if has_addr:
                        print(f"   Address: {location.address.standardized}")
                    print(f"   Type: {location.location_type}, Visits: {location.visit_count}")
                    passed += 1
                else:
                    print(f"❌ Test {i}: {test['name']} - Unexpected result")
                    failed += 1
            else:
                print(f"❌ Test {i}: {test['name']} - No location created")
                failed += 1
        except Exception as e:
            print(f"❌ Test {i}: {test['name']} - Error: {e}")
            failed += 1
        print()
    
    # Test distance calculation
    print("Testing distance calculation...")
    loc1 = enricher.normalize_coordinate((40.7128, -74.0060))  # NYC
    loc2 = enricher.normalize_coordinate((34.0522, -118.2437))  # LA
    if loc1 and loc2:
        distance_km = enricher.calculate_distance(loc1, loc2) / 1000
        print(f"✅ NYC to LA distance: {distance_km:.0f} km (expected ~3944 km)")
        if 3900 <= distance_km <= 4000:
            passed += 1
        else:
            failed += 1
    print()
    
    # Test clustering
    print("Testing location clustering...")
    # Add some nearby locations
    enricher.create_location(coordinate=(37.7749, -122.4194), name='Office')
    enricher.create_location(coordinate=(37.7750, -122.4195), name='Coffee Shop')  # ~15m away
    enricher.create_location(coordinate=(37.7800, -122.4200), name='Park')  # ~600m away
    
    clusters = enricher.cluster_locations(radius_meters=50)
    print(f"✅ Created {len(clusters)} clusters from {len(enricher.locations)} locations")
    for cluster in clusters:
        print(f"   Cluster {cluster.cluster_id}: {len(cluster.locations)} locations, {cluster.visit_count} visits")
    passed += 1
    print()
    
    # Statistics
    stats = enricher.get_statistics()
    print("Statistics:")
    print(f"  Total locations: {stats['total_locations']}")
    print(f"  With coordinates: {stats['with_coordinates']}")
    print(f"  With addresses: {stats['with_addresses']}")
    print(f"  Total clusters: {stats['total_clusters']}")
    print(f"  Total visits: {stats['total_visits']}")
    print()
    
    print("=" * 70)
    print(f"✅ Tests Passed: {passed}")
    print(f"❌ Tests Failed: {failed}")
    print(f"📊 Success Rate: {passed}/{passed+failed} ({100*passed/(passed+failed):.0f}%)")
    print("=" * 70)
    
    return passed, failed


if __name__ == '__main__':
    _test_location_enricher()
