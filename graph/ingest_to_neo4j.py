"""
Neo4j Ingestion Module
Populates knowledge graph from parsed UFDR data
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class Neo4jIngestor:
    """Ingests parsed UFDR data into Neo4j knowledge graph"""
    
    def __init__(self, 
                 uri: str = None,
                 user: str = None,
                 password: str = None):
        """
        Initialize Neo4j connection
        
        Args:
            uri: Neo4j URI (default from env)
            user: Neo4j username (default from env)
            password: Neo4j password (default from env)
        """
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password123")
        
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self._verify_connection()
            logger.info(f"Connected to Neo4j at {self.uri}")
        except ServiceUnavailable:
            logger.error(f"Could not connect to Neo4j at {self.uri}")
            self.driver = None
    
    def _verify_connection(self):
        """Verify Neo4j connection"""
        with self.driver.session() as session:
            session.run("RETURN 1")
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
    
    def initialize_schema(self):
        """Run schema initialization from schema.cypher"""
        schema_file = Path(__file__).parent / "schema.cypher"
        
        if not schema_file.exists():
            logger.error("Schema file not found")
            return False
        
        with open(schema_file, 'r') as f:
            schema_content = f.read()
        
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in schema_content.split(';') if s.strip() and not s.strip().startswith('//')]
        
        with self.driver.session() as session:
            for statement in statements:
                if statement:
                    try:
                        session.run(statement)
                        logger.debug(f"Executed: {statement[:50]}...")
                    except Exception as e:
                        logger.error(f"Failed to execute statement: {e}")
        
        logger.info("Schema initialized successfully")
        return True
    
    def ingest_case(self, case_id: str, parsed_dir: str = "data/parsed"):
        """
        Ingest all data for a case into Neo4j
        
        Args:
            case_id: Case identifier
            parsed_dir: Directory with parsed JSON files
        """
        if not self.driver:
            logger.error("Neo4j connection not available")
            return False
        
        case_path = Path(parsed_dir) / case_id
        
        if not case_path.exists():
            logger.error(f"Case directory not found: {case_path}")
            return False
        
        logger.info(f"Ingesting case {case_id} into Neo4j")
        
        # Create case node
        self._create_case_node(case_id)
        
        # Ingest different data types
        self._ingest_messages(case_id, case_path)
        self._ingest_calls(case_id, case_path)
        self._ingest_contacts(case_id, case_path)
        self._ingest_locations(case_id, case_path)
        self._ingest_devices(case_id, case_path)
        
        # Create relationships
        self._create_relationships(case_id)
        
        # Detect patterns
        self._detect_crypto_addresses(case_id)
        self._detect_foreign_contacts(case_id)
        
        logger.info(f"Case {case_id} ingestion complete")
        return True
    
    def _create_case_node(self, case_id: str):
        """Create case node"""
        query = """
        MERGE (c:Case {case_id: $case_id})
        SET c.created_at = datetime(),
            c.status = 'active'
        RETURN c
        """
        
        with self.driver.session() as session:
            session.run(query, case_id=case_id)
    
    def _ingest_messages(self, case_id: str, case_path: Path):
        """Ingest messages into graph"""
        message_files = list(case_path.glob("messages_*.json"))
        
        for msg_file in message_files:
            with open(msg_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            with self.driver.session() as session:
                for msg in messages:
                    # Create message node
                    msg_query = """
                    MERGE (m:Message {id: $id})
                    SET m.text = $text,
                        m.timestamp = datetime($timestamp),
                        m.application = $app,
                        m.thread_id = $thread
                    WITH m
                    MATCH (c:Case {case_id: $case_id})
                    MERGE (c)-[:CONTAINS]->(m)
                    RETURN m
                    """
                    
                    session.run(msg_query,
                               id=msg.get('id'),
                               text=msg.get('text'),
                               timestamp=msg.get('timestamp'),
                               app=msg.get('application'),
                               thread=msg.get('thread_id'),
                               case_id=case_id)
                    
                    # Create/link sender
                    if msg.get('sender'):
                        sender_query = """
                        MERGE (p:Person {phone: $phone})
                        SET p.is_foreign = $is_foreign,
                            p.country_code = $country_code
                        WITH p
                        MATCH (m:Message {id: $msg_id})
                        MERGE (p)-[:SENT]->(m)
                        MATCH (c:Case {case_id: $case_id})
                        MERGE (c)-[:CONTAINS]->(p)
                        """
                        
                        is_foreign = not msg['sender'].startswith('+91')
                        country_code = self._extract_country_code(msg['sender'])
                        
                        session.run(sender_query,
                                   phone=msg['sender'],
                                   is_foreign=is_foreign,
                                   country_code=country_code,
                                   msg_id=msg['id'],
                                   case_id=case_id)
                    
                    # Create/link recipient
                    if msg.get('recipient'):
                        recipient_query = """
                        MERGE (p:Person {phone: $phone})
                        SET p.is_foreign = $is_foreign,
                            p.country_code = $country_code
                        WITH p
                        MATCH (m:Message {id: $msg_id})
                        MERGE (p)-[:RECEIVED]->(m)
                        MATCH (c:Case {case_id: $case_id})
                        MERGE (c)-[:CONTAINS]->(p)
                        """
                        
                        is_foreign = not msg['recipient'].startswith('+91')
                        country_code = self._extract_country_code(msg['recipient'])
                        
                        session.run(recipient_query,
                                   phone=msg['recipient'],
                                   is_foreign=is_foreign,
                                   country_code=country_code,
                                   msg_id=msg['id'],
                                   case_id=case_id)
        
        logger.info(f"Ingested messages for case {case_id}")
    
    def _ingest_calls(self, case_id: str, case_path: Path):
        """Ingest call records into graph"""
        call_files = list(case_path.glob("calls_*.json"))
        
        for call_file in call_files:
            with open(call_file, 'r', encoding='utf-8') as f:
                calls = json.load(f)
            
            with self.driver.session() as session:
                for call in calls:
                    # Create call node
                    call_query = """
                    MERGE (c:Call {id: $id})
                    SET c.timestamp = datetime($timestamp),
                        c.duration = $duration,
                        c.call_type = $call_type
                    WITH c
                    MATCH (case:Case {case_id: $case_id})
                    MERGE (case)-[:CONTAINS]->(c)
                    RETURN c
                    """
                    
                    session.run(call_query,
                               id=call.get('id'),
                               timestamp=call.get('timestamp'),
                               duration=call.get('duration'),
                               call_type=call.get('call_type'),
                               case_id=case_id)
                    
                    # Link caller and callee
                    if call.get('caller') and call.get('callee'):
                        call_relationship = """
                        MERGE (caller:Person {phone: $caller})
                        MERGE (callee:Person {phone: $callee})
                        WITH caller, callee
                        MATCH (c:Call {id: $call_id})
                        MERGE (caller)-[:MADE_CALL {call_id: $call_id}]->(callee)
                        MERGE (caller)-[:PARTICIPATED_IN]->(c)
                        MERGE (callee)-[:PARTICIPATED_IN]->(c)
                        """
                        
                        session.run(call_relationship,
                                   caller=call['caller'],
                                   callee=call['callee'],
                                   call_id=call['id'])
        
        logger.info(f"Ingested calls for case {case_id}")
    
    def _ingest_contacts(self, case_id: str, case_path: Path):
        """Ingest contacts into graph"""
        contact_files = list(case_path.glob("contacts_*.json"))
        
        for contact_file in contact_files:
            with open(contact_file, 'r', encoding='utf-8') as f:
                contacts = json.load(f)
            
            with self.driver.session() as session:
                for contact in contacts:
                    # Create person nodes for each phone number
                    for phone in contact.get('phone_numbers', []):
                        person_query = """
                        MERGE (p:Person {phone: $phone})
                        SET p.name = coalesce(p.name, $name)
                        WITH p
                        MATCH (c:Case {case_id: $case_id})
                        MERGE (c)-[:CONTAINS]->(p)
                        """
                        
                        session.run(person_query,
                                   phone=phone,
                                   name=contact.get('name'),
                                   case_id=case_id)
                    
                    # Add email if present
                    for email in contact.get('emails', []):
                        if contact.get('phone_numbers'):
                            email_query = """
                            MATCH (p:Person {phone: $phone})
                            SET p.email = $email
                            """
                            session.run(email_query,
                                       phone=contact['phone_numbers'][0],
                                       email=email)
        
        logger.info(f"Ingested contacts for case {case_id}")
    
    def _ingest_locations(self, case_id: str, case_path: Path):
        """Ingest location data into graph"""
        location_files = list(case_path.glob("locations_*.json"))
        
        for loc_file in location_files:
            with open(loc_file, 'r', encoding='utf-8') as f:
                locations = json.load(f)
            
            with self.driver.session() as session:
                for loc in locations:
                    loc_query = """
                    MERGE (l:Location {id: $id})
                    SET l.latitude = $lat,
                        l.longitude = $lon,
                        l.address = $address,
                        l.timestamp = datetime($timestamp),
                        l.accuracy = $accuracy
                    WITH l
                    MATCH (c:Case {case_id: $case_id})
                    MERGE (c)-[:CONTAINS]->(l)
                    """
                    
                    session.run(loc_query,
                               id=loc.get('id'),
                               lat=loc.get('latitude'),
                               lon=loc.get('longitude'),
                               address=loc.get('address'),
                               timestamp=loc.get('timestamp'),
                               accuracy=loc.get('accuracy'),
                               case_id=case_id)
        
        logger.info(f"Ingested locations for case {case_id}")
    
    def _ingest_devices(self, case_id: str, case_path: Path):
        """Ingest device information into graph"""
        device_files = list(case_path.glob("devices_*.json"))
        
        for dev_file in device_files:
            with open(dev_file, 'r', encoding='utf-8') as f:
                devices = json.load(f)
            
            with self.driver.session() as session:
                for device in devices:
                    if device.get('imei'):
                        dev_query = """
                        MERGE (d:Device {imei: $imei})
                        SET d.model = $model,
                            d.manufacturer = $manufacturer,
                            d.os = $os,
                            d.os_version = $os_version
                        WITH d
                        MATCH (c:Case {case_id: $case_id})
                        MERGE (c)-[:CONTAINS]->(d)
                        """
                        
                        session.run(dev_query,
                                   imei=device.get('imei'),
                                   model=device.get('model'),
                                   manufacturer=device.get('manufacturer'),
                                   os=device.get('os'),
                                   os_version=device.get('os_version'),
                                   case_id=case_id)
        
        logger.info(f"Ingested devices for case {case_id}")
    
    def _create_relationships(self, case_id: str):
        """Create derived relationships"""
        with self.driver.session() as session:
            # Create KNOWS relationships based on communication
            knows_query = """
            MATCH (p1:Person)-[:SENT|RECEIVED]->(:Message)<-[:SENT|RECEIVED]-(p2:Person)
            WHERE p1 <> p2
            MERGE (p1)-[:KNOWS]-(p2)
            """
            session.run(knows_query)
            
            # Mark frequent contacts
            frequent_query = """
            MATCH (p1:Person)-[r:SENT|RECEIVED|MADE_CALL]-(p2:Person)
            WITH p1, p2, count(r) as comm_count
            WHERE comm_count > 10
            MERGE (p1)-[:FREQUENTLY_CONTACTS {count: comm_count}]->(p2)
            """
            session.run(frequent_query)
            
            logger.info("Created relationship patterns")
    
    def _detect_crypto_addresses(self, case_id: str):
        """Detect and flag cryptocurrency addresses"""
        crypto_patterns = [
            r'\b(?:bc1|[13])[a-zA-HJ-NP-Z0-9]{25,62}\b',  # Bitcoin
            r'\b0x[a-fA-F0-9]{40}\b',  # Ethereum
            r'\b[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}\b'  # Litecoin
        ]
        
        with self.driver.session() as session:
            # Get all messages
            result = session.run("""
                MATCH (m:Message)<-[:CONTAINS]-(c:Case {case_id: $case_id})
                WHERE m.text IS NOT NULL
                RETURN m.id as id, m.text as text
            """, case_id=case_id)
            
            for record in result:
                text = record['text']
                msg_id = record['id']
                
                # Check for crypto addresses
                for pattern in crypto_patterns:
                    matches = re.findall(pattern, text)
                    for address in matches:
                        # Create crypto address node and relationship
                        crypto_query = """
                        MERGE (ca:CryptoAddress {address: $address})
                        SET ca.first_seen = coalesce(ca.first_seen, datetime()),
                            ca.last_seen = datetime()
                        WITH ca
                        MATCH (m:Message {id: $msg_id})
                        MERGE (m)-[:MENTIONS]->(ca)
                        WITH m, ca
                        MERGE (f:Flag {
                            id: $flag_id,
                            type: 'crypto_address',
                            severity: 'high',
                            description: $desc
                        })
                        MERGE (f)-[:RELATES_TO]->(m)
                        """
                        
                        flag_id = f"crypto_{msg_id}_{address[:8]}"
                        desc = f"Cryptocurrency address detected: {address}"
                        
                        session.run(crypto_query,
                                   address=address,
                                   msg_id=msg_id,
                                   flag_id=flag_id,
                                   desc=desc)
                        
                        logger.info(f"Detected crypto address: {address}")
    
    def _detect_foreign_contacts(self, case_id: str):
        """Detect and flag foreign contacts"""
        with self.driver.session() as session:
            foreign_query = """
            MATCH (p:Person {is_foreign: true})<-[:CONTAINS]-(c:Case {case_id: $case_id})
            WITH p, c
            MERGE (f:Flag {
                id: $flag_id,
                type: 'foreign_contact',
                severity: 'medium',
                description: $desc
            })
            MERGE (f)-[:RELATES_TO]->(p)
            MERGE (f)-[:IN_CASE]->(c)
            """
            
            # Get foreign contacts
            result = session.run("""
                MATCH (p:Person {is_foreign: true})<-[:CONTAINS]-(c:Case {case_id: $case_id})
                RETURN p.phone as phone, p.country_code as country
            """, case_id=case_id)
            
            for record in result:
                flag_id = f"foreign_{record['phone']}"
                desc = f"Foreign contact detected: {record['phone']} (Country: {record['country']})"
                
                session.run(foreign_query,
                           case_id=case_id,
                           flag_id=flag_id,
                           desc=desc)
                
                logger.info(f"Flagged foreign contact: {record['phone']}")
    
    def _extract_country_code(self, phone: str) -> str:
        """Extract country code from phone number"""
        if phone.startswith('+91'):
            return 'IN'
        elif phone.startswith('+1'):
            return 'US'
        elif phone.startswith('+44'):
            return 'UK'
        elif phone.startswith('+86'):
            return 'CN'
        elif phone.startswith('+971'):
            return 'AE'
        elif phone.startswith('+7'):
            return 'RU'
        else:
            return 'UNKNOWN'
    
    def get_case_statistics(self, case_id: str) -> Dict:
        """Get statistics for a case"""
        if not self.driver:
            return {}
        
        stats = {}
        
        with self.driver.session() as session:
            # Count nodes
            node_counts = session.run("""
                MATCH (c:Case {case_id: $case_id})-[:CONTAINS]->(n)
                RETURN labels(n)[0] as type, count(n) as count
            """, case_id=case_id)
            
            for record in node_counts:
                stats[record['type']] = record['count']
            
            # Count relationships
            rel_counts = session.run("""
                MATCH (c:Case {case_id: $case_id})-[:CONTAINS]->(n)-[r]->()
                RETURN type(r) as rel_type, count(r) as count
            """, case_id=case_id)
            
            stats['relationships'] = {}
            for record in rel_counts:
                stats['relationships'][record['rel_type']] = record['count']
            
            # Count flags
            flag_count = session.run("""
                MATCH (f:Flag)-[:IN_CASE]->(c:Case {case_id: $case_id})
                RETURN f.type as type, count(f) as count
            """, case_id=case_id)
            
            stats['flags'] = {}
            for record in flag_count:
                stats['flags'][record['type']] = record['count']
        
        return stats


def main():
    """CLI interface for Neo4j ingestion"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest UFDR data into Neo4j")
    parser.add_argument("--case-id", required=True, help="Case identifier")
    parser.add_argument("--parsed-dir", default="data/parsed", help="Parsed data directory")
    parser.add_argument("--init-schema", action="store_true", help="Initialize schema")
    
    args = parser.parse_args()
    
    ingestor = Neo4jIngestor()
    
    try:
        if args.init_schema:
            print("Initializing Neo4j schema...")
            ingestor.initialize_schema()
        
        print(f"Ingesting case {args.case_id} into Neo4j...")
        ingestor.ingest_case(args.case_id, args.parsed_dir)
        
        # Get statistics
        stats = ingestor.get_case_statistics(args.case_id)
        
        print("\n✓ Ingestion complete!")
        print("\nGraph Statistics:")
        for key, value in stats.items():
            if key != 'relationships' and key != 'flags':
                print(f"  {key}: {value}")
        
        if 'relationships' in stats:
            print("\nRelationships:")
            for rel, count in stats['relationships'].items():
                print(f"  {rel}: {count}")
        
        if 'flags' in stats:
            print("\nFlags:")
            for flag_type, count in stats['flags'].items():
                print(f"  {flag_type}: {count}")
        
    except Exception as e:
        print(f"✗ Ingestion failed: {str(e)}")
        return 1
    finally:
        ingestor.close()
    
    return 0


if __name__ == "__main__":
    exit(main())