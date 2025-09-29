// UFDR Analysis Tool - Neo4j Schema
// Knowledge Graph Schema for Forensic Data
// This schema defines nodes and relationships for UFDR data

// ========================================
// CONSTRAINTS & INDEXES
// ========================================

// Unique constraints ensure data integrity
CREATE CONSTRAINT unique_person_phone IF NOT EXISTS 
FOR (p:Person) REQUIRE p.phone IS UNIQUE;

CREATE CONSTRAINT unique_message_id IF NOT EXISTS 
FOR (m:Message) REQUIRE m.id IS UNIQUE;

CREATE CONSTRAINT unique_call_id IF NOT EXISTS 
FOR (c:Call) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT unique_device_imei IF NOT EXISTS 
FOR (d:Device) REQUIRE d.imei IS UNIQUE;

CREATE CONSTRAINT unique_case_id IF NOT EXISTS 
FOR (c:Case) REQUIRE c.case_id IS UNIQUE;

CREATE CONSTRAINT unique_crypto_address IF NOT EXISTS 
FOR (ca:CryptoAddress) REQUIRE ca.address IS UNIQUE;

// Indexes for performance
CREATE INDEX person_name_index IF NOT EXISTS 
FOR (p:Person) ON (p.name);

CREATE INDEX message_timestamp_index IF NOT EXISTS 
FOR (m:Message) ON (m.timestamp);

CREATE INDEX call_timestamp_index IF NOT EXISTS 
FOR (c:Call) ON (c.timestamp);

CREATE INDEX location_coords_index IF NOT EXISTS 
FOR (l:Location) ON (l.latitude, l.longitude);

CREATE INDEX flag_severity_index IF NOT EXISTS 
FOR (f:Flag) ON (f.severity);

// ========================================
// NODE TYPES
// ========================================

// Person Node - Represents individuals
// Properties:
// - phone: String (primary identifier, E.164 format)
// - name: String (optional)
// - email: String (optional)
// - risk_score: Float (0-1, calculated)
// - is_foreign: Boolean
// - country_code: String

// Message Node - Represents messages/chats
// Properties:
// - id: String (unique identifier)
// - text: String (message content)
// - timestamp: DateTime
// - application: String (WhatsApp, Telegram, SMS, etc.)
// - thread_id: String
// - has_media: Boolean
// - language: String

// Call Node - Represents phone calls
// Properties:
// - id: String (unique identifier)
// - timestamp: DateTime
// - duration: Integer (seconds)
// - call_type: String (incoming/outgoing/missed)

// Device Node - Represents devices
// Properties:
// - imei: String (unique identifier)
// - model: String
// - manufacturer: String
// - os: String
// - os_version: String
// - phone_number: String

// Location Node - Represents geographic locations
// Properties:
// - id: String
// - latitude: Float
// - longitude: Float
// - address: String
// - timestamp: DateTime
// - accuracy: Float

// CryptoAddress Node - Represents cryptocurrency addresses
// Properties:
// - address: String (unique)
// - currency: String (Bitcoin, Ethereum, etc.)
// - first_seen: DateTime
// - last_seen: DateTime
// - transaction_count: Integer

// Media Node - Represents media files
// Properties:
// - id: String
// - filename: String
// - mime_type: String
// - size: Integer
// - hash: String (SHA256)
// - ocr_text: String (extracted text)
// - has_faces: Boolean

// Application Node - Represents apps
// Properties:
// - name: String (WhatsApp, Telegram, etc.)
// - package_name: String
// - version: String

// Flag Node - Represents alerts/suspicious findings
// Properties:
// - id: String
// - type: String (crypto, foreign_contact, pattern, etc.)
// - severity: String (high, medium, low)
// - description: String
// - created_at: DateTime
// - reviewed: Boolean
// - investigator_notes: String

// Case Node - Represents investigation cases
// Properties:
// - case_id: String (unique)
// - created_at: DateTime
// - operator: String
// - status: String (active, closed, archived)
// - sha256_hash: String
// - file_size: Integer

// ========================================
// RELATIONSHIP TYPES
// ========================================

// Communication Relationships
// (Person)-[:SENT]->(Message)
// (Person)-[:RECEIVED]->(Message)
// (Person)-[:CALLED]->(Person) // with Call node as relationship property
// (Message)-[:IN_THREAD]->(Thread)
// (Message)-[:HAS_ATTACHMENT]->(Media)

// Device Relationships
// (Device)-[:BELONGS_TO]->(Person)
// (Device)-[:INSTALLED]->(Application)
// (Message)-[:SENT_FROM]->(Device)
// (Call)-[:MADE_FROM]->(Device)

// Location Relationships
// (Person)-[:WAS_AT]->(Location)
// (Device)-[:LOCATED_AT]->(Location)
// (Message)-[:SENT_FROM_LOCATION]->(Location)

// Contact Relationships
// (Person)-[:KNOWS]->(Person)
// (Person)-[:FREQUENTLY_CONTACTS]->(Person) // with frequency property

// Crypto Relationships
// (Message)-[:MENTIONS]->(CryptoAddress)
// (Person)-[:OWNS]->(CryptoAddress)
// (CryptoAddress)-[:TRANSACTED_WITH]->(CryptoAddress)

// Investigation Relationships
// (Case)-[:CONTAINS]->(Person)
// (Case)-[:CONTAINS]->(Message)
// (Case)-[:CONTAINS]->(Call)
// (Flag)-[:RELATES_TO]->(Message|Call|Person)
// (Flag)-[:IN_CASE]->(Case)

// Analysis Relationships
// (Person)-[:SIMILAR_TO]->(Person) // with similarity score
// (Message)-[:SIMILAR_TO]->(Message) // for duplicate detection

// ========================================
// SAMPLE QUERIES
// ========================================

// Find all contacts of a person
// MATCH (p:Person {phone: "+919876543210"})-[:KNOWS]-(contact:Person)
// RETURN p, contact

// Find messages with crypto addresses
// MATCH (m:Message)-[:MENTIONS]->(ca:CryptoAddress)
// RETURN m, ca ORDER BY m.timestamp DESC

// Find communication patterns between two people
// MATCH path = (p1:Person {phone: $phone1})-[*1..3]-(p2:Person {phone: $phone2})
// RETURN path

// Find foreign contacts
// MATCH (p:Person {is_foreign: true})-[r:SENT|RECEIVED|CALLED]-(indian:Person)
// WHERE indian.country_code = "IN"
// RETURN p, r, indian

// Find suspicious patterns (late night + crypto)
// MATCH (m:Message)-[:MENTIONS]->(ca:CryptoAddress)
// WHERE time(m.timestamp) >= time("00:00") AND time(m.timestamp) <= time("06:00")
// RETURN m, ca

// Find communication clusters
// MATCH (p:Person)-[:KNOWS*1..2]-(connected:Person)
// WITH p, collect(connected) as cluster
// WHERE size(cluster) > 5
// RETURN p, cluster

// Timeline of events for a person
// MATCH (p:Person {phone: $phone})-[r]-(event)
// WHERE event:Message OR event:Call OR event:Location
// RETURN event ORDER BY event.timestamp

// Find deleted messages (gaps in thread)
// MATCH (m1:Message)-[:IN_THREAD]->(t:Thread)<-[:IN_THREAD]-(m2:Message)
// WHERE m2.timestamp > m1.timestamp + duration({hours: 1})
// RETURN m1, m2, t

// ========================================
// INITIALIZATION DATA
// ========================================

// Create root case node for system
MERGE (system:Case {case_id: "SYSTEM", created_at: datetime(), status: "active"});

// Create common applications
MERGE (whatsapp:Application {name: "WhatsApp", package_name: "com.whatsapp"});
MERGE (telegram:Application {name: "Telegram", package_name: "org.telegram.messenger"});
MERGE (signal:Application {name: "Signal", package_name: "org.thoughtcrime.securesms"});
MERGE (sms:Application {name: "SMS", package_name: "com.android.mms"});