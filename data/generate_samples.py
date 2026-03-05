"""
Generate Synthetic UFDR Samples
Creates realistic test data for development and testing
"""

import json
import xml.etree.ElementTree as ET
import random
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker(['en_IN', 'hi_IN'])

# Crypto addresses for testing
CRYPTO_ADDRESSES = [
    "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",  # Bitcoin
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",  # Bitcoin Segwit
    "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb8",  # Ethereum
    "LVg2kJoFNg45Nbpy53h7Fe6r17e6PQnb5G"  # Litecoin
]

# Foreign numbers
FOREIGN_NUMBERS = [
    "+1-555-0123",  # USA
    "+44-20-7123-4567",  # UK  
    "+86-138-0013-8000",  # China
    "+971-50-123-4567",  # UAE
    "+7-495-123-4567"  # Russia
]

class UFDRSampleGenerator:
    def __init__(self, output_dir: str = "data/samples"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_message(self, msg_id: int, include_crypto: bool = False, include_foreign: bool = False):
        """Generate a message artifact"""
        message = {
            "id": f"msg_{msg_id:06d}",
            "from": fake.phone_number() if not include_foreign else random.choice(FOREIGN_NUMBERS),
            "to": "+91" + "".join([str(random.randint(0,9)) for _ in range(10)]),
            "timestamp": (datetime.now() - timedelta(days=random.randint(0,365))).isoformat(),
            "app": random.choice(["WhatsApp", "Telegram", "SMS", "Signal"]),
            "thread": f"thread_{random.randint(1,100)}",
        }
        
        # Generate message text
        if include_crypto:
            message["text"] = f"Send payment to {random.choice(CRYPTO_ADDRESSES)} urgently"
        else:
            message["text"] = fake.text(max_nb_chars=200)
            
        return message
    
    def generate_call(self, call_id: int, include_foreign: bool = False):
        """Generate a call record"""
        call = {
            "id": f"call_{call_id:06d}",
            "from": fake.phone_number() if not include_foreign else random.choice(FOREIGN_NUMBERS),
            "to": "+91" + "".join([str(random.randint(0,9)) for _ in range(10)]),
            "timestamp": (datetime.now() - timedelta(days=random.randint(0,365))).isoformat(),
            "duration": random.randint(10, 3600),
            "type": random.choice(["incoming", "outgoing", "missed"])
        }
        return call
    
    def generate_contact(self, contact_id: int):
        """Generate a contact entry"""
        contact = {
            "id": f"contact_{contact_id:06d}",
            "name": fake.name(),
            "phone": [fake.phone_number() for _ in range(random.randint(1,3))],
            "email": [fake.email() for _ in range(random.randint(0,2))]
        }
        return contact
    
    def generate_location(self, loc_id: int):
        """Generate location data"""
        # Some known locations in India
        locations = [
            {"lat": 28.5355, "lon": 77.3910, "address": "Noida, Uttar Pradesh"},
            {"lat": 28.6139, "lon": 77.2090, "address": "Connaught Place, New Delhi"},
            {"lat": 19.0760, "lon": 72.8777, "address": "Mumbai, Maharashtra"},
            {"lat": 12.9716, "lon": 77.5946, "address": "Bangalore, Karnataka"},
        ]
        
        loc = random.choice(locations)
        return {
            "id": f"loc_{loc_id:06d}",
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "address": loc["address"],
            "timestamp": (datetime.now() - timedelta(days=random.randint(0,365))).isoformat(),
            "accuracy": random.uniform(5, 50)
        }
    
    def create_ufdr_xml(self, case_name: str, num_messages: int = 100, num_calls: int = 50):
        """Create UFDR XML with synthetic data"""
        root = ET.Element("UFDRReport")
        root.set("version", "1.0")
        root.set("generated", datetime.now().isoformat())
        
        # Add metadata
        metadata = ET.SubElement(root, "Metadata")
        ET.SubElement(metadata, "CaseID").text = case_name
        ET.SubElement(metadata, "DeviceModel").text = "OnePlus 9 Pro"
        ET.SubElement(metadata, "IMEI").text = "".join([str(random.randint(0,9)) for _ in range(15)])
        ET.SubElement(metadata, "ExtractionTool").text = "UFDR Extractor v2.0"
        
        # Add messages
        messages = ET.SubElement(root, "Messages")
        for i in range(num_messages):
            # 10% chance of crypto, 15% chance of foreign number
            include_crypto = random.random() < 0.1
            include_foreign = random.random() < 0.15
            
            msg_data = self.generate_message(i, include_crypto, include_foreign)
            msg = ET.SubElement(messages, "Message")
            for key, value in msg_data.items():
                ET.SubElement(msg, key).text = str(value)
        
        # Add calls
        calls = ET.SubElement(root, "Calls")
        for i in range(num_calls):
            include_foreign = random.random() < 0.15
            call_data = self.generate_call(i, include_foreign)
            call = ET.SubElement(calls, "Call")
            for key, value in call_data.items():
                ET.SubElement(call, key).text = str(value)
        
        # Add contacts
        contacts = ET.SubElement(root, "Contacts")
        for i in range(50):
            contact_data = self.generate_contact(i)
            contact = ET.SubElement(contacts, "Contact")
            ET.SubElement(contact, "id").text = contact_data["id"]
            ET.SubElement(contact, "name").text = contact_data["name"]
            for phone in contact_data["phone"]:
                ET.SubElement(contact, "phone").text = phone
            for email in contact_data["email"]:
                ET.SubElement(contact, "email").text = email
        
        # Add locations
        locations = ET.SubElement(root, "Locations")
        for i in range(30):
            loc_data = self.generate_location(i)
            location = ET.SubElement(locations, "Location")
            for key, value in loc_data.items():
                ET.SubElement(location, key).text = str(value)
        
        return ET.tostring(root, encoding='unicode')
    
    def create_sample_ufdr(self, case_name: str):
        """Create a complete UFDR zip file"""
        # Generate XML
        xml_content = self.create_ufdr_xml(case_name)
        
        # Create zip file
        zip_path = self.output_dir / f"{case_name}.ufdr"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add main report
            zf.writestr("report.xml", xml_content)
            
            # Add some dummy media references
            media_list = {
                "Media/Images/IMG_001.jpg": "dummy_image_reference",
                "Media/Audio/REC_001.m4a": "dummy_audio_reference",
                "Media/Documents/DOC_001.pdf": "dummy_document_reference"
            }
            
            for path, content in media_list.items():
                zf.writestr(path, content)
            
            # Add extraction info
            info = {
                "extraction_date": datetime.now().isoformat(),
                "device_info": {
                    "model": "OnePlus 9 Pro",
                    "os": "Android 12",
                    "storage": "128GB"
                }
            }
            zf.writestr("extraction_info.json", json.dumps(info, indent=2))
        
        print(f"✓ Created sample UFDR: {zip_path}")
        print(f"  Size: {zip_path.stat().st_size:,} bytes")
        
        return zip_path


def main():
    generator = UFDRSampleGenerator()
    
    # Generate 3 sample UFDR files with different characteristics
    samples = [
        ("case_crypto_investigation", 150, 75),  # More messages for crypto case
        ("case_foreign_contacts", 100, 100),     # Balanced case
        ("case_local_fraud", 200, 50)            # Message-heavy case
    ]
    
    for case_name, num_msgs, num_calls in samples:
        generator.create_sample_ufdr(case_name)
        generator.create_ufdr_xml(case_name, num_msgs, num_calls)
    
    print("\n✓ Sample generation complete!")
    print("  Generated 3 synthetic UFDR files for testing")


if __name__ == "__main__":
    main()