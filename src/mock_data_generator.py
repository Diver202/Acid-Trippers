"""
Mock Data Generator for Adaptive Ingestion System
Simulates messy JSON data with:
- Field name variations (ip vs IP vs IpAddress)
- Type drifting (int -> string)
- Nested structures
- Client timestamps (t_stamp)
"""

import random
import time
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List
from pathlib import Path


class MockDataGenerator:
    """Generates realistic messy JSON records for testing"""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.record_count = 0
        
        # Simulate field name variations
        self.field_variations = {
            'ip': ['ip', 'IP', 'IpAddress', 'ip_address'],
            'username': ['username', 'userName', 'user_name', 'Username'],
            'age': ['age', 'Age', 'user_age'],
            'email': ['email', 'Email', 'email_address'],
            'timestamp': ['timestamp', 't_stamp', 'time_stamp'],
            'country': ['country', 'Country', 'location_country'],
            'status': ['status', 'Status', 'user_status']
        }
        
        # Sample data pools
        self.usernames = [f"user_{i}" for i in range(1, 51)]
        self.countries = ["USA", "UK", "India", "Canada", "Germany", "France", "Japan", "Australia"]
        self.statuses = ["active", "inactive", "pending", "suspended"]
        self.emails = [f"{name}@example.com" for name in self.usernames[:20]]
        
    def _get_random_field_name(self, canonical_name: str) -> str:
        """Return a random variation of a field name"""
        variations = self.field_variations.get(canonical_name, [canonical_name])
        return random.choice(variations)
    
    def _generate_ip(self) -> str:
        """Generate random IP address"""
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
    
    def _generate_timestamp(self) -> str:
        """Generate timestamp in the past 30 days"""
        days_ago = random.randint(0, 30)
        dt = datetime.now() - timedelta(days=days_ago, 
                                        hours=random.randint(0, 23),
                                        minutes=random.randint(0, 59))
        return dt.isoformat()
    
    def generate_record(self, record_type: str = "random") -> Dict[str, Any]:
        """Generate a single JSON record with intentional messiness"""
        self.record_count += 1
        
        if record_type == "random":
            record_type = random.choice(["clean", "messy", "nested"])
        
        # Base record - username and timestamp are mandatory
        record = {
            self._get_random_field_name('username'): random.choice(self.usernames),
            self._get_random_field_name('timestamp'): self._generate_timestamp()
        }
        
        # Add common fields with varying presence
        if random.random() > 0.1:  # 90% presence
            ip_field = self._get_random_field_name('ip')
            if random.random() > 0.95:  # 5% type drift (malformed IP)
                record[ip_field] = f"{random.randint(1, 255)}.{random.randint(0, 255)}"
            else:
                record[ip_field] = self._generate_ip()
        
        if random.random() > 0.2:  # 80% presence
            age_field = self._get_random_field_name('age')
            age_value = random.randint(18, 75)
            # Type drift: 15% chance age comes as a string
            record[age_field] = str(age_value) if random.random() > 0.85 else age_value
        
        if random.random() > 0.3:  # 70% presence
            record[self._get_random_field_name('email')] = random.choice(self.emails)
        
        if random.random() > 0.25:  # 75% presence
            record[self._get_random_field_name('country')] = random.choice(self.countries)
        
        if random.random() > 0.4:  # 60% presence
            record[self._get_random_field_name('status')] = random.choice(self.statuses)
        
        # Primary key simulation
        if random.random() > 0.05:
            record['session_id'] = f"sess_{self.record_count}_{random.randint(1000, 9999)}"
        
        # Nested structures (MongoDB-bound data)
        if record_type == "nested" or (record_type == "random" and random.random() > 0.6):
            record['metadata'] = {
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'os': random.choice(['Windows', 'macOS', 'Linux', 'iOS', 'Android']),
                'device': {
                    'type': random.choice(['mobile', 'desktop', 'tablet']),
                    'screen_size': f"{random.choice([1920, 1366, 1024, 768])}x{random.choice([1080, 768, 768, 1024])}"
                }
            }
        
        # Arrays and sparse fields
        if random.random() > 0.8:
            record['tags'] = random.sample(['premium', 'verified', 'new', 'vip', 'beta_tester'], 
                                          k=random.randint(1, 3))
        
        return record
    
    def generate_stream(self, num_records: int = 100) -> List[Dict[str, Any]]:
        """Generate a list of records"""
        return [self.generate_record() for _ in range(num_records)]
    
    def save_to_file(self, num_records: int = 100, filename: str = None):
        """Save generated records to a JSON file, creating directories if needed"""
        if filename is None:
            filename = f"data/mock_stream_{int(time.time())}.json"
        
        output_path = Path(filename)
        
        # Ensure the directory exists to avoid FileNotFoundError
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        records = self.generate_stream(num_records)
        
        with output_path.open('w') as f:
            json.dump(records, f, indent=2)
        
        print(f"Successfully generated {num_records} records.")
        print(f"Saved to: {output_path.resolve()}")
        return str(output_path)


if __name__ == "__main__":
    generator = MockDataGenerator()
    
    print("Previewing 3 messy records:")
    print("-" * 30)
    for i in range(3):
        print(json.dumps(generator.generate_record(), indent=2))
    
    # Save a larger dataset (using a relative path for better compatibility)
    target_file = "../adaptive-ingestion/data/sample_stream.json"
    generator.save_to_file(num_records=500, filename=target_file)