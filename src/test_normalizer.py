"""
Test normalizer with mock data generator
"""

import json
import sys
import os
from pathlib import Path

# Dynamically find the 'src' directory relative to this script
# This replaces the hardcoded '/adaptive-ingestion/src' which causes issues across environments
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parents[1]  # Goes up from 'generator' to 'src' to 'adaptive-ingestion'
sys.path.append(str(project_root / "src"))

try:
    from normalizer import FieldNormalizer
    from mock_data_generator import MockDataGenerator
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

# Generate some test data
generator = MockDataGenerator(seed=42)
records = generator.generate_stream(num_records=10)

# Initialize normalizer
normalizer = FieldNormalizer()

print("Testing Normalizer with Mock Data")
print("=" * 100)

# Normalize all records
normalized_records = []
for i, record in enumerate(records, 1):
    normalized, mapping = normalizer.normalize_record(record)
    normalized_records.append(normalized)
    
    if i <= 3:  # Show first 3 in detail
        print(f"\n--- Record {i} ---")
        print(f"Original keys: {list(record.keys())}")
        print(f"Normalized keys: {list(normalized.keys())}")
        print(f"Mappings:")
        for orig, canon in mapping.items():
            if orig.lower() != canon:  # Only show if there was a change
                print(f"  {orig:20s} -> {canon}")

# Show statistics
print("\n" + "=" * 100)
print("\nNormalization Statistics:")
stats = normalizer.get_statistics()
print(f"Total field variations encountered: {stats['total_variations']}")
print(f"Unique canonical fields: {stats['canonical_fields']}")

print("\nCanonical Fields and Their Variations:")
for canonical, variations in sorted(stats['variation_details'].items()):
    if len(variations) > 1:  # Only show fields with variations
        print(f"\n  {canonical}:")
        for var in sorted(variations):
            print(f"    - {var}")

# --- FIXED FILE SAVING LOGIC ---
# Use Path to handle directory creation
output_path = Path('../adaptive-ingestion/data/normalized_sample.json')

# Create the 'data' directory if it doesn't exist
output_path.parent.mkdir(parents=True, exist_ok=True)

with output_path.open('w') as f:
    json.dump(normalized_records, f, indent=2)

print(f"\nNormalized records saved to: {output_path.resolve()}")