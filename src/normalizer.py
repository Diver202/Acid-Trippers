"""
Field Normalizer Module
Handles field name normalization and canonical mapping.

Key responsibilities:
- Convert field name variations to canonical forms (ip, IP, IpAddress -> ip_address)
- Maintain consistent casing (snake_case)
- Build mapping table for tracking variations
- Preserve original names for audit trail
"""

import re
from typing import Dict, Set, List, Tuple
from collections import defaultdict


class FieldNormalizer:
    """
    Normalizes field names to canonical forms to handle variations
    like: username/userName/Username -> username
    """
    
    def __init__(self):
        # Canonical mappings - these are learned over time
        self.canonical_map: Dict[str, str] = {}
        
        # Track all variations seen for each canonical form
        self.variations: Dict[str, Set[str]] = defaultdict(set)
        
        # Predefined common patterns (can be extended)
        self.known_patterns = {
            'username': ['username', 'user_name', 'userName', 'Username', 'UserName'],
            'timestamp': ['timestamp', 't_stamp', 'time_stamp', 'timeStamp', 'Timestamp'],
            'ip_address': ['ip', 'IP', 'IpAddress', 'ip_address', 'ipAddress', 'Ip'],
            'email': ['email', 'Email', 'email_address', 'emailAddress', 'e_mail'],
            'age': ['age', 'Age', 'user_age', 'userAge'],
            'country': ['country', 'Country', 'location_country'],
            'status': ['status', 'Status', 'user_status', 'userStatus'],
        }
        
        # Initialize canonical map with known patterns
        for canonical, variations in self.known_patterns.items():
            for variant in variations:
                self.canonical_map[variant.lower()] = canonical
                self.variations[canonical].add(variant)
    
    def _to_snake_case(self, name: str) -> str:
        """
        Convert camelCase or PascalCase to snake_case
        Examples:
            userName -> user_name
            IpAddress -> ip_address
            HTTPSConnection -> https_connection
        """
        # Insert underscore before uppercase letters (except at start)
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        # Insert underscore before uppercase in sequences
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()
    
    def _find_canonical_match(self, field_name: str) -> str:
        """
        Find the canonical form for a field name
        Strategy:
        1. Check direct lookup in canonical_map
        2. Check snake_case version
        3. Use similarity matching for new fields
        4. If no match, use snake_case version as canonical
        """
        # Direct match
        lower_name = field_name.lower()
        if lower_name in self.canonical_map:
            return self.canonical_map[lower_name]
        
        # Try snake_case conversion
        snake_name = self._to_snake_case(field_name)
        if snake_name in self.canonical_map.values():
            # Found a canonical form that matches
            self.canonical_map[lower_name] = snake_name
            self.variations[snake_name].add(field_name)
            return snake_name
        
        # Check if snake_case matches any existing canonical (fuzzy)
        for canonical in self.canonical_map.values():
            if self._is_similar(snake_name, canonical):
                self.canonical_map[lower_name] = canonical
                self.variations[canonical].add(field_name)
                return canonical
        
        # New field - use snake_case as canonical
        canonical = snake_name
        self.canonical_map[lower_name] = canonical
        self.variations[canonical].add(field_name)
        return canonical
    
    def _is_similar(self, name1: str, name2: str) -> bool:
        """
        Check if two field names are similar enough to be the same field
        Uses simple heuristics:
        - Remove underscores and compare
        - Check if one is substring of other
        """
        clean1 = name1.replace('_', '')
        clean2 = name2.replace('_', '')
        
        # Exact match after cleaning
        if clean1 == clean2:
            return True
        
        # One is substring of other (handles user vs username)
        # But only if length difference is small
        if len(clean1) > 3 and len(clean2) > 3:
            if clean1 in clean2 or clean2 in clean1:
                if abs(len(clean1) - len(clean2)) <= 3:
                    return True
        
        return False
    
    def normalize_field_name(self, field_name: str) -> str:
        """
        Normalize a single field name to its canonical form
        
        Args:
            field_name: Original field name from JSON
            
        Returns:
            Canonical field name
        """
        return self._find_canonical_match(field_name)
    
    def normalize_record(self, record: Dict) -> Tuple[Dict, Dict]:
        """
        Normalize all field names in a record
        
        Args:
            record: Original JSON record with messy field names
            
        Returns:
            Tuple of (normalized_record, mapping_used)
            mapping_used shows original -> canonical mappings for audit
        """
        normalized = {}
        mapping = {}
        
        for original_name, value in record.items():
            canonical_name = self.normalize_field_name(original_name)
            normalized[canonical_name] = value
            mapping[original_name] = canonical_name
        
        return normalized, mapping
    
    def get_variations(self, canonical_name: str) -> Set[str]:
        """Get all variations seen for a canonical field name"""
        return self.variations.get(canonical_name, set())
    
    def get_all_canonical_fields(self) -> List[str]:
        """Get list of all canonical field names discovered"""
        return list(set(self.canonical_map.values()))
    
    def get_statistics(self) -> Dict:
        """Get normalization statistics"""
        return {
            'total_variations': len(self.canonical_map),
            'canonical_fields': len(set(self.canonical_map.values())),
            'variation_details': {
                canonical: list(variations)
                for canonical, variations in self.variations.items()
            }
        }
    
    def export_mappings(self) -> Dict:
        """Export current mappings for persistence"""
        return {
            'canonical_map': self.canonical_map,
            'variations': {k: list(v) for k, v in self.variations.items()}
        }
    
    def import_mappings(self, mappings: Dict):
        """Import previously saved mappings"""
        self.canonical_map = mappings.get('canonical_map', {})
        self.variations = defaultdict(set, {
            k: set(v) for k, v in mappings.get('variations', {}).items()
        })


if __name__ == "__main__":
    # Test the normalizer
    normalizer = FieldNormalizer()
    
    # Test records with field variations
    test_records = [
        {'username': 'user1', 'IP': '1.2.3.4', 'Age': 25},
        {'userName': 'user2', 'ip_address': '5.6.7.8', 'age': 30},
        {'Username': 'user3', 'ip': '9.10.11.12', 'user_age': 35},
        {'user_name': 'user4', 'IpAddress': '13.14.15.16', 'Age': 40},
    ]
    
    print("Field Normalization Test")
    print("=" * 80)
    
    for i, record in enumerate(test_records, 1):
        print(f"\nOriginal Record {i}:")
        print(f"  {record}")
        
        normalized, mapping = normalizer.normalize_record(record)
        print(f"Normalized Record {i}:")
        print(f"  {normalized}")
        print(f"Mapping Used:")
        for orig, canon in mapping.items():
            print(f"    {orig} -> {canon}")
    
    print("\n" + "=" * 80)
    print("\nNormalization Statistics:")
    stats = normalizer.get_statistics()
    print(f"Total variations seen: {stats['total_variations']}")
    print(f"Canonical fields: {stats['canonical_fields']}")
    print("\nVariation Details:")
    for canonical, variations in stats['variation_details'].items():
        print(f"  {canonical}: {variations}")