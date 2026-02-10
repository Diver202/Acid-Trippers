"""
Data Analyzer Module
Tracks field patterns, frequencies, type stability, and characteristics
to inform SQL vs MongoDB placement decisions.

Key Metrics:
- Field Frequency: What % of records contain this field?
- Type Stability: What % of values are of the dominant type?
- Cardinality: How many unique values exist?
- Nesting Complexity: Does the field contain nested objects/arrays?
- Value Patterns: Is it an IP, email, URL, etc.?
"""

import re
from typing import Dict, Any, List, Set, Tuple
from collections import defaultdict, Counter
from datetime import datetime
import json


class DataAnalyzer:
    """
    Analyzes incoming JSON records to extract field characteristics
    """
    
    def __init__(self):
        # Track field statistics
        self.total_records = 0
        
        # Field presence tracking
        self.field_counts = defaultdict(int)  # How many times each field appears
        
        # Type tracking for each field
        self.field_types = defaultdict(lambda: defaultdict(int))  # field -> {type: count}
        
        # Value tracking for cardinality analysis
        self.field_values = defaultdict(set)  # field -> set of unique values (limited size)
        self.value_count_limit = 10000  # Don't store more than this many unique values
        
        # Nested structure tracking
        self.nested_fields = set()  # Fields that contain nested dicts
        self.array_fields = set()  # Fields that contain arrays
        
        # Value pattern tracking
        self.pattern_matches = defaultdict(lambda: defaultdict(int))  # field -> {pattern: count}
        
        # Define value patterns to detect
        self.patterns = {
            'ip_address': re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'url': re.compile(r'^https?://'),
            'uuid': re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.I),
            'iso_timestamp': re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),
        }
    
    def _get_type_name(self, value: Any) -> str:
        """Get a normalized type name for a value"""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            return 'array'
        elif isinstance(value, dict):
            return 'object'
        else:
            return type(value).__name__
    
    def _detect_pattern(self, value: Any) -> str:
        """Detect if a value matches known patterns"""
        if not isinstance(value, str):
            return 'none'
        
        for pattern_name, pattern_regex in self.patterns.items():
            if pattern_regex.match(value):
                return pattern_name
        
        return 'none'
    
    def _analyze_value(self, field_name: str, value: Any, path: str = ""):
        """
        Recursively analyze a value and its nested structure
        
        Args:
            field_name: Name of the field
            value: Value to analyze
            path: Path for nested fields (e.g., "metadata.device")
        """
        # Track field presence
        self.field_counts[field_name] += 1
        
        # Track type
        type_name = self._get_type_name(value)
        self.field_types[field_name][type_name] += 1
        
        # Handle nested structures
        if isinstance(value, dict):
            self.nested_fields.add(field_name)
            # Don't recurse into nested fields for now - treat as complex object
            
        elif isinstance(value, list):
            self.array_fields.add(field_name)
            # Don't recurse into arrays - treat as complex array
            
        else:
            # Track unique values for cardinality (with limit)
            if len(self.field_values[field_name]) < self.value_count_limit:
                # Store value as string for comparison
                self.field_values[field_name].add(str(value))
            
            # Detect patterns in string values
            if isinstance(value, str):
                pattern = self._detect_pattern(value)
                if pattern != 'none':
                    self.pattern_matches[field_name][pattern] += 1
    
    def analyze_record(self, record: Dict[str, Any]):
        """
        Analyze a single normalized record
        
        Args:
            record: Normalized JSON record
        """
        self.total_records += 1
        
        for field_name, value in record.items():
            self._analyze_value(field_name, value)
    
    def analyze_batch(self, records: List[Dict[str, Any]]):
        """Analyze a batch of records"""
        for record in records:
            self.analyze_record(record)
    
    def get_field_frequency(self, field_name: str) -> float:
        """
        Get the frequency of a field (0.0 to 1.0)
        
        Returns:
            Percentage of records that contain this field
        """
        if self.total_records == 0:
            return 0.0
        return self.field_counts[field_name] / self.total_records
    
    def get_type_stability(self, field_name: str) -> Tuple[str, float]:
        """
        Get the dominant type and its stability score
        
        Returns:
            Tuple of (dominant_type, stability_score)
            stability_score is the percentage of values that are of the dominant type
        """
        if field_name not in self.field_types:
            return ('unknown', 0.0)
        
        type_counts = self.field_types[field_name]
        if not type_counts:
            return ('unknown', 0.0)
        
        # Find dominant type
        dominant_type = max(type_counts.items(), key=lambda x: x[1])
        total_occurrences = sum(type_counts.values())
        
        stability = dominant_type[1] / total_occurrences if total_occurrences > 0 else 0.0
        
        return (dominant_type[0], stability)
    
    def get_cardinality(self, field_name: str) -> float:
        """
        Get cardinality ratio (unique values / total occurrences)
        High cardinality (close to 1.0) suggests unique/primary key
        Low cardinality suggests enum/category
        
        Returns:
            Cardinality ratio (0.0 to 1.0)
        """
        unique_count = len(self.field_values[field_name])
        total_count = self.field_counts[field_name]
        
        if total_count == 0:
            return 0.0
        
        # If we hit the limit, estimate
        if unique_count >= self.value_count_limit:
            return 1.0  # Assume very high cardinality
        
        return unique_count / total_count
    
    def is_nested(self, field_name: str) -> bool:
        """Check if field contains nested objects"""
        return field_name in self.nested_fields
    
    def is_array(self, field_name: str) -> bool:
        """Check if field contains arrays"""
        return field_name in self.array_fields
    
    def get_dominant_pattern(self, field_name: str) -> str:
        """Get the dominant pattern detected for string values"""
        if field_name not in self.pattern_matches:
            return 'none'
        
        patterns = self.pattern_matches[field_name]
        if not patterns:
            return 'none'
        
        return max(patterns.items(), key=lambda x: x[1])[0]
    
    def get_field_analysis(self, field_name: str) -> Dict[str, Any]:
        """
        Get comprehensive analysis for a single field
        
        Returns:
            Dictionary with all metrics for the field
        """
        frequency = self.get_field_frequency(field_name)
        dominant_type, type_stability = self.get_type_stability(field_name)
        cardinality = self.get_cardinality(field_name)
        
        return {
            'field_name': field_name,
            'frequency': frequency,
            'frequency_percent': f"{frequency * 100:.1f}%",
            'total_occurrences': self.field_counts[field_name],
            'dominant_type': dominant_type,
            'type_stability': type_stability,
            'type_stability_percent': f"{type_stability * 100:.1f}%",
            'type_distribution': dict(self.field_types[field_name]),
            'cardinality': cardinality,
            'unique_values': len(self.field_values[field_name]),
            'is_nested': self.is_nested(field_name),
            'is_array': self.is_array(field_name),
            'dominant_pattern': self.get_dominant_pattern(field_name),
        }
    
    def get_all_fields_analysis(self) -> List[Dict[str, Any]]:
        """Get analysis for all discovered fields"""
        all_fields = set(self.field_counts.keys())
        analyses = []
        
        for field_name in sorted(all_fields):
            analyses.append(self.get_field_analysis(field_name))
        
        return analyses
    
    def get_summary(self) -> Dict[str, Any]:
        """Get overall analysis summary"""
        return {
            'total_records_analyzed': self.total_records,
            'total_fields_discovered': len(self.field_counts),
            'nested_fields': len(self.nested_fields),
            'array_fields': len(self.array_fields),
            'fields': self.get_all_fields_analysis()
        }
    
    def export_state(self) -> Dict:
        """Export analyzer state for persistence"""
        return {
            'total_records': self.total_records,
            'field_counts': dict(self.field_counts),
            'field_types': {k: dict(v) for k, v in self.field_types.items()},
            'nested_fields': list(self.nested_fields),
            'array_fields': list(self.array_fields),
            'pattern_matches': {k: dict(v) for k, v in self.pattern_matches.items()},
        }
    
    def import_state(self, state: Dict):
        """Import previously saved state"""
        self.total_records = state.get('total_records', 0)
        self.field_counts = defaultdict(int, state.get('field_counts', {}))
        self.field_types = defaultdict(
            lambda: defaultdict(int),
            {k: defaultdict(int, v) for k, v in state.get('field_types', {}).items()}
        )
        self.nested_fields = set(state.get('nested_fields', []))
        self.array_fields = set(state.get('array_fields', []))
        self.pattern_matches = defaultdict(
            lambda: defaultdict(int),
            {k: defaultdict(int, v) for k, v in state.get('pattern_matches', {}).items()}
        )


if __name__ == "__main__":
    # Test the analyzer
    from mock_data_generator import MockDataGenerator
    from normalizer import FieldNormalizer
    
    print("Data Analyzer Test")
    print("=" * 100)
    
    # Generate test data
    generator = MockDataGenerator(seed=42)
    normalizer = FieldNormalizer()
    analyzer = DataAnalyzer()
    
    # Analyze 200 records
    print("\nAnalyzing 200 records...")
    for i in range(200):
        record = generator.generate_record()
        normalized, _ = normalizer.normalize_record(record)
        analyzer.analyze_record(normalized)
    
    # Get summary
    summary = analyzer.get_summary()
    
    print(f"\nTotal records analyzed: {summary['total_records_analyzed']}")
    print(f"Total fields discovered: {summary['total_fields_discovered']}")
    print(f"Nested fields: {summary['nested_fields']}")
    print(f"Array fields: {summary['array_fields']}")
    
    print("\n" + "=" * 100)
    print("Field Analysis (sorted by frequency):")
    print("=" * 100)
    
    # Sort by frequency
    fields = sorted(summary['fields'], key=lambda x: x['frequency'], reverse=True)
    
    for field in fields:
        print(f"\n{field['field_name']}:")
        print(f"  Frequency: {field['frequency_percent']} ({field['total_occurrences']} records)")
        print(f"  Type: {field['dominant_type']} (stability: {field['type_stability_percent']})")
        print(f"  Cardinality: {field['cardinality']:.3f} ({field['unique_values']} unique values)")
        
        if field['is_nested']:
            print(f"  Contains nested objects")
        if field['is_array']:
            print(f"  Contains arrays")
        if field['dominant_pattern'] != 'none':
            print(f"  Pattern detected: {field['dominant_pattern']}")
        
        if len(field['type_distribution']) > 1:
            print(f"  Type distribution: {field['type_distribution']}")