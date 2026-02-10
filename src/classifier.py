"""
Classifier Module
Makes autonomous decisions about field placement (SQL vs MongoDB)
based on analyzed data patterns and heuristics.

Decision Logic:
- SQL: High frequency, stable types, non-nested, structured
- MongoDB: Low frequency, nested objects, arrays, type drift
- Both: username and sys_ingested_at (for joins)
"""

from typing import Dict, Any, List, Tuple, Set
from dataclasses import dataclass
from enum import Enum
import json


class Backend(Enum):
    """Storage backend options"""
    SQL = "sql"
    MONGODB = "mongodb"
    BOTH = "both"  # For fields that must exist in both backends


@dataclass
class FieldClassification:
    """Result of classifying a single field"""
    field_name: str
    backend: Backend
    reason: str
    confidence: float  # 0.0 to 1.0
    
    # Supporting metrics
    frequency: float
    type_stability: float
    dominant_type: str
    is_nested: bool
    is_array: bool
    cardinality: float
    is_unique: bool


class Classifier:
    """
    Classifies fields into SQL or MongoDB based on heuristic rules
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize classifier with configuration
        
        Args:
            config: Classification thresholds and rules
        """
        # Default thresholds (can be overridden)
        self.config = config or {}
        
        # Thresholds for SQL placement
        self.sql_frequency_threshold = self.config.get('sql_frequency', 0.80)
        self.sql_type_stability_threshold = self.config.get('sql_type_stability', 0.90)
        self.unique_cardinality_threshold = self.config.get('unique_cardinality', 0.95)
        self.sparse_threshold = self.config.get('sparse_threshold', 0.30)
        
        # Fields that must be in both backends
        self.mandatory_both_fields = {'username', 'sys_ingested_at'}
        
        # Store classification results
        self.classifications: Dict[str, FieldClassification] = {}
    
    def _classify_field(self, field_analysis: Dict[str, Any]) -> FieldClassification:
        """
        Classify a single field based on its analysis
        
        Args:
            field_analysis: Analysis results from DataAnalyzer
            
        Returns:
            FieldClassification with decision and reasoning
        """
        field_name = field_analysis['field_name']
        frequency = field_analysis['frequency']
        type_stability = field_analysis['type_stability']
        dominant_type = field_analysis['dominant_type']
        is_nested = field_analysis['is_nested']
        is_array = field_analysis['is_array']
        cardinality = field_analysis['cardinality']
        
        # Determine if field should be UNIQUE in SQL
        is_unique = (
            cardinality >= self.unique_cardinality_threshold and
            frequency >= self.sql_frequency_threshold
        )
        
        # Rule 1: Mandatory fields must be in BOTH backends (for joins)
        if field_name in self.mandatory_both_fields:
            return FieldClassification(
                field_name=field_name,
                backend=Backend.BOTH,
                reason="Mandatory join field - required in both backends",
                confidence=1.0,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=is_unique
            )
        
        # Rule 2: Nested objects MUST go to MongoDB
        if is_nested:
            return FieldClassification(
                field_name=field_name,
                backend=Backend.MONGODB,
                reason="Contains nested objects - MongoDB handles nesting better",
                confidence=1.0,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=False
            )
        
        # Rule 3: Arrays MUST go to MongoDB
        if is_array:
            return FieldClassification(
                field_name=field_name,
                backend=Backend.MONGODB,
                reason="Contains arrays - MongoDB handles arrays natively",
                confidence=1.0,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=False
            )
        
        # Rule 4: Very sparse fields go to MongoDB
        if frequency < self.sparse_threshold:
            return FieldClassification(
                field_name=field_name,
                backend=Backend.MONGODB,
                reason=f"Sparse field (only {frequency*100:.1f}% frequency) - MongoDB handles optional fields better",
                confidence=0.9,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=False
            )
        
        # Rule 5: Type instability suggests MongoDB
        if type_stability < self.sql_type_stability_threshold:
            return FieldClassification(
                field_name=field_name,
                backend=Backend.MONGODB,
                reason=f"Type instability ({type_stability*100:.1f}% stable) - MongoDB handles schema flexibility",
                confidence=0.85,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=False
            )
        
        # Rule 6: High frequency + stable type + simple = SQL
        if (frequency >= self.sql_frequency_threshold and 
            type_stability >= self.sql_type_stability_threshold and
            dominant_type in ['string', 'integer', 'float', 'boolean']):
            
            confidence = min(frequency, type_stability)
            
            return FieldClassification(
                field_name=field_name,
                backend=Backend.SQL,
                reason=f"High frequency ({frequency*100:.1f}%), stable type ({type_stability*100:.1f}%), structured",
                confidence=confidence,
                frequency=frequency,
                type_stability=type_stability,
                dominant_type=dominant_type,
                is_nested=is_nested,
                is_array=is_array,
                cardinality=cardinality,
                is_unique=is_unique
            )
        
        # Rule 7: Default to MongoDB for ambiguous cases
        # This is the "schema-on-read" philosophy
        return FieldClassification(
            field_name=field_name,
            backend=Backend.MONGODB,
            reason="Ambiguous pattern - MongoDB provides flexibility",
            confidence=0.6,
            frequency=frequency,
            type_stability=type_stability,
            dominant_type=dominant_type,
            is_nested=is_nested,
            is_array=is_array,
            cardinality=cardinality,
            is_unique=False
        )
    
    def classify_all_fields(self, field_analyses: List[Dict[str, Any]]) -> Dict[str, FieldClassification]:
        """
        Classify all fields from analyzer output
        
        Args:
            field_analyses: List of field analysis results from DataAnalyzer
            
        Returns:
            Dictionary mapping field_name -> FieldClassification
        """
        self.classifications = {}
        
        for analysis in field_analyses:
            classification = self._classify_field(analysis)
            self.classifications[classification.field_name] = classification
        
        return self.classifications
    
    def get_sql_fields(self) -> List[FieldClassification]:
        """Get all fields classified for SQL storage"""
        return [
            c for c in self.classifications.values()
            if c.backend in [Backend.SQL, Backend.BOTH]
        ]
    
    def get_mongodb_fields(self) -> List[FieldClassification]:
        """Get all fields classified for MongoDB storage"""
        return [
            c for c in self.classifications.values()
            if c.backend in [Backend.MONGODB, Backend.BOTH]
        ]
    
    def get_unique_fields(self) -> List[str]:
        """Get fields that should have UNIQUE constraint in SQL"""
        return [
            c.field_name for c in self.get_sql_fields()
            if c.is_unique
        ]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get classification summary"""
        sql_count = len([c for c in self.classifications.values() if c.backend == Backend.SQL])
        mongo_count = len([c for c in self.classifications.values() if c.backend == Backend.MONGODB])
        both_count = len([c for c in self.classifications.values() if c.backend == Backend.BOTH])
        
        return {
            'total_fields': len(self.classifications),
            'sql_only': sql_count,
            'mongodb_only': mongo_count,
            'both': both_count,
            'unique_fields': len(self.get_unique_fields()),
            'thresholds_used': {
                'sql_frequency': self.sql_frequency_threshold,
                'sql_type_stability': self.sql_type_stability_threshold,
                'unique_cardinality': self.unique_cardinality_threshold,
                'sparse_threshold': self.sparse_threshold
            }
        }
    
    def export_classifications(self) -> Dict:
        """Export classifications for persistence"""
        return {
            field_name: {
                'backend': classification.backend.value,
                'reason': classification.reason,
                'confidence': classification.confidence,
                'is_unique': classification.is_unique,
                'dominant_type': classification.dominant_type
            }
            for field_name, classification in self.classifications.items()
        }
    
    def print_classification_report(self):
        """Print a detailed classification report"""
        print("\n" + "=" * 100)
        print("FIELD CLASSIFICATION REPORT")
        print("=" * 100)
        
        summary = self.get_summary()
        print(f"\nTotal Fields: {summary['total_fields']}")
        print(f"  → SQL Only: {summary['sql_only']}")
        print(f"  → MongoDB Only: {summary['mongodb_only']}")
        print(f"  → Both (Join Fields): {summary['both']}")
        print(f"  → Fields with UNIQUE constraint: {summary['unique_fields']}")
        
        print(f"\nThresholds Used:")
        for key, value in summary['thresholds_used'].items():
            print(f"  {key}: {value}")
        
        # Group by backend
        for backend in [Backend.BOTH, Backend.SQL, Backend.MONGODB]:
            fields = [c for c in self.classifications.values() if c.backend == backend]
            if not fields:
                continue
            
            print(f"\n{'─' * 100}")
            print(f"{backend.value.upper()} FIELDS ({len(fields)} fields)")
            print('─' * 100)
            
            for field in sorted(fields, key=lambda x: x.frequency, reverse=True):
                print(f"\n{field.field_name}")
                print(f"  Backend: {field.backend.value}")
                print(f"  Confidence: {field.confidence:.2f}")
                print(f"  Reason: {field.reason}")
                print(f"  Metrics: freq={field.frequency:.2%}, type_stab={field.type_stability:.2%}, " +
                      f"card={field.cardinality:.3f}")
                if field.is_unique:
                    print(f"  ⭐ UNIQUE constraint")


if __name__ == "__main__":
    # Test the classifier
    from mock_data_generator import MockDataGenerator
    from normalizer import FieldNormalizer
    from analyzer import DataAnalyzer
    
    print("Classifier Test")
    print("=" * 100)
    
    # Generate and analyze data
    generator = MockDataGenerator(seed=42)
    normalizer = FieldNormalizer()
    analyzer = DataAnalyzer()
    
    print("\nAnalyzing 300 records...")
    for i in range(300):
        record = generator.generate_record()
        normalized, _ = normalizer.normalize_record(record)
        
        # Add sys_ingested_at (server timestamp) - required for bi-temporal tracking
        from datetime import datetime
        normalized['sys_ingested_at'] = datetime.now().isoformat()
        
        analyzer.analyze_record(normalized)
    
    # Get field analyses
    field_analyses = analyzer.get_all_fields_analysis()
    
    # Classify fields
    classifier = Classifier()
    classifier.classify_all_fields(field_analyses)
    
    # Print report
    classifier.print_classification_report()
    
    # Show SQL schema suggestion
    print("\n" + "=" * 100)
    print("SUGGESTED SQL SCHEMA")
    print("=" * 100)
    
    sql_fields = classifier.get_sql_fields()
    print("\nCREATE TABLE records (")
    for field in sql_fields:
        # Map types to SQL types
        type_map = {
            'string': 'VARCHAR(255)',
            'integer': 'INTEGER',
            'float': 'FLOAT',
            'boolean': 'BOOLEAN'
        }
        sql_type = type_map.get(field.dominant_type, 'TEXT')
        unique = ' UNIQUE' if field.is_unique else ''
        print(f"    {field.field_name:20s} {sql_type:15s}{unique},")
    print("    PRIMARY KEY (sys_ingested_at, username)")
    print(");")