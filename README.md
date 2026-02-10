# Adaptive Ingestion & Hybrid Backend Placement

**CS 432 - Databases | Assignment 1**  
**IIT Gandhinagar | Instructor: Dr. Yogesh K. Meena**  
**Deadline: February 15, 2026**

## Project Overview

An autonomous data ingestion system that dynamically determines optimal storage backend (SQL or MongoDB) for incoming JSON records based on data patterns, field behaviors, and structural complexity.

## System Architecture

```
JSON Stream → Normalizer → Buffer → Analyzer → Classifier
                                                    ↓
                                            Metadata Store
                                                    ↓
                                    ┌───────────────┴──────────────┐
                                    ↓                              ↓
                              SQL Backend                   MongoDB Backend
                        (structured fields)             (nested/complex fields)
                        username + timestamps           username + timestamps
```

## Project Structure

```
adaptive_ingestion/
├── src/
│   ├── mock_data_generator.py    # Mock JSON stream generator (until real API is available)
│   ├── normalizer.py              # Field name normalization
│   ├── analyzer.py                # Data pattern analysis
│   ├── classifier.py              # Heuristic-based placement logic
│   ├── metadata_store.py          # Persistent decision storage
│   ├── sql_backend.py             # PostgreSQL/MySQL interface
│   ├── mongo_backend.py           # MongoDB interface
│   └── main.py                    # Orchestration pipeline
├── tests/
│   └── ...
├── data/
│   └── sample_stream.json         # Generated test data
├── config/
│   └── config.json                # System configuration
├── logs/
│   └── ...
├── requirements.txt
└── README.md
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- PostgreSQL or MySQL
- MongoDB
- Git

### Installation

1. Clone the repository (once provided by instructor)
```bash
git clone <repository_url>
cd adaptive_ingestion
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Configure databases
```bash
# Edit config/config.json with your database credentials
```

4. Run the system
```bash
python src/main.py
```

## Key Features

### Phase 1: Ingestion & Normalization
- Consumes JSON streams
- Resolves field naming ambiguities (ip vs IP vs IpAddress)
- Injects server timestamp (`sys_ingested_at`)
- Preserves client timestamp (`t_stamp`)

### Phase 2: Data Analysis
- Tracks field frequency (appears in X% of records)
- Measures type stability (dominant type %)
- Detects nesting complexity
- Identifies value patterns (IPs, emails, etc.)

### Phase 3: Heuristic Classification
- **SQL**: High frequency (>80%), stable types (>90%), non-nested
- **MongoDB**: Nested objects, arrays, low frequency, type drift
- **Special**: `username` and `sys_ingested_at` in BOTH backends

### Phase 4: Commit & Routing
- Dynamic SQL schema generation
- UNIQUE constraints for high-cardinality fields
- Metadata persistence for restart resilience

## Current Status

- [x] Mock data generator
- [x] Field normalizer
- [x] Data analyzer
- [x] Classifier with heuristics
- [ ] SQL backend integration
- [ ] MongoDB backend integration
- [ ] Metadata persistence
- [ ] End-to-end pipeline
- [ ] Technical report

## Next Steps

1. **Get actual data stream API** from course GitHub repository
2. Build normalizer module
3. Implement analyzer with metrics tracking
4. Define and test classification heuristics
5. Set up database backends
6. Test end-to-end flow
7. Write technical report

## Questions to Answer (Report)

1. How did you resolve type naming ambiguities?
2. What thresholds were used for SQL vs MongoDB placement?
3. How did you identify UNIQUE fields?
4. How did you differentiate string IPs from floats?
5. How did you handle type drift mid-stream?

## Notes

- **Bi-temporal timestamps**: Every record has `t_stamp` (client) and `sys_ingested_at` (server)
- **Username linking**: Must exist in both SQL and MongoDB for joins
- **No hardcoding**: All field mappings discovered dynamically
- **Schema-on-read**: System learns as data arrives