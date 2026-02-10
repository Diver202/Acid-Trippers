"""
Data Stream Client
Fetches JSON records from the synthetic data streaming API
"""

import requests
import json
import time
from typing import List, Dict, Any, Optional, Iterator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataStreamClient:
    """
    Client for fetching data from the synthetic data streaming API
    """
    
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        """
        Initialize the client
        
        Args:
            base_url: Base URL of the FastAPI server
        """
        self.base_url = base_url.rstrip('/')
        self._verify_connection()
    
    def _verify_connection(self):
        """Verify that the API is accessible"""
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            response.raise_for_status()
            logger.info(f"✓ Successfully connected to API at {self.base_url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f" Could not connect to API at {self.base_url}")
            logger.warning(f"  Error: {e}")
            logger.warning(f"  Make sure the FastAPI server is running:")
            logger.warning(f"    uvicorn app:app --reload --port 8000")
    
    def fetch_single_record(self) -> Dict[str, Any]:
        """
        Fetch a single record from the API
        
        Returns:
            Single JSON record
        """
        try:
            response = requests.get(f"{self.base_url}/")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching single record: {e}")
            raise
    
    def fetch_batch(self, count: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch multiple records in a batch
        
        Args:
            count: Number of records to fetch
            
        Returns:
            List of JSON records
        """
        try:
            response = requests.get(f"{self.base_url}/record/{count}")
            response.raise_for_status()
            data = response.json()
            
            # The API might return a list directly or wrapped in a key
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'records' in data:
                return data['records']
            else:
                # Assume it's a single record, wrap it in a list
                return [data]
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching batch of {count} records: {e}")
            raise
    
    def stream_records(self, total_records: int, batch_size: int = 100, 
                      delay: float = 0.1) -> Iterator[Dict[str, Any]]:
        """
        Stream records in batches with configurable delay
        
        Args:
            total_records: Total number of records to fetch
            batch_size: Records per batch request
            delay: Delay between batches in seconds
            
        Yields:
            Individual JSON records
        """
        records_fetched = 0
        
        while records_fetched < total_records:
            # Calculate how many to fetch in this batch
            remaining = total_records - records_fetched
            current_batch_size = min(batch_size, remaining)
            
            try:
                batch = self.fetch_batch(current_batch_size)
                
                for record in batch:
                    yield record
                    records_fetched += 1
                
                logger.info(f"Fetched {records_fetched}/{total_records} records")
                
                # Delay before next batch (if more to fetch)
                if records_fetched < total_records:
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error in streaming at record {records_fetched}: {e}")
                break
    
    def save_to_file(self, num_records: int, filename: str, 
                     batch_size: int = 100) -> str:
        """
        Fetch records and save to JSON file
        
        Args:
            num_records: Number of records to fetch
            filename: Output filename
            batch_size: Records per API call
            
        Returns:
            Path to saved file
        """
        records = []
        
        for record in self.stream_records(num_records, batch_size):
            records.append(record)
        
        with open(filename, 'w') as f:
            json.dump(records, f, indent=2)
        
        logger.info(f"Saved {len(records)} records to {filename}")
        return filename


if __name__ == "__main__":
    # Test the client
    client = DataStreamClient()
    
    print("\n" + "=" * 80)
    print("Testing Data Stream Client")
    print("=" * 80)
    
    try:
        # Fetch a single record
        print("\n1. Fetching single record...")
        record = client.fetch_single_record()
        print("Sample record:")
        print(json.dumps(record, indent=2))
        
        # Fetch a small batch
        print("\n2. Fetching batch of 5 records...")
        batch = client.fetch_batch(5)
        print(f"Received {len(batch)} records")
        print("First record keys:", list(batch[0].keys()))
        
        # Save a larger dataset
        print("\n3. Saving 100 records to file...")
        output_file = "/home/claude/adaptive_ingestion/data/api_stream.json"
        client.save_to_file(100, output_file, batch_size=50)
        
        print("\n" + "=" * 80)
        print("✓ All tests passed!")
        print("=" * 80)
        
    except Exception as e:
        print("\n" + "=" * 80)
        print(f"✗ Error: {e}")
        print("\nMake sure the FastAPI server is running:")
        print("  cd Course_Resources/CS432_Databases/Assignments/T2")
        print("  uvicorn app:app --reload --port 8000")
        print("=" * 80)