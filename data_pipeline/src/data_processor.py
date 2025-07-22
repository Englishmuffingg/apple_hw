"""
Data processing functions
Load, clean, and save event data
"""

import json
import pandas as pd
from pathlib import Path
from .data_validator import validate_batch
from .data_models import (clean_metadata, create_base_event, 
                         create_purchase_detail)


def load_data(file_path):
    """Load raw JSON data from file"""
    print(f"Loading data from {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        print(f"Loaded {len(data)} events")
        return data
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        raise


def clean_single_event(event):
    """Clean up one event and return base event and optional purchase detail"""
    timestamp = event['_timestamp']
    event_date = timestamp.date()
    metadata = clean_metadata(event.get('metadata', {}))
    
    # Create base event (for all events)
    base_event = create_base_event(
        user_id=event['user_id'],
        timestamp=timestamp,
        event_date=event_date,
        event_type=event['event_type'],
        screen=metadata['screen'],
        button_id=metadata['button_id']
    )
    
    # Create purchase detail (only for purchase events)
    purchase_detail = None
    if event['event_type'] == 'purchase' and metadata['amount'] is not None:
        purchase_detail = create_purchase_detail(
            user_id=event['user_id'],
            timestamp=timestamp,
            event_date=event_date,
            amount=metadata['amount'],
            currency=metadata['currency']
        )
    
    return base_event, purchase_detail


def process_events(raw_events):
    """Process raw events into separated dataframes"""
    if not raw_events:
        print("No events to process")
        return pd.DataFrame(), pd.DataFrame()
    
    # Validate events
    good_events, bad_events, _ = validate_batch(raw_events)
    
    if bad_events:
        print(f"Found {len(bad_events)} invalid events")
    
    if not good_events:
        print("No valid events after validation")
        return pd.DataFrame(), pd.DataFrame()
    
    # Convert to separated format
    base_events = []
    purchase_details = []
    
    for event in good_events:
        base_event, purchase_detail = clean_single_event(event)
        base_events.append(base_event)
        
        if purchase_detail is not None:
            purchase_details.append(purchase_detail)
    
    # Create DataFrames
    base_df = pd.DataFrame(base_events)
    purchase_df = pd.DataFrame(purchase_details)
    
    # Fix data types
    if not base_df.empty:
        base_df['timestamp'] = pd.to_datetime(base_df['timestamp'], utc=True)
        base_df['event_date'] = pd.to_datetime(base_df['event_date'])
    
    if not purchase_df.empty:
        purchase_df['timestamp'] = pd.to_datetime(purchase_df['timestamp'], utc=True)
        purchase_df['event_date'] = pd.to_datetime(purchase_df['event_date'])
        purchase_df['amount'] = purchase_df['amount'].astype('float64')
    
    print(f"Processed {len(base_df)} base events and {len(purchase_df)} purchase details")
    
    return base_df, purchase_df


def save_data(base_df, purchase_df, output_dir):
    """Save separated data to disk"""
    output_path = Path(output_dir)
    
    base_file = output_path / "base_events.parquet"
    purchase_file = output_path / "purchase_details.parquet"
    
    base_df.to_parquet(base_file, index=False)
    print(f"Saved base events to {base_file}")
    
    if not purchase_df.empty:
        purchase_df.to_parquet(purchase_file, index=False)
        print(f"Saved purchase details to {purchase_file}")


def get_processing_summary(base_df, purchase_df):
    """Get summary of results"""
    if base_df.empty:
        return {"message": "No data"}
    
    return {
        "total_events": len(base_df),
        "purchase_events": len(purchase_df),
        "unique_users": base_df['user_id'].nunique()
    } 