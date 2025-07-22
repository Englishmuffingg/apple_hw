"""
Basic data structures for the pipeline
"""

import pandas as pd

# Constants
REQUIRED_FIELDS = ['user_id', 'timestamp', 'event_type']
EVENT_TYPES = ['signup', 'login', 'click', 'purchase']


def clean_metadata(metadata):
    """Extract useful fields from metadata"""
    result = {
        'screen': metadata.get('screen', ''),
        'button_id': metadata.get('button_id', ''),
        'amount': None,
        'currency': None
    }
    
    # Handle purchase amount
    if 'amount' in metadata:
        try:
            result['amount'] = float(metadata['amount'])
        except:
            pass
    
    if 'currency' in metadata:
        result['currency'] = metadata['currency']
    
    return result


def create_base_event(user_id, timestamp, event_date, event_type, screen, button_id):
    """Create a base event record as a dictionary"""
    return {
        'user_id': user_id,
        'timestamp': timestamp,
        'event_date': event_date,
        'event_type': event_type,
        'screen': screen,
        'button_id': button_id
    }


def create_purchase_detail(user_id, timestamp, event_date, amount, currency):
    """Create a purchase detail record as a dictionary"""
    return {
        'user_id': user_id,
        'timestamp': timestamp,
        'event_date': event_date,
        'amount': amount,
        'currency': currency
    }