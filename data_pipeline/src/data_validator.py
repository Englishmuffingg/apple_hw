"""
Data validation functions
"""

from dateutil import parser as date_parser
from .data_models import REQUIRED_FIELDS, EVENT_TYPES


def check_required_fields(event):
    """Check if all required fields are present"""
    missing = []
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            missing.append(field)
    
    if missing:
        return False, "Missing required fields"
    return True, None


def check_event_type(event):
    """Validate event type"""
    event_type = event.get('event_type', '')
    if event_type not in EVENT_TYPES:
        return False, "Invalid event type"
    return True, None


def parse_timestamp(event):
    """Parse timestamp string to datetime"""
    timestamp_str = event.get('timestamp', '')
    try:
        parsed = date_parser.parse(timestamp_str)
        return True, parsed, None
    except:
        return False, None, "Invalid timestamp"


def check_metadata(event):
    """Check metadata format and content"""
    metadata = event.get('metadata', {})
    
    if not isinstance(metadata, dict):
        return False, "Metadata must be a dictionary"
    
    # Purchase events need amount and currency
    if event.get('event_type') == 'purchase':
        if 'amount' not in metadata or 'currency' not in metadata:
            return False, "Purchase events need amount and currency"
    
    return True, None


def validate_event(event):
    """Validate a single event"""
    # Check required fields
    valid, error = check_required_fields(event)
    if not valid:
        return False, None, error
    
    # Check event type
    valid, error = check_event_type(event)
    if not valid:
        return False, None, error
    
    # Parse timestamp
    valid, parsed_time, error = parse_timestamp(event)
    if not valid:
        return False, None, error
    
    # Check metadata
    valid, error = check_metadata(event)
    if not valid:
        return False, None, error
    
    return True, parsed_time, None


def validate_batch(events):
    """Validate multiple events"""
    good_events = []
    bad_events = []
    errors = []
    
    for event in events:
        is_valid, parsed_time, error = validate_event(event)
        if is_valid:
            event['_timestamp'] = parsed_time
            good_events.append(event)
        else:
            bad_events.append(event)
            if error:
                errors.append(error)
    
    return good_events, bad_events, errors 