"""
Data aggregation functions
Calculate stats and summaries
"""

import pandas as pd
from pathlib import Path
from datetime import datetime


def events_per_day(base_df):
    """Count events by type per day"""
    if base_df.empty:
        return pd.DataFrame(columns=['event_date', 'event_type', 'count'])
    
    # Group by date and event type
    daily = base_df.groupby(['event_date', 'event_type']).size().reset_index(name='count')
    daily = daily.sort_values(['event_date', 'event_type'])
    
    print(f"Created {len(daily)} daily aggregations")
    return daily


def count_active_users(base_df):
    """Count total active users"""
    if base_df.empty:
        return pd.DataFrame(columns=['total_active_users', 'calculation_date'])
    
    total_users = base_df['user_id'].nunique()
    today = datetime.now().strftime('%Y-%m-%d')
    
    result = pd.DataFrame({
        'total_active_users': [total_users],
        'calculation_date': [today]
    })
    
    print(f"Found {total_users} active users")
    return result


def find_most_active_user(base_df):
    """Find user with most events"""
    if base_df.empty:
        return pd.DataFrame(columns=['user_id', 'total_events', 
                                   'signup_count', 'login_count', 
                                   'click_count', 'purchase_count'])
    
    # Count total events per user
    user_counts = base_df.groupby('user_id').size().reset_index(name='total_events')
    
    # Count events by type per user
    event_type_counts = base_df.groupby(['user_id', 'event_type']).size().unstack(fill_value=0)
    event_type_counts = event_type_counts.reset_index()
    
    # Ensure all event types are present
    for event_type in ['signup', 'login', 'click', 'purchase']:
        if event_type not in event_type_counts.columns:
            event_type_counts[event_type] = 0
    
    # Rename columns
    event_type_counts.columns = ['user_id'] + [f'{col}_count' for col in event_type_counts.columns[1:]]
    
    # Merge with total counts
    result = user_counts.merge(event_type_counts, on='user_id')
    
    # Find most active user
    most_active = result.loc[result['total_events'].idxmax()]
    
    print(f"Most active user: {most_active['user_id']} with {most_active['total_events']} events")
    
    return pd.DataFrame([most_active])


def run_all_aggregations(base_df, purchase_df):
    """Run all aggregation functions"""
    results = {}
    
    # Required aggregations only
    results['events_by_type_per_day'] = events_per_day(base_df)
    results['active_users'] = count_active_users(base_df)
    results['most_active_user'] = find_most_active_user(base_df)
    
    return results


def save_aggregation_results(results, output_dir):
    """Save aggregation results to CSV files"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for name, df in results.items():
        if not df.empty:
            file_path = output_path / f"{name}.csv"
            df.to_csv(file_path, index=False)
            print(f"Saved {name} to {file_path}")


def get_aggregation_summary(results):
    """Get summary of aggregation results"""
    summary = {}
    
    # Most active user
    if not results['most_active_user'].empty:
        most_active = results['most_active_user'].iloc[0]
        summary['most_active_user'] = {
            'user_id': most_active['user_id'],
            'total_events': most_active['total_events']
        }
    
    return summary 