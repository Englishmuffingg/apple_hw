"""
Main script to run the data pipeline
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent / "src"))

from src.pipeline import run_pipeline, check_pipeline_setup






def main():
    """Run the pipeline with tests"""
    

    # Set up file paths
    script_dir = Path(__file__).parent
    raw_data_path = script_dir / "data/raw/raw_events.json"
    processed_dir = script_dir / "data/processed"
    aggregated_dir = script_dir / "data/aggregated"
    
    try:
        # Check setup
        if not check_pipeline_setup(raw_data_path, processed_dir, aggregated_dir):
            print("Setup failed")
            sys.exit(1)
        
        # Run the pipeline
        print("Running data pipeline...")
        results = run_pipeline(raw_data_path, processed_dir, aggregated_dir)
        
        # Show results
        proc_summary = results.get('processing_summary', {})
        if proc_summary.get('message') != "No data":
            print(f"Processed {proc_summary.get('total_events', 0)} total events")
            print(f"Including {proc_summary.get('purchase_events', 0)} purchase events")
            print(f"Found {proc_summary.get('unique_users', 0)} unique users")
            
            # Most active user
            agg_summary = results.get('aggregation_summary', {})
            most_active = agg_summary.get('most_active_user', {})
            if most_active:
                print(f"Most active user: {most_active.get('user_id', 'N/A')} ({most_active.get('total_events', 0)} events)")
        
        print("Pipeline completed successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 