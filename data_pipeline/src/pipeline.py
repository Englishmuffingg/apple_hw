"""
Main pipeline functions
"""

from pathlib import Path
from .data_processor import load_data, process_events, save_data, get_processing_summary
from .aggregator import run_all_aggregations, save_aggregation_results, get_aggregation_summary


def run_pipeline(raw_data_path, processed_dir, aggregated_dir):
    """Run the data pipeline"""
    print("Starting pipeline...")
    
    # Create output directories
    Path(processed_dir).mkdir(parents=True, exist_ok=True)
    Path(aggregated_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        # Load raw data
        print("Loading data...")
        raw_data = load_data(raw_data_path)
        
        # Process the data
        print("Processing data...")
        base_df, purchase_df = process_events(raw_data)
        
        # Save processed data
        print("Saving processed data...")
        if not base_df.empty:
            save_data(base_df, purchase_df, processed_dir)
        
        # Run aggregations
        print("Running aggregations...")
        agg_results = run_all_aggregations(base_df, purchase_df)
        
        # Save aggregation results
        print("Saving aggregation results...")
        save_aggregation_results(agg_results, aggregated_dir)
        
        # Get summaries
        processing_summary = get_processing_summary(base_df, purchase_df)
        agg_summary = get_aggregation_summary(agg_results)
        
        return {
            'processing_summary': processing_summary,
            'aggregation_summary': agg_summary
        }
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


def check_pipeline_setup(raw_data_path, processed_dir, aggregated_dir):
    """Check if pipeline setup is ready"""
    # Check if raw data exists
    if not Path(raw_data_path).exists():
        print(f"Raw data file not found: {raw_data_path}")
        return False
    
    # Try to create output directories
    try:
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        Path(aggregated_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Cannot create output directories: {e}")
        return False
    
    return True 