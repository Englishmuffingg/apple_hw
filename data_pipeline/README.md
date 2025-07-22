
## My Approach

I built a data pipeline that processes user event data (clicks, logins, signups, purchases). The pipeline separates basic event information from purchase-specific details to optimize data structure and avoid unnecessary null values. The implementation includes data validation, cleaning, and generates the three required aggregations. The pipeline uses a functional programming approach with modular functions for each processing step.

## Assumptions I Made

- The raw data is valid JSON with the required fields 
- Purchase events will always have amount and currency fields
- Timestamps follow standard ISO format
- The data size is reasonable for in-memory processing 
- I have write permissions for the output directories

## How to Run

```bash
# Run from project root 
python data_pipeline/main.py


## Output Files

**Processed data** (`data_pipeline/data/processed/`):
- `base_events.parquet` - All events with basic info 
- `purchase_details.parquet` - Purchase-specific data 

**Aggregation results** (`data_pipeline/data/aggregated/`):
- `events_by_type_per_day.csv` - Daily event counts by type
- `active_users.csv` - Total number of active users
- `most_active_user.csv` - User with most events

