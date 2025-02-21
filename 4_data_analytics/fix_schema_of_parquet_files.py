import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq

# Directory where your parquet files are stored
DOWNLOAD_DIR = "."

# Process green tripdata files for 'ehail_fee'
green_pattern = os.path.join(DOWNLOAD_DIR, "green_tripdata_*.parquet")
for file_path in glob.glob(green_pattern):
    print(f"Processing {file_path} for green data...")
    # Read the parquet file as a PyArrow Table
    table = pq.read_table(file_path)
    
    # Check if the 'ehail_fee' column exists
    if 'ehail_fee' in table.schema.names:
        col_index = table.schema.get_field_index('ehail_fee')
        new_column = table.column('ehail_fee').cast(pa.float64())
        table = table.set_column(col_index, 'ehail_fee', new_column)
        pq.write_table(table, file_path)
        print(f"Updated 'ehail_fee' to FLOAT64 in {file_path}.")
    else:
        print(f"No 'ehail_fee' column found in {file_path}.")

# Process yellow tripdata files for 'airport_fee' and 'ehail_fee'
yellow_pattern = os.path.join(DOWNLOAD_DIR, "yellow_tripdata_*.parquet")
for file_path in glob.glob(yellow_pattern):
    print(f"Processing {file_path} for yellow data...")
    # Read the parquet file as a PyArrow Table
    table = pq.read_table(file_path)
    
    # Process 'airport_fee' column if it exists
    if 'airport_fee' in table.schema.names:
        col_index = table.schema.get_field_index('airport_fee')
        new_column = table.column('airport_fee').cast(pa.float64())
        table = table.set_column(col_index, 'airport_fee', new_column)
        print(f"Updated 'airport_fee' to FLOAT64 in {file_path}.")
    else:
        print(f"No 'airport_fee' column found in {file_path}.")
    
    # Write the updated table back to the file (overwriting it)
    pq.write_table(table, file_path)
    
# Process each fhv file
fhv_pattern = os.path.join(DOWNLOAD_DIR, "fhv_tripdata_*.parquet")
for file_path in glob.glob(fhv_pattern):
    print(f"Processing {file_path} for fhv data...")
    # Read the Parquet file as a PyArrow Table
    table = pq.read_table(file_path)
    
    # Check if the 'DOlocationID' column exists
    if 'DOlocationID' in table.schema.names:
        col_index = table.schema.get_field_index('DOlocationID')
        new_column = table.column('DOlocationID').cast(pa.float64())
        table = table.set_column(col_index, 'DOlocationID', new_column)
        pq.write_table(table, file_path)
        print(f"Updated 'DOlocationID' to FLOAT64 in {file_path}.")
    else:
        print(f"No 'DOlocationID' column found in {file_path}.")
        
        # Check if the 'PUlocationID' column exists
    if 'PUlocationID' in table.schema.names:
        col_index = table.schema.get_field_index('PUlocationID')
        new_column = table.column('PUlocationID').cast(pa.float64())
        table = table.set_column(col_index, 'PUlocationID', new_column)
        pq.write_table(table, file_path)
        print(f"Updated 'PUlocationID' to FLOAT64 in {file_path}.")
    else:
        print(f"No 'PUlocationID' column found in {file_path}.")
        
    if 'SR_Flag' in table.schema.names:
        col_index = table.schema.get_field_index('SR_Flag')
        new_column = table.column('SR_Flag').cast(pa.float64())
        table = table.set_column(col_index, 'SR_Flag', new_column)
        pq.write_table(table, file_path)
        print(f"Updated 'SR_Flag' to FLOAT64 in {file_path}.")
    else:
        print(f"No 'SR_Flag' column found in {file_path}.")
