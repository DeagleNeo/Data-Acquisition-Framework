# todo: CSV and JSON injestion

import json
import pyodbc
import os
import glob
from typing import Any, Dict, List, Optional
from rich.console import Console
from pathlib import Path
import pandas as pd
import numpy as np

def read_sql_table(server_name, database_name, sql_table_name):
    try:
        conn_str = (
            f"Driver={{SQL Server}};"
            f"Server={server_name};"
            f"Database={database_name};"
            "Trusted_Connection=yes;"
        )

        conn = pyodbc.connect(conn_str)

        query = f"SELECT * FROM {sql_table_name}"

        df = pd.read_sql(query, conn)

        conn.close()

        return df
    
    except pyodbc.Error as e:
        raise ValueError(f"Database error: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error: {str(e)}")
    finally:
        try:
            conn.close()
        except:
            pass

def sql_to_parquet(server_name,
                   database_name,
                   sql_table_name,
                   output_path,
                   chunk_size=None):
        try:
            conn_str = (
                f"Driver={{SQL Server}};"
                f"Server={server_name};"
                f"Database={database_name};"
                "Trusted_Connection=yes;"
            )

            Path(output_path).mkdir(parents=True, exist_ok=True)

            output_file = Path(output_path) / f"{sql_table_name}.parquet"

            if chunk_size:
                conn = pyodbc.connect(conn_str)
                query = f"SELECT * FROM {sql_table_name}"

                for i, chunk in enumerate(pd.read_sql(query, conn, chunksize=chunk_size)):
                    if i == 0:
                        chunk.to_parquet(output_file, index=False)
                    else:
                        chunk.to_parquet(output_file, index=False, append=True)
                    print(f"Processed chunk {i+1} ({len(chunk)} rows)")
                conn.close()
            else:
                conn = pyodbc.connect(conn_str)
                query = f"SELECT * FROM {sql_table_name}"
                df = pd.read_sql(query, conn)
                conn.close()

                df.to_parquet(output_file, index=False)
                print(f"Processed {len(df)} rows")
            
            print(f"Successfully saved to {output_file}")
        except Exception as e:
            raise ValueError()

def csv_to_parquet(source_dir, output_dir=None):
    try:
        source_path = Path(source_dir)
        if output_dir is None:
            output_path = source_path
        else:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        csv_files = glob.glob(str(source_path/ "*.csv"))
        if not csv_files:
            raise ValueError(f"[bright_red][bold]No matching files[/bold] found in the path [bold]{source_path}[/bold] specified in Config {config_id}.[/bright_red]")
        print(f"Found {len(csv_files)} CSV files")

        for csv_file in csv_files:
            try:
                file_path = Path(csv_file)
                print(r"\nProcessing {file_path.name}...")

                df = pd.read_csv(file_path)
                print(f"Read {len(df)} rows")

                parquet_filename = file_path.stem + ".parquet"
                parquet_path = output_path / parquet_filename

                df.to_parquet(parquet_path, index=False)
                print(f"Successfully saved to {parquet_path}")

            except Exception as e:
                raise ValueError(f"[bright_red]Error processing [bold]{file_path.name}: {str(e)}[/bold].[/bright_red]")
        print("\nConversion completed!")
    
    except Exception as e:
        raise ValueError(f"[bright_red]Error: [bold]{str(e)}[/bold][/bright_red]")

def try_all_orients(json_data) -> Optional[pd.DataFrame]:
    """
    Try all possible orient options to convert JSON to DataFrame
    Returns DataFrame if successful, None if all attemps fail
    """
    orient_options = ['records', 'split', 'index', 'columns', 'values', 'table']

    # First try direct DataFrame conversion
    try:
        return pd.DataFrame(json_data)
    except: pass

    # Try each orient option
    for orient in orient_options:
        try:
            # convert to JSON string first as pd.read_json expects string
            json_str = json.dumps(json_data)
            df = pd.read_json(json_str, orient=orient)
            print(f"Successfully converted using orient: '{orient}'")
            return df
        except:
            continue

    return None

def flatten_json(json_obj: Any, parent_key: str = '', sep: str = '_') -> Dict:
    """
    Recursively flatten nested JSON structure
    """
    items: List = []

    def _flatten(obj: Any, parent: str = '') -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{parent}{sep}{key}" if parent else key
                if isinstance(value, (dict, list)):
                    _flatten(value, new_key)
                else:
                    items.append((new_key, value))
        elif isinstance(obj, list):
            for i, value in enumerate(obj):
                new_key = f"{parent}{sep}{i}" if parent else str(i)
                if isinstance(value, (dict, list)):
                    _flatten(value, new_key)
                else:
                    items.append((new_key, value))
        else:
            items.append((parent, obj))
    
    _flatten(json_obj)
    return dict(items)

def normalize_json_structure(data: List[Dict]) -> List[Dict]:
    """
    Normalize irregular JSON structure by ensuring all records have the same fields
    """
    all_keys = set()
    for record in data:
        all_keys.update(flatten_json(record).keys())
    
    normalized_data = []
    for record in data:
        flat_record = flatten_json(record)
        normalized_record = {key: flat_record.get(key, None) for key in all_keys}
        normalized_data.append(normalized_record)
    return normalized_data

def json_to_parquet(source_dir, output_dir=None, chunk_size: Optional[int] = None):
    """
        Convert all JSON files in a directory to parquet format
        
        Parameters:
        source_dir (str): Directory containing JSON files
        output_dir (str): Directory to save parquet files (if None, saves in same directory)
        orient (str): The format of JSON data - 'records', 'split', 'index', etc.
                    'records' is for list-like JSON [{"column":"value"}, ... ]
                    'split' is for {'index': [...], 'columns': [...], 'data': [...]}
    """
    try:
        source_path = Path(source_dir)

        output_path = Path(output_dir) if output_dir else source_path
        output_path.mkdir(parents=True, exist_ok=True)
        
        json_files = glob.glob(str(source_path / "*.json"))

        if not json_files:
            raise ValueError(f"[bright_red][bold]No matching files[/bold] found in the path [bold]{source_path}[/bold] specified in Config {config_id}.[/bright_red]")
        print(f"Found {len(json_files)} JSON files")

        for json_file in json_files:
            try:
                file_path = Path(json_file)
                print(f"\nProcessing {file_path.name}...")

                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                df = try_all_orients(json_data)

                if df is not None:
                    print("Successfully converted using standard pandas methods")
                else:
                    print("Standard conversion failed, using advanced processing...")

                    if not isinstance(json_data, list):
                        if isinstance(json, dict):
                            if all(isinstance(v, list) for v in json_data.values()):
                                df = pd.DataFrame(json_data)
                            else:
                                json_data = [json_data]
                        else:
                            raise ValueError(f"[bright_red]Error parsing JSON structure: Unsupported JSON structure in [bold]{file_path.name}[/bold][/bright_red]")
                    
                # Process data
                if chunk_size is not None:
                    total_records = len(json_data)
                    for i in range(0, total_records, chunk_size):
                        chunk = json_data[i:i + chunk_size]
                        normalized_data = normalize_json_structure(chunk)
                        df = pd.DataFrame(normalized_data)

                        # Handle mixed types
                        for col in df.columns:
                            if df[col].dtype == object:
                                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                        
                        # Save chunk
                        parquet_filename = f"{file_path.stem}_chunk_{i//chunk_size}.parquet"
                        parquet_path = output_path / parquet_filename
                        df.to_parquet(parquet_path, index=False)
                        print(f"Processed chunk {i//chunk_size + 1} ({len(chunk)} records)")
                else:
                    # Process entire file at once
                    normalized_data = normalize_json_structure(json_data)
                    df = pd.DataFrame(normalized_data)

                    # Handle mixed types
                    for col in df.columns:
                        if df[col].dtype == object:
                            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                    
                    parquet_filename = f"{file_path.stem}.parquet"
                    parquet_path = output_path / parquet_filename
                    df.to_parquet(parquet_path, index=False)
                    print(f"Processed {len(df)} records")
                    
                print(f"Successfully converted {file_path.name} to parquet")
            except Exception as e:
                raise ValueError(f"[bright_red]Error processing [bold]{file_path.name}: {str(e)}[/bold].[/bright_red]")
        
        print("\nConversion completed!")
    
    except Exception as e:
        raise ValueError(f"[bright_red]Error: [bold]{str(e)}[/bold][/bright_red]")

if __name__ == "__main__":
    console = Console()
    try:
        config_data = read_sql_table(
            server_name='server_name',
            database_name='database_name',
            sql_table_name='sql_table_name'
        )
        
        # Display the first few rows
        # print(config_data.head())
        for index, row in config_data.iterrows():
            # print(row['file_path'])
            # print(row['target_path'])
            # print(row)
            print(f"Processing Config {row['ConfigID']}...")

            if row['is_active']:
                file_type = row['file_type'].lower()
                config_id = row['ConfigID']
                if file_type not in ['csv', 'json', 'sql']:
                    raise ValueError(f"[bright_red]Unsupported file format: [bold]{row['file_type'].lower()}[/bold] for Config {config_id}.[/bright_red]")
                if file_type == 'sql':
                    if row['file_path'] is not None: 
                        raise ValueError(f"[bright_red][bold]File path[/bold] value should be [bold]NULL[/bold] in the configuration table for SQL Database for Config [bold]{row['ConfigID']}[/bold] and also make sure it is not an empty string.")
                    elif row['source_database'] is None or row['source_table'] is None:
                        raise ValueError(f"[bright_red]Either the [bold]database name[/bold] or the [bold]table name[/bold] or both is missing for Config [bold]{row['ConfigID']}[/bold].[/bright_red]")
                    else:
                        try:
                            table_name = row['source_table'] if row['source_schema'] is None else row['source_schema'] + "." + row['source_table']
                            sql_to_parquet(
                                server_name='DESKTOP-GGC1FHH\SQLEXPRESS',
                                database_name=row['source_database'],
                                sql_table_name=table_name,
                                output_path=row['target_path']
                            )
                        except Exception as e:
                            raise ValueError(f"Error: {str(e)}")
                if file_type in ['csv', 'json']:
                    if row['file_path'] is None:
                        raise ValueError(f"[bright_red][bold]Missing file path[/bold] value in the configuration table for Config {config_id}. Did you forget to create it?[/bright_red]")
                    if not os.path.exists(row['file_path']):
                        output_file_path = row['file_path'] + "\\"
                        raise ValueError(f"[bright_red]Directory [bold]{output_file_path}[/bold] does not exist for Config {row['ConfigID']}.[/bright_red]")
                    if row['target_path'] is None:
                        raise ValueError(f"[bright_red][bold]Missing output directory path[/bold] value for Config {config_id} in the configuration table, did you forget to create it?[/bright_red]")
                    # if not os.path.exists(row['target_path']):
                    #     output_file_path = row['target_path'] + "\\"
                    #     raise ValueError(f"[bright_red] Output directory [bold]{output_file_path}[/bold] does not exist for Config {row['ConfigID']}.[/bright_red]")
                if file_type == 'csv':
                    csv_to_parquet(source_dir=row['file_path'], output_dir=row['target_path'])
                if file_type == 'json':
                    json_to_parquet(source_dir=row['file_path'], output_dir=row['target_path'])
            else:
                print(f"Skipping Config {row['ConfigID']} due to the is_active setting...")
        print("Complete all ingestions in the configuration table!")
    except Exception as e:
        console.print(f"Failed to read configuration due to:\n {str(e)}")
        # print(e)
