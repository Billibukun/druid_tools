import psycopg2
import csv
import os
import time
from datetime import datetime, timedelta
from psycopg2 import sql
import pandas as pd


# Function to get user-defined date
def get_target_date():
    date_str = input("Enter the date (YYYY-MM-DD): ")
    try:
        # Convert the input string to a date object
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return target_date
    except ValueError:
        print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
        return get_target_date()


# Function to get user-defined date range
def get_date_range():
    while True:
        start_date_str = input("Enter the start date (YYYY-MM-DD): ")
        end_date_str = input("Enter the end date (YYYY-MM-DD): ")
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            if start_date <= end_date:
                return start_date, end_date
            else:
                print("Invalid date range. Start date must be before or equal to end date.")
        except ValueError:
            print("Invalid date format. Please enter dates in YYYY-MM-DD format.")


# Function to connect to the database
def connect_to_db():
    return psycopg2.connect(
        database=DB_BNAME,
        host=HOSTNAME,
        user= USER,
        password=PASSWORD,
        port= PORT   
    )


def extract_full_data(conn, table_name, chunk_size=1000, start_offset=0, output_file=None):
    """
    Extracts full data from a PostgreSQL table in batches, with a restart mechanism.
    Args:
        conn: psycopg2 connection object.
        table_name (str): Name of the table to extract data from.
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
        start_offset (int): Starting row offset for initial extraction (default: 0).
        output_file (str): Name of the output CSV file (default: None, uses table name).
    """
    output_file = output_file or f'{table_name}_data.csv'
    query = sql.SQL("SELECT * FROM {} ORDER BY (SELECT NULL) OFFSET %s LIMIT %s").format(sql.Identifier(table_name))
    
    try:
        with conn.cursor() as cursor:
            # Check if file exists and get last processed offset
            if os.path.exists(output_file):
                with open(output_file, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = sum(1 for _ in reader) - 1  # Subtract 1 for header
                    start_offset = max(start_offset, rows)
                print(f"Resuming from offset {start_offset}")
                file_mode = 'a'  # Append to existing file
            else:
                file_mode = 'w'  # Create a new file

            # Write or append to CSV
            with open(output_file, file_mode, newline='') as csvfile:
                writer = csv.writer(csvfile)

                # Write header if it's a new file
                if file_mode == 'w':
                    cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 1").format(sql.Identifier(table_name)))
                    writer.writerow([desc[0] for desc in cursor.description])

                offset = start_offset
                while True:
                    start_time = time.time()
                    cursor.execute(query, (offset, chunk_size))
                    chunk = cursor.fetchall()
                    if not chunk:
                        break
                    writer.writerows(chunk)
                    end_time = time.time()
                    batch_time = end_time - start_time
                    print(f"Processed {len(chunk)} rows (total: {offset + len(chunk)}) in {batch_time:.2f} seconds")
                    offset += len(chunk)
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    else:
        conn.commit()
        print(f"Data extraction completed. Output saved to {output_file}")


def extract_daily_data(conn, table_name, target_date, column="Date_Registerred", chunk_size=1000):
    """
    Extracts data for a specific date from a PostgreSQL table in batches.

    Args:
        conn: psycopg2 connection object.
        table_name (str): Name of the table to extract data from.
        column (str): Name of the date column (default: "Date_Registerred").
        target_date (date): Date for which to extract data.
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
    """
    query = f'SELECT * FROM {table_name} WHERE "{column}" >= %s AND "{column}" < %s OFFSET %s LIMIT %s'
    start_time = time.time()

    try:
        with conn.cursor() as cursor:
            output_file = f'{table_name}_{target_date.strftime("%Y-%m-%d")}.csv'
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                writer.writerow([desc[0] for desc in cursor.description])

                offset = 0
                while True:
                    cursor.execute(query, (target_date, target_date + timedelta(days=1), offset, chunk_size))
                    chunk = cursor.fetchall()
                    if not chunk:
                        break

                    writer.writerows(chunk)
                    offset += chunk_size

        end_time = time.time()
        extraction_time = end_time - start_time
        print(f"Extracted data for {target_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()


def extract_daily_data(conn, table_name, target_date, output_folder="data", column="Date_Registerred", chunk_size=1000):
    """
    Extracts data for a specific date from a PostgreSQL table in batches.

    Args:
        conn: psycopg2 connection object.
        table_name (str): Name of the table to extract data from.
        column (str): Name of the date column (default: "Date_Registerred").
        target_date (str): Date for which to extract data (YYYY-MM-DD).
        output_folder (str): Name of the desired output folder (default: "data").
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
    """
    try:
        # Convert target_date string to datetime.date object
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()

        # Define the SQL query
        query = f'SELECT * FROM {table_name} WHERE "{column}" >= %s AND "{column}" < %s OFFSET %s LIMIT %s'
        start_time = time.time()

        with conn.cursor() as cursor:
            # Create output CSV file
            output_file = f'{output_folder}/{table_name}_{target_date_obj.strftime("%Y-%m-%d")}.csv'
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)

                # Write header
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                writer.writerow([desc[0] for desc in cursor.description])

                offset = 0
                while True:
                    # Extract data in chunks
                    cursor.execute(query, (target_date_obj, target_date_obj + timedelta(days=1), offset, chunk_size))
                    chunk = cursor.fetchall()
                    if not chunk:
                        break

                    # Write data to CSV
                    writer.writerows(chunk)
                    offset += chunk_size

        end_time = time.time()
        extraction_time = end_time - start_time
        print(f"Extracted data for {target_date_obj.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()


# Extract data for a specific date range
def extract_data_in_date_range(conn, table_name,  start_date, end_date, output_folder="data", column="Date_Registerred", chunk_size=1000):
    """
    Extracts data within a specified date range from a PostgreSQL table in batches.

    Args:
        conn: psycopg2 connection object.
        table_name (str): Name of the table to extract data from.
        start_date (str): Start date for data extraction in YYYY-MM-DD format.
        end_date (str): End date for data extraction in YYYY-MM-DD format.
        output_folder (str): Name of the desired output folder (default: "data").
        column (str): Name of the date column (default: "Date_Registerred").
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
    """
    # Convert start_date and end_date strings to datetime.date objects
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Error: {e}")
        return

    # SQL query with placeholders
    query = f"""
    SELECT * FROM {table_name} 
    WHERE "{column}" >= %s AND "{column}" < %s OFFSET %s LIMIT %s
    """

    start_time = time.time()

    with conn.cursor() as cursor:
        with open(f'{output_folder}/{table_name}_{start_date}_to_{end_date}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            writer.writerow([desc[0] for desc in cursor.description])

            offset = 0
            while True:
                cursor.execute(query, (start_date, end_date + timedelta(days=1), offset, chunk_size))
                chunk = cursor.fetchall()
                if not chunk:
                    break

                writer.writerows(chunk)
                offset += chunk_size

    end_time = time.time()
    extraction_time = end_time - start_time
    print(f"Extracted data for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")


# Daily extraction with person data
def extract_person_data(conn, target_date, chunk_size=1000):
    # Step 1: Fetch child, mother, father, informant IDs from birth_registration
    query_ids = """
    SELECT child, mother, father, informant 
    FROM birth_registration
    WHERE "Date_Registerred" >= %s AND "Date_Registerred" < %s
    OFFSET %s LIMIT %s;
    """
    try:
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Error: {e}")
        return
        
    start_time = time.time()
    
    with conn.cursor() as cursor:
        # Create a CSV file to store the extracted person data
        with open(f'persons_data/person_data_{target_date.strftime("%Y-%m-%d")}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write the header row for person data
            cursor.execute('SELECT * FROM person LIMIT 1')
            writer.writerow([desc[0] for desc in cursor.description])

            offset = 0
            while True:
                # Fetch the birth registration data (child, mother, father, informant IDs)
                cursor.execute(query_ids, (target_date, target_date + timedelta(days=1), offset, chunk_size))
                id_chunks = cursor.fetchall()
                
                if not id_chunks:
                    break
                
                # Step 2: Extract all unique IDs for persons (child, mother, father, informant)
                unique_ids = set()
                for row in id_chunks:
                    unique_ids.update(row)
                
                # Skip if no unique IDs are found
                if not unique_ids:
                    print(f"No records found for the date {target_date.strftime('%Y-%m-%d')}")
                    return
                
                # Step 3: Fetch the person data for all IDs from the person table
                id_placeholders = ', '.join(['%s'] * len(unique_ids))
                query_persons = f"""
                SELECT * 
                FROM person 
                WHERE id IN ({id_placeholders});
                """
                
                # Execute query for person data
                cursor.execute(query_persons, tuple(unique_ids))
                person_data = cursor.fetchall()
                
                # Write person data to CSV
                writer.writerows(person_data)
                
                # Increment offset for pagination
                offset += chunk_size
    
    end_time = time.time()
    extraction_time = end_time - start_time
    print(f"Extracted person data for {target_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")



def extract_person_data_in_date_range(conn, start_date, end_date, chunk_size=1000):
    """
    Extracts person data from the birth_registration table for a range of dates.
    
    Args:
        conn: psycopg2 connection object.
        start_date (str): Start date for data extraction in YYYY-MM-DD format.
        end_date (str): End date for data extraction in YYYY-MM-DD format.
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
    """
    # Convert start_date and end_date strings to datetime.date objects
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Error: {e}")
        return
    
    query_ids = """
    SELECT child, mother, father, informant 
    FROM birth_registration
    WHERE "Date_Registerred" >= %s AND "Date_Registerred" < %s
    OFFSET %s LIMIT %s;
    """
    
    current_date = start_date
    while current_date <= end_date:
        start_time = time.time()
        
        with conn.cursor() as cursor:
            # Create a CSV file for each day in the date range
            with open(f'persons_data/person_data_{current_date.strftime("%Y-%m-%d")}.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write the header row for person data
                cursor.execute('SELECT * FROM person LIMIT 1')
                writer.writerow([desc[0] for desc in cursor.description])

                offset = 0
                while True:
                    # Fetch the birth registration data (child, mother, father, informant IDs)
                    cursor.execute(query_ids, (current_date, current_date + timedelta(days=1), offset, chunk_size))
                    id_chunks = cursor.fetchall()

                    if not id_chunks:
                        break

                    # Step 2: Extract all unique IDs for persons (child, mother, father, informant)
                    unique_ids = set()
                    for row in id_chunks:
                        unique_ids.update(row)

                    # Skip if no unique IDs are found
                    if not unique_ids:
                        print(f"No records found for the date {current_date.strftime('%Y-%m-%d')}")
                        break

                    # Step 3: Fetch the person data for all IDs from the person table
                    id_placeholders = ', '.join(['%s'] * len(unique_ids))
                    query_persons = f"""
                    SELECT * 
                    FROM person 
                    WHERE id IN ({id_placeholders});
                    """

                    # Execute query for person data
                    cursor.execute(query_persons, tuple(unique_ids))
                    person_data = cursor.fetchall()

                    # Write person data to CSV
                    writer.writerows(person_data)

                    # Increment offset for pagination
                    offset += chunk_size

        end_time = time.time()
        extraction_time = end_time - start_time
        print(f"Extracted person data for {current_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")
        
        # Move to the next date
        current_date += timedelta(days=1)


def extract_full_table_data(conn, table_name, output_file=None, chunk_size=1000, where_clause=None, order_by=None):
    """
    Extracts all data from a PostgreSQL table with optional WHERE and ORDER BY clauses, and saves it to a CSV file.

    Args:
        conn: psycopg2 connection object.
        table_name (str): Name of the table to extract data from.
        output_file (str): Name of the output CSV file (default: None, uses table name).
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
        where_clause (str): Optional WHERE clause to filter data (default: None).
        order_by (str): Optional ORDER BY clause (default: None).
    """
    output_file = output_file or f'{table_name}_data.csv'
    
    # Construct the query with optional clauses
    base_query = f"SELECT * FROM {table_name}"
    if where_clause:
        base_query += f" WHERE {where_clause}"
    if order_by:
        base_query += f" ORDER BY {order_by}"
    
    base_query += " OFFSET %s LIMIT %s"
    
    try:
        with conn.cursor() as cursor:
            # Write to CSV file
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)

                # Write header
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                writer.writerow([desc[0] for desc in cursor.description])

                offset = 0
                while True:
                    # Execute query with pagination
                    cursor.execute(base_query, (offset, chunk_size))
                    chunk = cursor.fetchall()
                    if not chunk:
                        break

                    writer.writerows(chunk)
                    offset += len(chunk)

        print(f"Data extraction completed. Output saved to {output_file}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    else:
        conn.commit()


def load_person_data(folder, start_date, end_date):
    dataframes = []
    date_range = pd.date_range(start=start_date, end=end_date)

    for single_date in date_range:
        file_path = os.path.join(folder, f'person_data_{single_date.strftime("%Y-%m-%d")}.csv')
        if os.path.exists(file_path):
            print(f"Loading {file_path}...")
            try:
                # Try reading the file with 'utf-8' encoding first
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                print(f"Encoding error for {file_path}, trying ISO-8859-1 encoding...")
                # If there's an encoding error, try 'ISO-8859-1' encoding
                df = pd.read_csv(file_path, encoding='ISO-8859-1')
            dataframes.append(df)
        else:
            print(f"File not found: {file_path}")
    
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        print("No files found in the specified date range.")
        return pd.DataFrame()


# Function to merge birth registration data with person data
def merge_birth_and_person_data(birth_file, person_folder, start_date, end_date, birth_col_child='child', birth_col_mother='mother', birth_col_father='father', person_col='id'):
    """
    Merges birth registration data with person data (for child, mother, father) from a specific date range.
    
    Args:
        birth_file (str): Path to the birth registration CSV file.
        person_folder (str): Path to the folder where person data CSV files are stored.
        start_date (str): Start date for loading person data in 'YYYY-MM-DD' format.
        end_date (str): End date for loading person data in 'YYYY-MM-DD' format.
        birth_col_child (str): Column name for child ID in the birth data.
        birth_col_mother (str): Column name for mother ID in the birth data.
        birth_col_father (str): Column name for father ID in the birth data.
        person_col (str): Column name for person ID in the person data (default is 'id').
    
    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    # Step 1: Load the birth registration data
    birth_registration_df = pd.read_csv(birth_file)
    print(f"Loaded birth registration data from {birth_file}")

    # Step 2: Load the person data for the specified date range
    person_data_df = load_person_data(person_folder, start_date, end_date)
    
    if person_data_df.empty:
        print("No person data found for the specified date range.")
        return pd.DataFrame()

    # Step 3: Merge for child information
    merged_df = birth_registration_df.merge(
        person_data_df,
        how='left',  # Use left join to keep all birth registration records
        left_on=birth_col_child,  # Match child IDs
        right_on=person_col,  # Match with person ID
        suffixes=('', '_child')  # Add suffix to avoid column name collision
    )

    # Step 4: Merge for mother information
    merged_df = merged_df.merge(
        person_data_df,
        how='left',
        left_on=birth_col_mother,
        right_on=person_col,
        suffixes=('', '_mother')  # Differentiate mother data
    )

    # Step 5: Merge for father information
    merged_df = merged_df.merge(
        person_data_df,
        how='left',
        left_on=birth_col_father,
        right_on=person_col,
        suffixes=('', '_father')  # Differentiate father data
    )

    # Step 6: Drop duplicate rows based on a unique column (if applicable)
    merged_df.drop_duplicates(subset='Certificate_No', inplace=True)

    # Return the merged DataFrame
    return merged_df


def load_person_data_with_chunks(folder, start_date, end_date, chunksize=100000):
    """
    Loads person data between the specified date range using chunks to handle large datasets.

    Args:
        folder (str): Path to the folder where person data CSV files are stored.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        chunksize (int): Number of rows per chunk (default is 100,000).
    
    Returns:
        pd.DataFrame: Concatenated DataFrame with all person data in the date range.
    """
    dataframes = []
    date_range = pd.date_range(start=start_date, end=end_date)

    for single_date in date_range:
        file_path = os.path.join(folder, f'person_data_{single_date.strftime("%Y-%m-%d")}.csv')
        if os.path.exists(file_path):
            print(f"Loading {file_path} in chunks...")
            for chunk in pd.read_csv(file_path, chunksize=chunksize, dtype=str):
                dataframes.append(chunk)
        else:
            print(f"File not found: {file_path}")

    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        print("No files found in the specified date range.")
        return pd.DataFrame()


def merge_birth_and_person_data(birth_file, person_folder, start_date, end_date, birth_col_child='child', birth_col_mother='mother', birth_col_father='father', person_col='id', chunksize=100000):
    """
    Merges birth registration data with person data (for child, mother, father) from a specific date range,
    using chunks to handle large datasets.

    Args:
        birth_file (str): Path to the birth registration CSV file.
        person_folder (str): Path to the folder where person data CSV files are stored.
        start_date (str): Start date for loading person data in 'YYYY-MM-DD' format.
        end_date (str): End date for loading person data in 'YYYY-MM-DD' format.
        birth_col_child (str): Column name for child ID in the birth data.
        birth_col_mother (str): Column name for mother ID in the birth data.
        birth_col_father (str): Column name for father ID in the birth data.
        person_col (str): Column name for person ID in the person data (default is 'id').
        chunksize (int): Number of rows per chunk (default is 100,000).
    
    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    # Step 1: Load the birth registration data
    birth_registration_df = pd.read_csv(birth_file, dtype=str)
    print(f"Loaded birth registration data from {birth_file}")

    # Step 2: Load the person data for the specified date range with chunking
    person_data_df = load_person_data_with_chunks(person_folder, start_date, end_date, chunksize=chunksize)
    
    if person_data_df.empty:
        print("No person data found for the specified date range.")
        return pd.DataFrame()

    # Step 3: Merge for child information
    merged_df = pd.merge(
        birth_registration_df,
        person_data_df,
        how='left',  # Use left join to keep all birth registration records
        left_on=birth_col_child,  # Match child IDs
        right_on=person_col,  # Match with person ID
        suffixes=('', '_child'),  # Add suffix to avoid column name collision
        sort=False  # Avoid sorting to reduce memory usage
    )

    # Step 4: Merge for mother information
    merged_df = pd.merge(
        merged_df,
        person_data_df,
        how='left',
        left_on=birth_col_mother,
        right_on=person_col,
        suffixes=('', '_mother'),
        sort=False
    )

    # Step 5: Merge for father information
    merged_df = pd.merge(
        merged_df,
        person_data_df,
        how='left',
        left_on=birth_col_father,
        right_on=person_col,
        suffixes=('', '_father'),
        sort=False
    )

    # Step 6: Drop duplicate rows based on a unique column (if applicable)
    merged_df.drop_duplicates(subset='Certificate_No', inplace=True)

    return merged_df
