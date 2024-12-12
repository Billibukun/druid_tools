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
        database='',
        host='',
        user='',
        password='',
        port=''   
    )


def extract_full_birth_data(conn, output_folder="data", chunk_size=1000):
    """
    Extracts specific columns from the full birth registration table without duplicates.
    Includes resume capability to continue from last processed ID.
    
    Args:
        conn: psycopg2 connection object
        output_folder (str): Output folder for the CSV file (default: "data")
        chunk_size (int): Number of rows to fetch per batch (default: 1000)
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    csv_file_path = f'{output_folder}/full_birth_data.csv'
    last_id = 0
    file_mode = 'w'
    
    # Check if file exists and get last processed ID
    if os.path.exists(csv_file_path):
        try:
            with open(csv_file_path, 'rb') as f:
                try:
                    f.seek(0, 2)
                    file_size = f.tell()
                    chunk_size_bytes = min(10000, file_size)
                    f.seek(file_size - chunk_size_bytes)
                    last_chunk = f.read().decode('latin1')
                    
                    lines = last_chunk.split('\n')
                    for line in reversed(lines):
                        if line.strip():
                            try:
                                last_id = int(line.split(',')[0])
                                file_mode = 'a'
                                print(f"Found last Birth_Reg_ID: {last_id}")
                                break
                            except (ValueError, IndexError):
                                continue
                except Exception as e:
                    print(f"Error reading file end: {e}")
        except Exception as e:
            print(f"Error handling existing file: {e}")
    
    if file_mode == 'a':
        print(f"Resuming extraction from Birth_Reg_ID: {last_id}")
    else:
        print("Starting new extraction")
        
    # Query with specific columns and duplicate prevention
    query = """
        WITH ranked_records AS (
            SELECT DISTINCT ON ("Birth_Reg_ID")
                "Birth_Reg_ID",
                "father",
                "mother",
                "informant",
                "child",
                "mother_age_at_birth",
                "father_age_at_birth",
                "mother_marital_status",
                "father_marital_status",
                "Reg_Center",
                "Certificate_No",
                "birth_place",
                "Birth_type",
                "birth_order",
                "locality_of_birth",
                "Date_Registerred",
                "Registered_By", "Informant_Relationship_ID", "Date_Modified", 
                "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                "Print_Status", "Date_Printed", "Printed_by", "nin",
                "reference", "nin_status", "checked", "initiated_at"
            FROM birth_registration
            WHERE "Birth_Reg_ID" > %s
            ORDER BY "Birth_Reg_ID"
            LIMIT %s
        )
        SELECT * FROM ranked_records
        ORDER BY "Birth_Reg_ID"
    """
    
    start_time = time.time()
    total_rows = 0
    processed_ids = set()
    
    try:
        with conn.cursor() as cursor:
            with open(csv_file_path, file_mode, newline='', encoding='utf-8', errors='replace') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header only in write mode
                if file_mode == 'w':
                    headers = [
                        "Birth_Reg_ID", "father", "mother", "informant", "child",
                        "mother_age_at_birth", "father_age_at_birth", "mother_marital_status",
                        "father_marital_status", "Reg_Center", "Certificate_No", "birth_place",
                        "Birth_type", "birth_order", "locality_of_birth", "Date_Registerred",
                        "Registered_By", "Informant_Relationship_ID", "Date_Modified", 
                        "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                        "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                        "Print_Status", "Date_Printed", "Printed_by", "nin",
                        "reference", "nin_status", "checked", "initiated_at"
                    ]
                    writer.writerow(headers)
                
                while True:
                    try:
                        cursor.execute(query, (last_id, chunk_size))
                        rows = cursor.fetchall()
                        
                        if not rows:
                            break
                            
                        # Filter out any duplicates
                        new_rows = []
                        for row in rows:
                            birth_reg_id = row[0]
                            if birth_reg_id not in processed_ids:
                                # Clean string values
                                cleaned_row = []
                                for item in row:
                                    if isinstance(item, str):
                                        item = item.encode('utf-8', 'replace').decode('utf-8')
                                    cleaned_row.append(item)
                                processed_ids.add(birth_reg_id)
                                new_rows.append(cleaned_row)
                        
                        if not new_rows:
                            break
                        
                        writer.writerows(new_rows)
                        last_id = new_rows[-1][0]
                        total_rows += len(new_rows)
                        
                        print(f"Processed {total_rows:,} unique records (Last Birth_Reg_ID: {last_id})...", end='\r')
                        
                        # Flush periodically
                        if total_rows % (chunk_size * 10) == 0:
                            csvfile.flush()
                            os.fsync(csvfile.fileno())
                        
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        print(f"\nDatabase connection lost: {e}")
                        print("Attempting to reconnect...")
                        conn = connect_to_db()
                        cursor = conn.cursor()
                        continue
                    
        end_time = time.time()
        print(f"\nExtracted {total_rows:,} unique records")
        print(f"Processing time: {end_time - start_time:.2f} seconds")
        print(f"Output saved to: {csv_file_path}")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print(f"Last successful Birth_Reg_ID: {last_id}")
        print("You can resume from this point later.")
        raise
    finally:
        conn.commit()
        
    return csv_file_path


def extract_birth_with_person_details(conn, output_folder="merged_data", chunk_size=1000):
    """
    Extracts birth registration data with exact case matching from schema.
    Includes resume capability and proper UTF-8 encoding.
    """
    import codecs
    import os
    import csv
    import time
    import psycopg2
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    csv_file_path = f'{output_folder}/birth_with_person_details.csv'
    last_id = 0
    file_mode = 'w'
    
    # File handling for resume capability
    if os.path.exists(csv_file_path):
        try:
            with codecs.open(csv_file_path, 'r', encoding='utf-8-sig') as f:
                try:
                    f.seek(0, 2)
                    file_size = f.tell()
                    chunk_size_bytes = min(10000, file_size)
                    f.seek(max(0, file_size - chunk_size_bytes))
                    last_chunk = f.read()
                    
                    lines = last_chunk.split('\n')
                    for line in reversed(lines):
                        if line.strip():
                            try:
                                last_id = int(line.split(',')[0])
                                file_mode = 'a'
                                print(f"Found last Birth_Reg_ID: {last_id}")
                                break
                            except (ValueError, IndexError):
                                continue
                except Exception as e:
                    print(f"Error reading file end: {e}")
        except Exception as e:
            print(f"Error handling existing file: {e}")

    # Query with exact schema case matching
    query = """
        WITH ranked_records AS (
            SELECT DISTINCT ON (br."Birth_Reg_ID")
                -- Birth Registration Core Fields
                br."Birth_Reg_ID",
                br."Certificate_No",
                bp."BirthPlace_Desc" as birth_place_desc,
                bt."Description" as birth_type_desc,
                bo."birth_order" as birth_order_number,
                bo."Desc" as birth_order_desc,
                lb."Description" as locality_desc,
                br."Date_Registerred",
                br."mother_age_at_birth",
                br."father_age_at_birth",
                ms_mother."Status_Desc" as mother_marital_status_desc,
                ms_father."Status_Desc" as father_marital_status_desc,
                
                -- Child Details
                child."id" as child_id,
                child."surname" as child_surname,
                child."firstname" as child_firstname,
                child."middle_name" as child_middle_name,
                child."birth_date" as child_birth_date,
                br."nin" as child_nin,
                br."nin_status" as child_nin_status,
                g_child."gender" as child_sex,
                c_child."Country_Name" as child_birth_country,
                s_child."State_Name" as child_birth_state,
                l_child."LGA_Name" as child_birth_lga,
                child."town_of_birth" as child_town_of_birth,
                child."town_of_origin" as child_town_of_origin,
                e_child."Ethnic_Grp_Name" as child_ethnic_group,
                
                -- Mother Details
                mother."id" as mother_id,
                mother."surname" as mother_surname,
                mother."firstname" as mother_firstname,
                mother."middle_name" as mother_middle_name,
                mother."maiden_name" as mother_maiden_name,
                mother."birth_date" as mother_birth_date,
                mother."nin" as mother_nin,
                mother."no_nin_reason" as mother_no_nin_reason,
                e_mother."Ethnic_Grp_Name" as mother_ethnic_group,
                c_mother."Country_Name" as mother_nationality,
                c_mother_res."Country_Name" as mother_residence_country,
                s_mother."State_Name" as mother_residence_state,
                l_mother."LGA_Name" as mother_residence_lga,
                ot_mother."Occupation" as mother_occupation,
                mother."current_phone_number" as mother_phone,
                mother."current_email" as mother_email,
                mother."current_address" as mother_address,
                
                -- Father Details
                father."id" as father_id,
                father."surname" as father_surname,
                father."firstname" as father_firstname,
                father."middle_name" as father_middle_name,
                father."birth_date" as father_birth_date,
                father."nin" as father_nin,
                father."no_nin_reason" as father_no_nin_reason,
                e_father."Ethnic_Grp_Name" as father_ethnic_group,
                c_father."Country_Name" as father_nationality,
                c_father_res."Country_Name" as father_residence_country,
                s_father."State_Name" as father_residence_state,
                l_father."LGA_Name" as father_residence_lga,
                ot_father."Occupation" as father_occupation,
                father."current_phone_number" as father_phone,
                father."current_email" as father_email,
                father."current_address" as father_address,
                
                -- Informant Details
                informant."id" as informant_id,
                informant."surname" as informant_surname,
                informant."firstname" as informant_firstname,
                informant."middle_name" as informant_middle_name,
                informant."nin" as informant_nin,
                informant."current_phone_number" as informant_phone,
                informant."current_email" as informant_email,
                informant."current_address" as informant_address,
                r."Description" as informant_relationship,
                br."informant_signature",
                
                -- Registration Center Details
                rc."Reg_Center_Name" as registration_center,
                s_rc."State_Name" as registration_center_state,
                l_rc."LGA_Name" as registration_center_lga,
                l_rc."LGA_Code" as registration_center_lga_code,
                gz."Geo_Zone_Name" as registration_center_geo_zone,
                
                -- Registration Officers Details
                -- Registered By
                u_reg."UserName" as registered_by_user,
                u_reg."Email" as registered_by_email,
                u_reg."Phone_No" as registered_by_phone,
                u_reg."nin" as registered_by_nin,
                r_reg."name" as registered_by_role,
                
                -- Modified By
                u_mod."UserName" as modified_by_user,
                u_mod."Email" as modified_by_email,
                u_mod."Phone_No" as modified_by_phone,
                u_mod."nin" as modified_by_nin,
                r_mod."name" as modified_by_role,
                br."Date_Modified",
                
                -- Approved By
                u_app."UserName" as approved_by_user,
                u_app."Email" as approved_by_email,
                u_app."Phone_No" as approved_by_phone,
                u_app."nin" as approved_by_nin,
                r_app."name" as approved_by_role,
                br."Approval_Status",
                br."Date_Approved",
                
                -- Printed By
                u_print."UserName" as printed_by_user,
                u_print."Email" as printed_by_email,
                u_print."Phone_No" as printed_by_phone,
                u_print."nin" as printed_by_nin,
                r_print."name" as printed_by_role,
                br."Print_Status",
                br."Date_Printed",
                
                -- Additional Birth Registration Fields
                br."shared" as br_shared,
                br."shared_by" as br_shared_by,
                br."Modified_Status",
                br."Modified_Print",
                br."initiated_at",
                CASE 
                    WHEN br."Approval_Status" = 1 THEN 'Approved'
                    WHEN br."Approval_Status" = 2 THEN 'Rejected'
                    ELSE 'Pending'
                END as approval_status_desc,
                CASE 
                    WHEN br."Print_Status" = 1 THEN 'Printed'
                    ELSE 'Not Printed'
                END as print_status_desc
                
            FROM birth_registration br
            -- Person Joins
            LEFT JOIN person child ON br."child" = child."id"
            LEFT JOIN person mother ON br."mother" = mother."id"
            LEFT JOIN person father ON br."father" = father."id"
            LEFT JOIN person informant ON br."informant" = informant."id"
            
            -- Registration Center Details
            LEFT JOIN registration_center rc ON br."Reg_Center" = rc."Reg_Center_id"
            LEFT JOIN lga l_rc ON rc."LGA_ID" = l_rc."LGA_ID"
            LEFT JOIN states s_rc ON l_rc."State_ID" = s_rc."State_ID"
            LEFT JOIN geo_zone gz ON s_rc."Geo_Zone_ID" = gz."Geo_Zone_id"
            
            -- Registration Officers
            LEFT JOIN users u_reg ON br."Registered_By" = u_reg."User_ID"
            LEFT JOIN roles r_reg ON u_reg."Role_ID" = r_reg."id"
            
            LEFT JOIN users u_mod ON br."Modified_By" = u_mod."User_ID"
            LEFT JOIN roles r_mod ON u_mod."Role_ID" = r_mod."id"
            
            LEFT JOIN users u_app ON br."Approved_By" = u_app."User_ID"
            LEFT JOIN roles r_app ON u_app."Role_ID" = r_app."id"
            
            LEFT JOIN users u_print ON br."Printed_by" = u_print."User_ID"
            LEFT JOIN roles r_print ON u_print."Role_ID" = r_print."id"
            
            -- Other Lookups
            LEFT JOIN birth_place bp ON br."birth_place" = bp."BirthPlace_ID"
            LEFT JOIN birth_type bt ON br."Birth_type" = bt."Birth_Type_ID"
            LEFT JOIN birth_order bo ON br."birth_order" = bo."Birth_Order_ID"
            LEFT JOIN locality_of_birth lb ON br."locality_of_birth" = lb."Locality_ID"
            LEFT JOIN marital_status ms_mother ON br."mother_marital_status" = ms_mother."Marital_Status_ID"
            LEFT JOIN marital_status ms_father ON br."father_marital_status" = ms_father."Marital_Status_ID"
            LEFT JOIN gender g_child ON child."gender" = g_child."Gender_ID"
            LEFT JOIN country c_child ON child."birth_country" = c_child."Country_Code"
            LEFT JOIN states s_child ON child."birth_state" = s_child."State_ID"
            LEFT JOIN lga l_child ON child."birth_lga" = l_child."LGA_ID"
            LEFT JOIN ethnic_group e_child ON child."ethnic_group" = e_child."Ethnic_Grp_ID"
            LEFT JOIN ethnic_group e_mother ON mother."ethnic_group" = e_mother."Ethnic_Grp_ID"
            LEFT JOIN ethnic_group e_father ON father."ethnic_group" = e_father."Ethnic_Grp_ID"
            LEFT JOIN country c_mother ON mother."current_nationality" = c_mother."Country_Code"
            LEFT JOIN country c_mother_res ON mother."current_residence_country" = c_mother_res."Country_Code"
            LEFT JOIN states s_mother ON mother."current_residence_state" = s_mother."State_ID"
            LEFT JOIN lga l_mother ON mother."current_residence_lga" = l_mother."LGA_ID"
            LEFT JOIN occupation_tpe ot_mother ON mother."current_occupation" = ot_mother."Occupation_Type_ID"
            LEFT JOIN country c_father ON father."current_nationality" = c_father."Country_Code"
            LEFT JOIN country c_father_res ON father."current_residence_country" = c_father_res."Country_Code"
            LEFT JOIN states s_father ON father."current_residence_state" = s_father."State_ID"
            LEFT JOIN lga l_father ON father."current_residence_lga" = l_father."LGA_ID"
            LEFT JOIN occupation_tpe ot_father ON father."current_occupation" = ot_father."Occupation_Type_ID"
            LEFT JOIN relationship r ON br."Informant_Relationship_ID" = r."Relationship_ID"
            
            WHERE br."Birth_Reg_ID" > %s
            ORDER BY br."Birth_Reg_ID"
            LIMIT %s
        )
        SELECT * FROM ranked_records
        ORDER BY "Birth_Reg_ID";
    """
    # Complete list of headers matching the query fields
    headers = [
        # Birth Registration Core Fields
        "Birth_Reg_ID", "Certificate_No", "birth_place_desc", "birth_type_desc",
        "birth_order_number", "birth_order_desc", "locality_desc",
        "Date_Registerred", "mother_age_at_birth", "father_age_at_birth",
        "mother_marital_status_desc", "father_marital_status_desc",
        
        # Child Details
        "child_id", "child_surname", "child_firstname", "child_middle_name",
        "child_birth_date", "child_nin", "child_nin_status", "child_sex", 
        "child_birth_country", "child_birth_state", "child_birth_lga",
        "child_town_of_birth", "child_town_of_origin", "child_ethnic_group",
        
        # Mother Details
        "mother_id", "mother_surname", "mother_firstname", "mother_middle_name",
        "mother_maiden_name", "mother_birth_date", "mother_nin", 
        "mother_no_nin_reason", "mother_ethnic_group", "mother_nationality",
        "mother_residence_country", "mother_residence_state", "mother_residence_lga",
        "mother_occupation", "mother_phone", "mother_email", "mother_address",
        
        # Father Details
        "father_id", "father_surname", "father_firstname", "father_middle_name",
        "father_birth_date", "father_nin", "father_no_nin_reason",
        "father_ethnic_group", "father_nationality", "father_residence_country",
        "father_residence_state", "father_residence_lga", "father_occupation",
        "father_phone", "father_email", "father_address",
        
        # Informant Details
        "informant_id", "informant_surname", "informant_firstname",
        "informant_middle_name", "informant_nin", "informant_phone",
        "informant_email", "informant_address", "informant_relationship",
        "informant_signature",
        
        # Registration Center Details
        "registration_center", "registration_center_state", "registration_center_lga",
        "registration_center_lga_code", "registration_center_geo_zone",
        
        # Registration Officers Details
        # Registered By
        "registered_by_user", "registered_by_email", "registered_by_phone",
        "registered_by_nin", "registered_by_role",
        
        # Modified By
        "modified_by_user", "modified_by_email", "modified_by_phone",
        "modified_by_nin", "modified_by_role", "Date_Modified",
        
        # Approved By
        "approved_by_user", "approved_by_email", "approved_by_phone",
        "approved_by_nin", "approved_by_role", "Approval_Status", "Date_Approved",
        
        # Printed By
        "printed_by_user", "printed_by_email", "printed_by_phone",
        "printed_by_nin", "printed_by_role", "Print_Status", "Date_Printed",
        
        # Additional Fields
        "br_shared", "br_shared_by", "Modified_Status", "Modified_Print",
        "initiated_at", "approval_status_desc", "print_status_desc"
    ]
    
    start_time = time.time()
    total_rows = 0
    processed_ids = set()
    
    try:
        with conn.cursor() as cursor:
            # Open file with UTF-8-BOM encoding for Excel compatibility
            with codecs.open(csv_file_path, file_mode, encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL, lineterminator='\n')
                
                # Write header only in write mode
                if file_mode == 'w':
                    writer.writerow(headers)
                
                while True:
                    try:
                        cursor.execute(query, (last_id, chunk_size))
                        rows = cursor.fetchall()
                        
                        if not rows:
                            break
                            
                        # Filter out any duplicates and clean data
                        new_rows = []
                        for row in rows:
                            birth_reg_id = row[0]  # Birth_Reg_ID is first column
                            if birth_reg_id not in processed_ids:
                                # Clean string values
                                cleaned_row = []
                                for item in row:
                                    if isinstance(item, str):
                                        # Remove any problematic characters and normalize
                                        item = ''.join(char for char in item if ord(char) < 65536)
                                        item = item.replace('\x00', '').strip()
                                        # Handle special characters
                                        item = item.replace('\r', ' ').replace('\n', ' ')
                                        item = ' '.join(item.split())  # Normalize whitespace
                                    elif item is None:
                                        item = ''
                                    cleaned_row.append(item)
                                processed_ids.add(birth_reg_id)
                                new_rows.append(cleaned_row)
                        
                        if not new_rows:
                            break
                        
                        writer.writerows(new_rows)
                        last_id = new_rows[-1][0]
                        total_rows += len(new_rows)
                        
                        # Progress update with percentage
                        print(f"Processed {total_rows:,} unique records (Last Birth_Reg_ID: {last_id})...", end='\r')
                        
                        # Flush periodically to ensure data is written
                        if total_rows % (chunk_size * 10) == 0:
                            csvfile.flush()
                            try:
                                os.fsync(csvfile.fileno())
                            except AttributeError:
                                pass  # Some file objects might not support fsync
                            print(f"\nSaved {total_rows:,} records. Continuing...")
                        
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        print(f"\nDatabase connection lost: {e}")
                        print("Attempting to reconnect...")
                        time.sleep(5)  # Wait before reconnecting
                        conn = connect_to_db()  # Your database connection function
                        cursor = conn.cursor()
                        continue
                    except Exception as e:
                        print(f"\nError processing chunk: {e}")
                        print(f"Last successful Birth_Reg_ID: {last_id}")
                        raise
                    
        end_time = time.time()
        processing_time = end_time - start_time
        minutes = int(processing_time // 60)
        seconds = int(processing_time % 60)
        
        print(f"\nExtraction completed successfully!")
        print(f"Total records processed: {total_rows:,}")
        print(f"Processing time: {minutes} minutes and {seconds} seconds")
        print(f"Average speed: {total_rows / processing_time:.1f} records/second")
        print(f"Output saved to: {csv_file_path}")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print(f"Last successful Birth_Reg_ID: {last_id}")
        print("You can resume from this point later.")
        raise
    finally:
        try:
            conn.commit()
        except:
            pass
        
    return csv_file_path


import pandas as pd
import sqlite3
import psycopg2
import time
from datetime import datetime

def extract_births_to_sqlite(pg_conn, sqlite_path="databases/birth_records.db", chunk_size=10000):
    """
    Extracts birth registration data from PostgreSQL and saves to SQLite
    for faster subsequent reads.
    
    Args:
        pg_conn: PostgreSQL connection
        sqlite_path: Path for the SQLite database
        chunk_size: Number of records to process at once
    """
    # Your PostgreSQL query (the one we created earlier)
    query = """
        WITH ranked_records AS (
            SELECT DISTINCT ON (br."Birth_Reg_ID")
                -- Birth Registration Core Fields
                br."Birth_Reg_ID",
                br."Certificate_No",
                bp."BirthPlace_Desc" as birth_place_desc,
                bt."Description" as birth_type_desc,
                bo."birth_order" as birth_order_number,
                bo."Desc" as birth_order_desc,
                lb."Description" as locality_desc,
                br."Date_Registerred",
                br."mother_age_at_birth",
                br."father_age_at_birth",
                ms_mother."Status_Desc" as mother_marital_status_desc,
                ms_father."Status_Desc" as father_marital_status_desc,
                
                -- Child Details
                child."id" as child_id,
                child."surname" as child_surname,
                child."firstname" as child_firstname,
                child."middle_name" as child_middle_name,
                child."birth_date" as child_birth_date,
                br."nin" as child_nin,
                br."nin_status" as child_nin_status,
                g_child."gender" as child_sex,
                c_child."Country_Name" as child_birth_country,
                s_child."State_Name" as child_birth_state,
                l_child."LGA_Name" as child_birth_lga,
                child."town_of_birth" as child_town_of_birth,
                child."town_of_origin" as child_town_of_origin,
                e_child."Ethnic_Grp_Name" as child_ethnic_group,
                
                -- Mother Details
                mother."id" as mother_id,
                mother."surname" as mother_surname,
                mother."firstname" as mother_firstname,
                mother."middle_name" as mother_middle_name,
                mother."maiden_name" as mother_maiden_name,
                mother."birth_date" as mother_birth_date,
                mother."nin" as mother_nin,
                mother."no_nin_reason" as mother_no_nin_reason,
                e_mother."Ethnic_Grp_Name" as mother_ethnic_group,
                c_mother."Country_Name" as mother_nationality,
                c_mother_res."Country_Name" as mother_residence_country,
                s_mother."State_Name" as mother_residence_state,
                l_mother."LGA_Name" as mother_residence_lga,
                ot_mother."Occupation" as mother_occupation,
                mother."current_phone_number" as mother_phone,
                mother."current_email" as mother_email,
                mother."current_address" as mother_address,
                
                -- Father Details
                father."id" as father_id,
                father."surname" as father_surname,
                father."firstname" as father_firstname,
                father."middle_name" as father_middle_name,
                father."birth_date" as father_birth_date,
                father."nin" as father_nin,
                father."no_nin_reason" as father_no_nin_reason,
                e_father."Ethnic_Grp_Name" as father_ethnic_group,
                c_father."Country_Name" as father_nationality,
                c_father_res."Country_Name" as father_residence_country,
                s_father."State_Name" as father_residence_state,
                l_father."LGA_Name" as father_residence_lga,
                ot_father."Occupation" as father_occupation,
                father."current_phone_number" as father_phone,
                father."current_email" as father_email,
                father."current_address" as father_address,
                
                -- Informant Details
                informant."id" as informant_id,
                informant."surname" as informant_surname,
                informant."firstname" as informant_firstname,
                informant."middle_name" as informant_middle_name,
                informant."nin" as informant_nin,
                informant."current_phone_number" as informant_phone,
                informant."current_email" as informant_email,
                informant."current_address" as informant_address,
                r."Description" as informant_relationship,
                br."informant_signature",
                
                -- Registration Center Details
                rc."Reg_Center_Name" as registration_center,
                s_rc."State_Name" as registration_center_state,
                l_rc."LGA_Name" as registration_center_lga,
                l_rc."LGA_Code" as registration_center_lga_code,
                gz."Geo_Zone_Name" as registration_center_geo_zone,
                
                -- Registration Officers Details
                -- Registered By
                u_reg."UserName" as registered_by_user,
                u_reg."Email" as registered_by_email,
                u_reg."Phone_No" as registered_by_phone,
                u_reg."nin" as registered_by_nin,
                r_reg."name" as registered_by_role,
                
                -- Modified By
                u_mod."UserName" as modified_by_user,
                u_mod."Email" as modified_by_email,
                u_mod."Phone_No" as modified_by_phone,
                u_mod."nin" as modified_by_nin,
                r_mod."name" as modified_by_role,
                br."Date_Modified",
                
                -- Approved By
                u_app."UserName" as approved_by_user,
                u_app."Email" as approved_by_email,
                u_app."Phone_No" as approved_by_phone,
                u_app."nin" as approved_by_nin,
                r_app."name" as approved_by_role,
                br."Approval_Status",
                br."Date_Approved",
                
                -- Printed By
                u_print."UserName" as printed_by_user,
                u_print."Email" as printed_by_email,
                u_print."Phone_No" as printed_by_phone,
                u_print."nin" as printed_by_nin,
                r_print."name" as printed_by_role,
                br."Print_Status",
                br."Date_Printed",
                
                -- Additional Birth Registration Fields
                br."shared" as br_shared,
                br."shared_by" as br_shared_by,
                br."Modified_Status",
                br."Modified_Print",
                br."initiated_at",
                CASE 
                    WHEN br."Approval_Status" = 1 THEN 'Approved'
                    WHEN br."Approval_Status" = 2 THEN 'Queried'
                    ELSE 'Pending'
                END as approval_status_desc,
                CASE 
                    WHEN br."Print_Status" = 1 THEN 'Printed'
                    ELSE 'Not Printed'
                END as print_status_desc
                
            FROM birth_registration br
            -- Person Joins
            LEFT JOIN person child ON br."child" = child."id"
            LEFT JOIN person mother ON br."mother" = mother."id"
            LEFT JOIN person father ON br."father" = father."id"
            LEFT JOIN person informant ON br."informant" = informant."id"
            
            -- Registration Center Details
            LEFT JOIN registration_center rc ON br."Reg_Center" = rc."Reg_Center_id"
            LEFT JOIN lga l_rc ON rc."LGA_ID" = l_rc."LGA_ID"
            LEFT JOIN states s_rc ON l_rc."State_ID" = s_rc."State_ID"
            LEFT JOIN geo_zone gz ON s_rc."Geo_Zone_ID" = gz."Geo_Zone_id"
            
            -- Registration Officers
            LEFT JOIN users u_reg ON br."Registered_By" = u_reg."User_ID"
            LEFT JOIN roles r_reg ON u_reg."Role_ID" = r_reg."id"
            
            LEFT JOIN users u_mod ON br."Modified_By" = u_mod."User_ID"
            LEFT JOIN roles r_mod ON u_mod."Role_ID" = r_mod."id"
            
            LEFT JOIN users u_app ON br."Approved_By" = u_app."User_ID"
            LEFT JOIN roles r_app ON u_app."Role_ID" = r_app."id"
            
            LEFT JOIN users u_print ON br."Printed_by" = u_print."User_ID"
            LEFT JOIN roles r_print ON u_print."Role_ID" = r_print."id"
            
            -- Other Lookups
            LEFT JOIN birth_place bp ON br."birth_place" = bp."BirthPlace_ID"
            LEFT JOIN birth_type bt ON br."Birth_type" = bt."Birth_Type_ID"
            LEFT JOIN birth_order bo ON br."birth_order" = bo."Birth_Order_ID"
            LEFT JOIN locality_of_birth lb ON br."locality_of_birth" = lb."Locality_ID"
            LEFT JOIN marital_status ms_mother ON br."mother_marital_status" = ms_mother."Marital_Status_ID"
            LEFT JOIN marital_status ms_father ON br."father_marital_status" = ms_father."Marital_Status_ID"
            LEFT JOIN gender g_child ON child."gender" = g_child."Gender_ID"
            LEFT JOIN country c_child ON child."birth_country" = c_child."Country_Code"
            LEFT JOIN states s_child ON child."birth_state" = s_child."State_ID"
            LEFT JOIN lga l_child ON child."birth_lga" = l_child."LGA_ID"
            LEFT JOIN ethnic_group e_child ON child."ethnic_group" = e_child."Ethnic_Grp_ID"
            LEFT JOIN ethnic_group e_mother ON mother."ethnic_group" = e_mother."Ethnic_Grp_ID"
            LEFT JOIN ethnic_group e_father ON father."ethnic_group" = e_father."Ethnic_Grp_ID"
            LEFT JOIN country c_mother ON mother."current_nationality" = c_mother."Country_Code"
            LEFT JOIN country c_mother_res ON mother."current_residence_country" = c_mother_res."Country_Code"
            LEFT JOIN states s_mother ON mother."current_residence_state" = s_mother."State_ID"
            LEFT JOIN lga l_mother ON mother."current_residence_lga" = l_mother."LGA_ID"
            LEFT JOIN occupation_tpe ot_mother ON mother."current_occupation" = ot_mother."Occupation_Type_ID"
            LEFT JOIN country c_father ON father."current_nationality" = c_father."Country_Code"
            LEFT JOIN country c_father_res ON father."current_residence_country" = c_father_res."Country_Code"
            LEFT JOIN states s_father ON father."current_residence_state" = s_father."State_ID"
            LEFT JOIN lga l_father ON father."current_residence_lga" = l_father."LGA_ID"
            LEFT JOIN occupation_tpe ot_father ON father."current_occupation" = ot_father."Occupation_Type_ID"
            LEFT JOIN relationship r ON br."Informant_Relationship_ID" = r."Relationship_ID"
            
            WHERE br."Birth_Reg_ID" > %s
            ORDER BY br."Birth_Reg_ID"
            LIMIT %s
        )
        SELECT * FROM ranked_records
        ORDER BY "Birth_Reg_ID";
    """
    
    # Initialize SQLite connection
    sqlite_conn = sqlite3.connect(sqlite_path)
    
    # Get total count for progress tracking
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM birth_registration")
        total_records = cursor.fetchone()[0]
    
    start_time = time.time()
    last_id = 0
    total_processed = 0
    first_chunk = True
    
    try:
        while True:
            # Extract chunk from PostgreSQL
            df = pd.read_sql_query(
                query, 
                pg_conn, 
                params=(last_id, chunk_size)
            )
            
            if df.empty:
                break
                
            # Convert timestamp columns to datetime
            timestamp_columns = [
                'Date_Registerred', 'Date_Modified', 'Date_Approved',
                'Date_Printed', 'initiated_at'
            ]
            for col in timestamp_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Write to SQLite
            if first_chunk:
                # First chunk: create table with proper schema
                df.to_sql('birth_records', sqlite_conn, if_exists='replace', index=False)
                
                # Create indexes for faster querying
                cur = sqlite_conn.cursor()
                cur.execute('CREATE INDEX idx_birth_reg_id ON birth_records(Birth_Reg_ID)')
                cur.execute('CREATE INDEX idx_certificate_no ON birth_records(Certificate_No)')
                cur.execute('CREATE INDEX idx_child_id ON birth_records(child_id)')
                sqlite_conn.commit()
                first_chunk = False
            else:
                # Append subsequent chunks
                df.to_sql('birth_records', sqlite_conn, if_exists='append', index=False)
            
            # Update progress
            last_id = df['Birth_Reg_ID'].max()
            total_processed += len(df)
            elapsed_time = time.time() - start_time
            records_per_sec = total_processed / elapsed_time
            eta = (total_records - total_processed) / records_per_sec if records_per_sec > 0 else 0
            
            print(f"\rProcessed: {total_processed:,}/{total_records:,} records "
                  f"({(total_processed/total_records*100):.1f}%) "
                  f"Speed: {records_per_sec:.0f} records/sec "
                  f"ETA: {eta/60:.1f} minutes", end="")
            
            # Commit periodically
            sqlite_conn.commit()
    
    except Exception as e:
        print(f"\nError occurred: {e}")
        print(f"Last successful Birth_Reg_ID: {last_id}")
        raise
    finally:
        sqlite_conn.close()
    
    print("\nConversion completed!")
    print(f"Total time: {(time.time() - start_time)/60:.1f} minutes")
    print(f"Output saved to: {sqlite_path}")
    
# Function to read from SQLite efficiently
def read_birth_records(sqlite_path="databases/birth_records.db", where_clause=None, columns=None):
    """
    Efficiently read birth records from SQLite database.
    
    Args:
        sqlite_path: Path to SQLite database
        where_clause: Optional SQL WHERE clause for filtering
        columns: Optional list of columns to select
    
    Returns:
        pandas DataFrame
    """
    query = f"""
        SELECT {', '.join(columns) if columns else '*'}
        FROM birth_records
        {f'WHERE {where_clause}' if where_clause else ''}
    """
    
    with sqlite3.connect(sqlite_path) as conn:
        # Set pragmas for faster reading
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA cache_size=-2000000')  # Use 2GB cache
        conn.execute('PRAGMA synchronous=OFF')
        
        return pd.read_sql_query(query, conn)

# Example usage:
if __name__ == "__main__":
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        dbname="your_database",
        user="your_user",
        password="your_password",
        host="your_host"
    )
    
    try:
        # Extract to SQLite
        extract_to_sqlite(pg_conn)
        
        # Example: Read specific columns with condition
        df = read_birth_records(
            columns=[
                'Birth_Reg_ID', 'Certificate_No', 'child_nin',
                'child_surname', 'child_firstname'
            ],
            where_clause="child_nin IS NOT NULL"
        )
        print("\nSample data:")
        print(df.head())
        
    finally:
        pg_conn.close()
        
import pandas as pd
import sqlite3
import psycopg2
import time
import os
from datetime import datetime

def extract_birth_records(pg_conn, output_folder="birth_data", chunk_size=10000):
    """
    Extracts birth registration data from PostgreSQL and saves to both CSV and SQLite.
    Creates a new folder with timestamp and saves both files there.
    
    Args:
        pg_conn: PostgreSQL connection
        output_folder: Base folder for outputs
        chunk_size: Number of records to process at once
    """
    # Create timestamped folder
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    folder_path = f"{output_folder}_{timestamp}"
    os.makedirs(folder_path, exist_ok=True)
    
    # Define output paths
    csv_path = f"{folder_path}/birth_records.csv"
    sqlite_path = f"{folder_path}/birth_records.db"
    
    # Initialize SQLite connection
    sqlite_conn = sqlite3.connect(sqlite_path)
    
    # Your PostgreSQL query (the one we created earlier)
    query = """
        WITH ranked_records AS (
            ... # Your full query here
        )
        SELECT * FROM ranked_records
        WHERE "Birth_Reg_ID" > %s
        ORDER BY "Birth_Reg_ID"
        LIMIT %s;
    """
    
    # Get total count for progress tracking
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM birth_registration")
        total_records = cursor.fetchone()[0]
    
    start_time = time.time()
    last_id = 0
    total_processed = 0
    first_chunk = True
    
    try:
        while True:
            # Extract chunk from PostgreSQL
            df = pd.read_sql_query(
                query, 
                pg_conn, 
                params=(last_id, chunk_size)
            )
            
            if df.empty:
                break
                
            # Convert timestamp columns to datetime
            timestamp_columns = [
                'Date_Registerred', 'Date_Modified', 'Date_Approved',
                'Date_Printed', 'initiated_at'
            ]
            for col in timestamp_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Write to CSV
            if first_chunk:
                df.to_csv(csv_path, index=False, mode='w')
            else:
                df.to_csv(csv_path, index=False, mode='a', header=False)
            
            # Write to SQLite
            if first_chunk:
                # First chunk: create table with proper schema
                df.to_sql('birth_records', sqlite_conn, if_exists='replace', index=False)
                
                # Create indexes for faster querying
                cur = sqlite_conn.cursor()
                cur.execute('CREATE INDEX idx_birth_reg_id ON birth_records(Birth_Reg_ID)')
                cur.execute('CREATE INDEX idx_certificate_no ON birth_records(Certificate_No)')
                cur.execute('CREATE INDEX idx_child_id ON birth_records(child_id)')
                sqlite_conn.commit()
                
                first_chunk = False
            else:
                # Append subsequent chunks
                df.to_sql('birth_records', sqlite_conn, if_exists='append', index=False)
            
            # Update progress
            last_id = df['Birth_Reg_ID'].max()
            total_processed += len(df)
            elapsed_time = time.time() - start_time
            records_per_sec = total_processed / elapsed_time
            eta = (total_records - total_processed) / records_per_sec if records_per_sec > 0 else 0
            
            print(f"\rProcessed: {total_processed:,}/{total_records:,} records "
                  f"({(total_processed/total_records*100):.1f}%) "
                  f"Speed: {records_per_sec:.0f} records/sec "
                  f"ETA: {eta/60:.1f} minutes", end="")
            
            # Commit SQLite periodically
            sqlite_conn.commit()
    
    except Exception as e:
        print(f"\nError occurred: {e}")
        print(f"Last successful Birth_Reg_ID: {last_id}")
        raise
    finally:
        sqlite_conn.close()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Get file sizes
    csv_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
    sqlite_size = os.path.getsize(sqlite_path) / (1024 * 1024)  # MB
    
    print("\nExtraction completed!")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Records processed: {total_processed:,}")
    print(f"Average speed: {total_processed/total_time:.0f} records/second")
    print("\nFiles created:")
    print(f"CSV: {csv_path} ({csv_size:.1f} MB)")
    print(f"SQLite: {sqlite_path} ({sqlite_size:.1f} MB)")

def read_from_sqlite(sqlite_path, query=None):
    """Read data from SQLite database."""
    conn = sqlite3.connect(sqlite_path)
    if query is None:
        query = "SELECT * FROM birth_records"
    return pd.read_sql_query(query, conn)

def read_from_csv(csv_path):
    """Read data from CSV file."""
    return pd.read_csv(csv_path)


def extract_person_table(conn, output_folder="person_data", chunk_size=1000):
    """
    Extracts the complete person table with proper deduplication and error handling.
    
    Args:
        conn: psycopg2 connection object
        output_folder: Folder to save the output CSV
        chunk_size: Number of records to process at a time
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    # Query to get person data with deduplication
    query = """
        WITH ranked_records AS (
            SELECT DISTINCT ON (id)
                id,
                current_marital_status,
                current_nationality,
                current_residence_country,
                current_residence_state,
                current_residence_lga,
                current_literacy_level,
                current_education_level,
                current_occupation,
                origin_country,
                origin_state,
                origin_lga,
                birth_date,
                birth_settlement_type,
                ethnic_group,
                gender,
                birth_country,
                birth_state,
                birth_lga,
                town_of_birth,
                surname,
                firstname,
                middle_name,
                maiden_name,
                town_of_origin,
                nin,
                tracking_id,
                no_nin_reason,
                current_phone_number,
                current_email,
                current_address
            FROM person
            WHERE id > %s
            ORDER BY id
            LIMIT %s
        )
        SELECT * FROM ranked_records
        ORDER BY id;
    """
    
    start_time = time.time()
    csv_file_path = f'{output_folder}/full_person_data.csv'
    
    try:
        with conn.cursor() as cursor:
            with open(csv_file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                # cursor.execute("""
                #     SELECT column_name 
                #     FROM information_schema.columns 
                #     WHERE table_name = 'person' 
                #     ORDER BY ordinal_position;
                # """)
                headers = [ 'id',
                'current_marital_status',
                'current_nationality',
                'current_residence_country',
                'current_residence_state',
                'current_residence_lga',
                'current_literacy_level',
                'current_education_level',
                'current_occupation',
                'origin_country',
                'origin_state',
                'origin_lga',
                'birth_date',
                'birth_settlement_type',
                'ethnic_group',
                'gender',
                'birth_country',
                'birth_state',
                'birth_lga',
                'town_of_birth',
                'surname',
                'firstname',
                'middle_name',
                'maiden_name',
                'town_of_origin',
                'nin',
                'tracking_id',
                'no_nin_reason',
                'current_phone_number',
                'current_email',
                'current_address',
                ]
                writer.writerow(headers)
                
                last_id = 0
                total_rows = 0
                processed_ids = set()
                
                while True:
                    cursor.execute(query, (last_id, chunk_size))
                    rows = cursor.fetchall()
                    
                    if not rows:
                        break
                        
                    # Filter duplicates
                    new_rows = []
                    for row in rows:
                        person_id = row[0]  # id is first column
                        if person_id not in processed_ids:
                            processed_ids.add(person_id)
                            new_rows.append(row)
                    
                    if not new_rows:
                        break
                        
                    writer.writerows(new_rows)
                    last_id = new_rows[-1][0]
                    total_rows += len(new_rows)
                    
                    print(f"Processed {total_rows:,} unique person records...", end='\r')
                    
        end_time = time.time()
        print(f"\nExtracted {total_rows:,} unique person records")
        print(f"Processing time: {end_time - start_time:.2f} seconds")
        print(f"Output saved to: {csv_file_path}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        if os.path.exists(csv_file_path):
            print("Partial data may have been written to the output file.")
        raise
    finally:
        conn.commit()
        
    return csv_file_path

def extract_person_table_with_resume(conn, output_folder="person_data", chunk_size=1000):
    """
    Extracts the complete person table with resume capability, ensuring uniqueness by ID.
    Only extracts required columns in correct order.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    csv_file_path = f'{output_folder}/full_person_data.csv'
    last_id = 0
    file_mode = 'w'
    
    # Check if file exists and get last processed ID
    if os.path.exists(csv_file_path):
        try:
            with open(csv_file_path, 'rb') as f:
                try:
                    f.seek(0, 2)
                    file_size = f.tell()
                    chunk_size_bytes = min(10000, file_size)
                    f.seek(file_size - chunk_size_bytes)
                    last_chunk = f.read().decode('latin1')
                    
                    lines = last_chunk.split('\n')
                    for line in reversed(lines):
                        if line.strip():
                            try:
                                last_id = int(line.split(',')[0])
                                file_mode = 'a'
                                print(f"Found last ID: {last_id}")
                                break
                            except (ValueError, IndexError):
                                continue
                except Exception as e:
                    print(f"Error reading file end: {e}")
        except Exception as e:
            print(f"Error handling existing file: {e}")
    
    if file_mode == 'a':
        print(f"Resuming extraction from ID: {last_id}")
    else:
        print("Starting new extraction")
        
    # Modified query with exact columns needed
    query = """
        SELECT DISTINCT ON (id) 
            id,
            current_marital_status,
            current_nationality,
            current_residence_country,
            current_residence_state,
            current_residence_lga,
            current_literacy_level,
            current_education_level,
            current_occupation,
            origin_country,
            origin_state,
            origin_lga,
            birth_date,
            birth_settlement_type,
            ethnic_group,
            gender,
            birth_country,
            birth_state,
            birth_lga,
            town_of_birth,
            surname,
            firstname,
            middle_name,
            maiden_name,
            town_of_origin,
            nin,
            tracking_id,
            no_nin_reason,
            current_phone_number,
            current_email,
            current_address
        FROM person 
        WHERE id > %s 
        ORDER BY id, current_marital_status DESC
        LIMIT %s;
    """
    
    start_time = time.time()
    total_rows = 0
    processed_ids = set()
    
    try:
        with conn.cursor() as cursor:
            with open(csv_file_path, file_mode, newline='', encoding='utf-8', errors='replace') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header only in write mode
                if file_mode == 'w':
                    headers = [
                        'id', 'current_marital_status', 'current_nationality',
                        'current_residence_country', 'current_residence_state',
                        'current_residence_lga', 'current_literacy_level',
                        'current_education_level', 'current_occupation',
                        'origin_country', 'origin_state', 'origin_lga',
                        'birth_date', 'birth_settlement_type', 'ethnic_group',
                        'gender', 'birth_country', 'birth_state', 'birth_lga',
                        'town_of_birth', 'surname', 'firstname', 'middle_name',
                        'maiden_name', 'town_of_origin', 'nin', 'tracking_id',
                        'no_nin_reason', 'current_phone_number', 'current_email',
                        'current_address'
                    ]
                    writer.writerow(headers)
                
                while True:
                    try:
                        cursor.execute(query, (last_id, chunk_size))
                        rows = cursor.fetchall()
                        
                        if not rows:
                            break
                            
                        # Filter out any duplicates
                        new_rows = []
                        for row in rows:
                            person_id = row[0]
                            if person_id not in processed_ids:
                                # Clean string values
                                cleaned_row = []
                                for item in row:
                                    if isinstance(item, str):
                                        item = item.encode('utf-8', 'replace').decode('utf-8')
                                    cleaned_row.append(item)
                                processed_ids.add(person_id)
                                new_rows.append(cleaned_row)
                        
                        if not new_rows:
                            break
                        
                        writer.writerows(new_rows)
                        last_id = new_rows[-1][0]
                        total_rows += len(new_rows)
                        
                        print(f"Processed {total_rows:,} unique records (Last ID: {last_id})...", end='\r')
                        
                        # Flush periodically
                        if total_rows % (chunk_size * 10) == 0:
                            csvfile.flush()
                            os.fsync(csvfile.fileno())
                        
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        print(f"\nDatabase connection lost: {e}")
                        print("Attempting to reconnect...")
                        conn = connect_to_db()
                        cursor = conn.cursor()
                        continue
                    
        end_time = time.time()
        print(f"\nExtracted {total_rows:,} unique records")
        print(f"Processing time: {end_time - start_time:.2f} seconds")
        print(f"Output saved to: {csv_file_path}")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print(f"Last successful ID: {last_id}")
        print("You can resume from this point later.")
        raise
    finally:
        conn.commit()
        
    return csv_file_path

def verify_person_data(csv_file):
    """
    Verifies that the extracted person data is correct and unique.
    """
    print(f"\nVerifying data in {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # Check uniqueness
    total_records = len(df)
    unique_ids = df['id'].nunique()
    print(f"Total records: {total_records:,}")
    print(f"Unique IDs: {unique_ids:,}")
    
    # Check columns
    expected_columns = [
        'id', 'current_marital_status', 'current_nationality',
        'current_residence_country', 'current_residence_state',
        'current_residence_lga', 'current_literacy_level',
        'current_education_level', 'current_occupation',
        'origin_country', 'origin_state', 'origin_lga',
        'birth_date', 'birth_settlement_type', 'ethnic_group',
        'gender', 'birth_country', 'birth_state', 'birth_lga',
        'town_of_birth', 'surname', 'firstname', 'middle_name',
        'maiden_name', 'town_of_origin', 'nin', 'tracking_id',
        'no_nin_reason', 'current_phone_number', 'current_email',
        'current_address'
    ]
    
    actual_columns = df.columns.tolist()
    
    print("\nColumn Check:")
    if actual_columns == expected_columns:
        print("✓ All columns are correct and in proper order")
    else:
        print("! Column mismatch found")
        print("Missing columns:", set(expected_columns) - set(actual_columns))
        print("Extra columns:", set(actual_columns) - set(expected_columns))
    
    # Check for empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        print(f"! Found {empty_rows} empty rows")
    else:
        print("✓ No empty rows found")
    
    return {
        'is_unique': total_records == unique_ids,
        'columns_match': actual_columns == expected_columns,
        'total_records': total_records,
        'empty_rows': empty_rows
    }


def merge_birth_and_person_data_efficient(birth_file, person_file, output_folder="merged_data", chunk_size=100000):
    """
    Efficiently merges birth registration data with person data using chunks.
    Handles encoding issues and large files.
    
    Args:
        birth_file: Path to birth registration CSV
        person_file: Path to person data CSV
        output_folder: Folder to save merged data
        chunk_size: Size of chunks for processing
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    output_file = f'{output_folder}/merged_birth_person_data.csv'
    
    print("Starting merge process...")
    start_time = time.time()
    
    try:
        # Try different encodings for birth data
        encodings_to_try = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        birth_df = None
        
        for encoding in encodings_to_try:
            try:
                print(f"Attempting to read birth data with {encoding} encoding...")
                birth_df = pd.read_csv(birth_file, 
                                     dtype={'child': str, 'mother': str, 'father': str},
                                     encoding=encoding)
                print(f"Successfully read birth data with {encoding} encoding")
                break
            except Exception as e:
                print(f"Failed with {encoding} encoding: {str(e)}")
                continue
                
        if birth_df is None:
            raise Exception("Could not read birth registration data with any encoding")

        # Get unique person IDs needed
        print("Identifying required person records...")
        relevant_ids = set()
        for col in ['child', 'mother', 'father']:
            relevant_ids.update(birth_df[col].dropna().unique())
        
        print(f"Found {len(relevant_ids)} unique person IDs to process")
        
        # Process person data in chunks with encoding handling
        print("Processing person data in chunks...")
        person_chunks = []
        
        for encoding in encodings_to_try:
            try:
                chunk_reader = pd.read_csv(person_file, 
                                         chunksize=chunk_size,
                                         dtype={'id': str},
                                         encoding=encoding)
                
                print(f"Reading person data with {encoding} encoding...")
                for i, chunk in enumerate(chunk_reader):
                    # Filter only relevant IDs
                    relevant_chunk = chunk[chunk['id'].isin(relevant_ids)]
                    if not relevant_chunk.empty:
                        # Clean string columns in the chunk
                        for col in relevant_chunk.select_dtypes(['object']):
                            relevant_chunk[col] = relevant_chunk[col].apply(
                                lambda x: x.encode('utf-8', 'replace').decode('utf-8') if isinstance(x, str) else x
                            )
                        person_chunks.append(relevant_chunk)
                        print(f"Processed chunk {i+1} with {len(relevant_chunk)} relevant records...", end='\r')
                
                print(f"\nSuccessfully read all person data with {encoding} encoding")
                break
                
            except Exception as e:
                print(f"Failed with {encoding} encoding: {str(e)}")
                continue
        
        if not person_chunks:
            raise Exception("Could not process person data with any encoding")
        
        # Combine person chunks
        print("\nCombining person data chunks...")
        person_df = pd.concat(person_chunks, ignore_index=True)
        person_df = person_df.drop_duplicates(subset='id')
        
        print(f"Found {len(person_df)} matching person records")
        
        # Perform merges with progress updates
        print("\nMerging data...")
        
        print("1/3: Merging child data...")
        merged_df = birth_df.merge(
            person_df,
            left_on='child',
            right_on='id',
            how='left',
            suffixes=('', '_child')
        )
        
        print("2/3: Merging mother data...")
        merged_df = merged_df.merge(
            person_df,
            left_on='mother',
            right_on='id',
            how='left',
            suffixes=('', '_mother')
        )
        
        print("3/3: Merging father data...")
        merged_df = merged_df.merge(
            person_df,
            left_on='father',
            right_on='id',
            how='left',
            suffixes=('', '_father')
        )
        
        # Clean up duplicate columns
        print("Cleaning up merged data...")
        columns_to_drop = [col for col in merged_df.columns if col.endswith('_x') or col.endswith('_y')]
        merged_df = merged_df.drop(columns=columns_to_drop)
        
        # Save merged data with encoding handling
        print("Saving merged data...")
        try:
            merged_df.to_csv(output_file, index=False, encoding='utf-8')
        except Exception as e:
            print(f"Failed to save with UTF-8, trying with latin1: {e}")
            merged_df.to_csv(output_file, index=False, encoding='latin1')
        
        end_time = time.time()
        print(f"\nMerge completed successfully!")
        print(f"Total processing time: {end_time - start_time:.2f} seconds")
        print(f"Output saved to: {output_file}")
        
        # Print merge statistics
        print("\nMerge Statistics:")
        print(f"Total birth records: {len(birth_df)}")
        print(f"Total person records: {len(person_df)}")
        print(f"Final merged records: {len(merged_df)}")
        
        # Print sample of unmatched records
        unmatched_children = merged_df[merged_df['firstname_child'].isna()]['child'].nunique()
        unmatched_mothers = merged_df[merged_df['firstname_mother'].isna()]['mother'].nunique()
        unmatched_fathers = merged_df[merged_df['firstname_father'].isna()]['father'].nunique()
        
        print("\nUnmatched Records:")
        print(f"Children without matches: {unmatched_children}")
        print(f"Mothers without matches: {unmatched_mothers}")
        print(f"Fathers without matches: {unmatched_fathers}")
        
        return merged_df
        
    except Exception as e:
        print(f"\nAn error occurred during merge: {e}")
        raise



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
                with open(output_file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = sum(1 for _ in reader) - 1  # Subtract 1 for header
                    start_offset = max(start_offset, rows)
                print(f"Resuming from offset {start_offset}")
                file_mode = 'a'  # Append to existing file
            else:
                file_mode = 'w'  # Create a new file
                
            # Write or append to CSV
            with open(output_file, file_mode, newline='', encoding='utf-8') as csvfile:
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
                        
                    # Clean any problematic characters
                    cleaned_chunk = []
                    for row in chunk:
                        cleaned_row = []
                        for item in row:
                            if isinstance(item, str):
                                # Replace or remove problematic characters
                                item = item.encode('utf-8', 'replace').decode('utf-8')
                            cleaned_row.append(item)
                        cleaned_chunk.append(cleaned_row)
                    
                    writer.writerows(cleaned_chunk)
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

import os
import csv
import time
from datetime import datetime, timedelta

def extract_birth_data_in_date_range(conn, start_date, end_date, output_folder="data", column="Date_Registerred", chunk_size=1000):
    """
    Extracts data within a specified date range from a PostgreSQL table in batches,
    ensuring each record is downloaded exactly once using keyset pagination.

    Args:
        conn: psycopg2 connection object.
        start_date (str): Start date for data extraction in YYYY-MM-DD format.
        end_date (str): End date for data extraction in YYYY-MM-DD format.
        output_folder (str): Name of the desired output folder (default: "data").
        column (str): Name of the date column (default: "Date_Registerred").
        chunk_size (int): Number of rows to fetch in each batch (default: 1000).
    """
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Error: {e}")
        return

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Modified query to use keyset pagination
    query = f"""
            SELECT "Birth_Reg_ID", "father", "mother", "informant", "child", 
                   "mother_age_at_birth", "father_age_at_birth", "mother_marital_status", 
                   "father_marital_status", "Reg_Center", "Certificate_No", "birth_place", 
                   "Birth_type", "birth_order", "locality_of_birth", "{column}", 
                   "Registered_By", "Informant_Relationship_ID", "Date_Modified", 
                   "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                   "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                   "Print_Status", "Date_Printed", "Printed_by", "nin", "hash", "self", 
                   "reference", "nin_status", "checked", "initiated_at"
            FROM birth_registration
            WHERE "{column}" >= %s 
            AND "{column}" < %s 
            AND "Birth_Reg_ID" > %s
            ORDER BY "Birth_Reg_ID"
            LIMIT %s
    """

    start_time = time.time()
    csv_file_path = f'{output_folder}/birth_reg_{start_date}_to_{end_date}.csv'

    with conn.cursor() as cursor:
        # Write header
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Get column names for header
            header_query = f"""
            SELECT "Birth_Reg_ID", "father", "mother", "informant", "child", 
                   "mother_age_at_birth", "father_age_at_birth", "mother_marital_status", 
                   "father_marital_status", "Reg_Center", "Certificate_No", "birth_place", 
                   "Birth_type", "birth_order", "locality_of_birth", "{column}", 
                   "Registered_By", "Informant_Relationship_ID", "Date_Modified", 
                   "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                   "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                   "Print_Status", "Date_Printed", "Printed_by", "nin", "hash", "self", 
                   "reference", "nin_status", "checked", "initiated_at"
            FROM birth_registration
            LIMIT 1
            """
            cursor.execute(header_query)
            writer.writerow([desc[0] for desc in cursor.description])

            # Use keyset pagination
            last_id = 0  # Start with ID 0
            total_rows = 0

            while True:
                cursor.execute(query, (start_date, end_date + timedelta(days=1), last_id, chunk_size))
                chunk = cursor.fetchall()
                
                if not chunk:
                    break

                # Write chunk to the CSV
                writer.writerows(chunk)
                
                # Update the last_id for the next iteration
                last_id = chunk[-1][0]  # Birth_Reg_ID is the first column
                total_rows += len(chunk)
                
                print(f"Processed {total_rows} records...", end='\r')

    end_time = time.time()
    extraction_time = end_time - start_time
    print(f"\nExtracted {total_rows} records for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")



# Extract data for a specific date range
def extract_approvals_in_date_range(conn, start_date, end_date, output_folder="data", column="Date_Registerred", chunk_size=1000):
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

    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # SQL query with placeholders
    query = f"""
            SELECT "Birth_Reg_ID", "Certificate_No",  "{column}", "Date_Modified", 
                   "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                   "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                   "Print_Status", "Date_Printed", "Printed_by"
            FROM birth_registration
            WHERE "{column}" >= %s AND "{column}" < %s 
            OFFSET %s LIMIT %s
    """

    start_time = time.time()

    with conn.cursor() as cursor:
        # Open the CSV file for writing
        csv_file_path = f'{output_folder}/approval_update_{start_date}_to_{end_date}.csv'
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            header_query = f"""
            SELECT "Birth_Reg_ID", "Certificate_No",  "{column}", "Date_Modified", 
                   "Modified_By", "shared", "shared_by", "Approval_ID", "Approval_Status", 
                   "Modified_Status", "Modified_Print", "Approved_By", "Date_Approved", 
                   "Print_Status", "Date_Printed", "Printed_by"
            FROM birth_registration
            LIMIT 1
            """
            cursor.execute(header_query)
            writer.writerow([desc[0] for desc in cursor.description])

            # Write data in chunks
            offset = 0
            while True:
                cursor.execute(query, (start_date, end_date + timedelta(days=1), offset, chunk_size))
                chunk = cursor.fetchall()
                if not chunk:
                    break

                # Write chunk to the CSV
                writer.writerows(chunk)
                offset += chunk_size

    end_time = time.time()
    extraction_time = end_time - start_time
    print(f"Extracted Approval Status data for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")




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


def extract_person_child_birth(conn, start_date, end_date, chunk_size=1000):
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
    SELECT child 
    FROM birth_registration 
    WHERE "Date_Registerred" >= %s AND "Date_Registerred" < %s
    OFFSET %s LIMIT %s;
    """

    current_date = start_date
    while current_date <= end_date:
        start_time = time.time()

        with conn.cursor() as cursor:
            with open(f'persons_data/child_record_{current_date.strftime("%Y-%m-%d")}.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)

                # Write the header row for person data
                cursor.execute('SELECT id, birth_date, gender, firstname, middle_name, surname FROM person LIMIT 1')
                writer.writerow([desc[0] for desc in cursor.description])

                offset = 0
                while True:
                    cursor.execute(query_ids, (current_date, current_date + timedelta(days=1), offset, chunk_size))
                    id_chunks = cursor.fetchall()

                    if not id_chunks:
                        break

                    unique_ids = set()
                    for row in id_chunks:
                        unique_ids.update(row)

                    if not unique_ids:
                        print(f"No records found for the date {current_date.strftime('%Y-%m-%d')}")
                        break

                    id_placeholders = ', '.join(['%s'] * len(unique_ids))
                    query_persons = f"""
                    SELECT id, birth_date, gender, firstname, middle_name, surname 
                    FROM person 
                    WHERE id IN ({id_placeholders});
                    """

                    cursor.execute(query_persons, tuple(unique_ids))
                    person_data = cursor.fetchall()

                    writer.writerows(person_data)

                    offset += chunk_size

        end_time = time.time()
        extraction_time = end_time - start_time
        print(f"Extracted person data for {current_date.strftime('%Y-%m-%d')} in {extraction_time:.2f} seconds")

        current_date += timedelta(days=1)


def load_child_birth_with_chunks(folder, start_date, end_date, chunksize=100000):
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
        file_path = os.path.join(folder, f'child_record_{single_date.strftime("%Y-%m-%d")}.csv')
        if os.path.exists(file_path):
            print(f"Loading {file_path} in chunks...",  end='\r')
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

def merge_birth_and_c_birth_data(birth_file, person_folder, start_date, end_date, birth_col_child='child', person_col='id', chunksize=100000):
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
    person_data_df = load_child_birth_with_chunks(person_folder, start_date, end_date, chunksize=chunksize)
    
    if person_data_df.empty:
        print("No person data found for the specified date range.")
        return pd.DataFrame()

    # Step 3: Merge for child information
    merged_df = birth_registration_df.merge(
        person_data_df,
        how='left',  
        left_on=birth_col_child,
        right_on=person_col,  
        suffixes=('', '_child')  
    )
    print("Merged Child Person Information.")
    
    # # Step 4: Merge for mother information
    # merged_df = merged_df.merge(
    #     person_data_df,
    #     how='left',
    #     left_on=birth_col_mother,
    #     right_on=person_col,
    #     suffixes=('', '_mother')  
    # )
    # print("Merged Mother Person Information.")

    # # Step 5: Merge for father information
    # merged_df = merged_df.merge(
    #     person_data_df,
    #     how='left',
    #     left_on=birth_col_father,
    #     right_on=person_col,
    #     suffixes=('', '_father')  
    # )
    # print("Merged Father Person Information.")

    # Step 6: Drop duplicate rows based on a unique column (if applicable)
    merged_df.drop_duplicates(subset='Certificate_No', inplace=True)

    # Return the merged DataFrame
    return merged_df
