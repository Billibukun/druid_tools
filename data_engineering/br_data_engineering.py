# br_engineering.py

import os
import csv
import json
import time
import sqlite3
import psycopg2
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, List, Dict, Any
from abc import ABC, abstractmethod


# Function to connect to the database
def connect_to_db():
    return psycopg2.connect(
        database='',
        host='',
        user='',
        password='',
        port='5432'   
    )


class BaseBirthRecordsReader(ABC):
    """Abstract base class for birth records processing"""
    
    def __init__(self, connect_to_db, output_path: str, chunk_size: int = 5000,
                 max_workers: int = 4):
        self.connect_to_db = connect_to_db
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.processed_ids = set()
        self.resume_file = f"{output_path}/.resume_state.json"
        os.makedirs(output_path, exist_ok=True)
        
    def load_resume_state(self) -> Tuple[int, int]:
        """Load resume state from file"""
        try:
            if os.path.exists(self.resume_file):
                with open(self.resume_file, 'r') as f:
                    state = json.load(f)
                    return state.get('last_id', 0), state.get('total_rows', 0)
            return 0, 0
        except Exception as e:
            print(f"Error loading resume state: {e}")
            return 0, 0

    def save_resume_state(self, last_id: int, total_rows: int) -> None:
        """Save current state for resume capability"""
        try:
            with open(self.resume_file, 'w') as f:
                json.dump({
                    'last_id': last_id,
                    'total_rows': total_rows,
                    'timestamp': datetime.now().isoformat(),
                    'output_file': self.output_file
                }, f)
        except Exception as e:
            print(f"Error saving resume state: {e}")

    @abstractmethod
    def setup_output(self) -> None:
        """Initialize output storage"""
        pass

    @abstractmethod
    def write_chunk(self, cleaned_rows: List[tuple]) -> None:
        """Write processed chunk to output"""
        pass

    def process_chunk(self, rows: List[tuple]) -> List[tuple]:
        """Process chunk of rows in parallel"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return list(executor.map(self.clean_data_row, rows))

    @staticmethod
    def clean_data_row(row: tuple) -> tuple:
        """Clean individual row data"""
        return tuple(
            '' if item is None else
            ' '.join(str(item).replace('\x00', '')
                    .replace('\r', ' ')
                    .replace('\n', ' ')
                    .split())
            for item in row
        )

    def extract_data(self) -> None:
        """Main extraction method"""
        last_id, total_rows = self.load_resume_state()
        start_time = time.time()
        conn = None
        
        try:
            conn = self.connect_to_db()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM birth_registration")
                total_records = cursor.fetchone()[0]
                
                while True:
                    cursor.execute(self.get_query(), (last_id, self.chunk_size))
                    rows = cursor.fetchall()
                    if not rows:
                        break
                        
                    cleaned_rows = self.process_chunk(rows)
                    self.write_chunk(cleaned_rows)
                    
                    last_id = cleaned_rows[-1][0]
                    total_rows += len(cleaned_rows)
                    
                    self.print_progress(total_rows, total_records, start_time)
                    self.save_resume_state(last_id, total_rows)
                    
        except Exception as e:
            print(f"\nError occurred: {e}")
            raise
        finally:
            if conn:
                conn.close()
            self.print_final_stats(total_rows, start_time)

    def print_progress(self, total_rows: int, total_records: int, start_time: float) -> None:
        """Print progress metrics"""
        elapsed = time.time() - start_time
        speed = total_rows / elapsed
        real_speed = total_rows / max(elapsed, 0.1)
        eta = (total_records - total_rows) / speed if speed > 0 else 0
        
        print(f"\rProcessed: {total_rows:,}/{total_records:,} "
              f"({total_rows/total_records*100:.1f}%) "
              f"Speed: {speed:.0f} r/s "
              f"Speed: {real_speed:.0f} r/s "
              f"ETA: {eta/60:.1f}m", end="")

    def print_final_stats(self, total_rows: int, start_time: float) -> None:
        """Print final execution statistics"""
        total_time = time.time() - start_time
        print(f"\n\nExtraction completed:"
              f"\nTotal records: {total_rows:,}"
              f"\nProcessing time: {total_time/3600:.1f}h"
              f"\nAverage speed: {total_rows/total_time:.1f} r/s"
              f"\nOutput: {self.output_file}")

    @staticmethod
    def get_headers() -> List[str]:
        return [
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
            "mother_occupation", "mother_phone", "mother_address",
            
            # Father Details
            "father_id", "father_surname", "father_firstname", "father_middle_name",
            "father_birth_date", "father_nin", "father_no_nin_reason",
            "father_ethnic_group", "father_nationality", "father_residence_country",
            "father_residence_state", "father_residence_lga", "father_occupation",
            "father_phone", "father_address",
            
            # Informant Details
            "informant_id", "informant_surname", "informant_firstname",
            "informant_middle_name", "informant_nin", "informant_phone",
            "informant_address", "informant_relationship",
            
            # Registration Center Details
            "registration_center", "registration_center_state",
            "registration_center_lga", "registration_center_lga_code",
            "registration_center_geo_zone",
            
            # Registration Officers Details
            "registered_by_user", "registered_by_email", "registered_by_phone",
            "registered_by_nin", "registered_by_role",
            "modified_by_user", "modified_by_email", "modified_by_phone",
            "modified_by_nin", "modified_by_role", "Date_Modified",
            "approved_by_user", "approved_by_email", "approved_by_phone",
            "approved_by_nin", "approved_by_role", "Approval_Status",
            "Date_Approved", "printed_by_user", "printed_by_email",
            "printed_by_phone", "printed_by_nin", "printed_by_role",
            "Print_Status", "Date_Printed",
            
            # Additional Status Fields
            "br_shared", "br_shared_by", "Modified_Status", "Modified_Print",
            "initiated_at", "approval_status_desc", "print_status_desc"
        ]
    
    @staticmethod
    def get_query() -> str:
        return """
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
            father."current_address" as father_address,
            
            -- Informant Details
            informant."id" as informant_id,
            informant."surname" as informant_surname,
            informant."firstname" as informant_firstname,
            informant."middle_name" as informant_middle_name,
            informant."nin" as informant_nin,
            informant."current_phone_number" as informant_phone,
            informant."current_address" as informant_address,
            r."Description" as informant_relationship,
            
            -- Registration Center Details
            rc."Reg_Center_Name" as registration_center,
            s_rc."State_Name" as registration_center_state,
            l_rc."LGA_Name" as registration_center_lga,
            l_rc."LGA_Code" as registration_center_lga_code,
            gz."Geo_Zone_Name" as registration_center_geo_zone,
            
            -- Registration Officers Details
            u_reg."UserName" as registered_by_user,
            u_reg."Email" as registered_by_email,
            u_reg."Phone_No" as registered_by_phone,
            u_reg."nin" as registered_by_nin,
            r_reg."name" as registered_by_role,
            u_mod."UserName" as modified_by_user,
            u_mod."Email" as modified_by_email,
            u_mod."Phone_No" as modified_by_phone,
            u_mod."nin" as modified_by_nin,
            r_mod."name" as modified_by_role,
            br."Date_Modified",
            u_app."UserName" as approved_by_user,
            u_app."Email" as approved_by_email,
            u_app."Phone_No" as approved_by_phone,
            u_app."nin" as approved_by_nin,
            r_app."name" as approved_by_role,
            br."Approval_Status",
            br."Date_Approved",
            u_print."UserName" as printed_by_user,
            u_print."Email" as printed_by_email,
            u_print."Phone_No" as printed_by_phone,
            u_print."nin" as printed_by_nin,
            r_print."name" as printed_by_role,
            br."Print_Status",
            br."Date_Printed",
            
            -- Additional Status Fields
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
        LEFT JOIN person child ON br."child" = child."id"
        LEFT JOIN person mother ON br."mother" = mother."id"
        LEFT JOIN person father ON br."father" = father."id"
        LEFT JOIN person informant ON br."informant" = informant."id"
        
        LEFT JOIN registration_center rc ON br."Reg_Center" = rc."Reg_Center_id"
        LEFT JOIN lga l_rc ON rc."LGA_ID" = l_rc."LGA_ID"
        LEFT JOIN states s_rc ON l_rc."State_ID" = s_rc."State_ID"
        LEFT JOIN geo_zone gz ON s_rc."Geo_Zone_ID" = gz."Geo_Zone_id"
        
        LEFT JOIN users u_reg ON br."Registered_By" = u_reg."User_ID"
        LEFT JOIN roles r_reg ON u_reg."Role_ID" = r_reg."id"
        LEFT JOIN users u_mod ON br."Modified_By" = u_mod."User_ID"
        LEFT JOIN roles r_mod ON u_mod."Role_ID" = r_mod."id"
        LEFT JOIN users u_app ON br."Approved_By" = u_app."User_ID"
        LEFT JOIN roles r_app ON u_app."Role_ID" = r_app."id"
        LEFT JOIN users u_print ON br."Printed_by" = u_print."User_ID"
        LEFT JOIN roles r_print ON u_print."Role_ID" = r_print."id"
        
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
        LIMIT %s;
        """


class CSVBirthRecordsReader(BaseBirthRecordsReader):
    """CSV implementation of birth records reader"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = f"{self.output_path}/birth_records.csv"
        self.setup_output()
        
    def setup_output(self) -> None:
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', buffering=65536) as f:
                writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(self.get_headers())

    def write_chunk(self, cleaned_rows: List[tuple]) -> None:
        with open(self.output_file, 'a', newline='', buffering=65536) as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerows(cleaned_rows)

class SQLiteBirthRecordsReader(BaseBirthRecordsReader):
    """SQLite implementation of birth records reader"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_file = f"{self.output_path}/birth_records.db"
        self.setup_output()
        
    def setup_output(self) -> None:
        conn = sqlite3.connect(self.output_file)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA page_size=4096")
        
        columns = []
        for header in self.get_headers():
            if "_date" in header.lower():
                col_type = "TIMESTAMP"
            elif "_id" in header.lower() or header.endswith("_ID"):
                col_type = "INTEGER"
            else:
                col_type = "TEXT"
            columns.append(f"{header} {col_type}")
            
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS birth_records (
            {', '.join(columns)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
        conn.execute(create_table_sql)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_birth_reg_id ON birth_records(Birth_Reg_ID)")
        conn.commit()
        conn.close()

    def write_chunk(self, cleaned_rows: List[tuple]) -> None:
        conn = sqlite3.connect(self.output_file)
        placeholders = ','.join(['?' for _ in self.get_headers()])
        conn.executemany(
            f"INSERT INTO birth_records ({','.join(self.get_headers())}) VALUES ({placeholders})",
            cleaned_rows
        )
        conn.commit()
        conn.close()

class HybridBirthRecordsReader:
    """Hybrid class that writes to both CSV and SQLite simultaneously"""
    
    def __init__(self, connect_to_db, output_path: str, chunk_size: int = 5000,
                 max_workers: int = 4):
        self.csv_reader = CSVBirthRecordsReader(
            connect_to_db, output_path, chunk_size, max_workers
        )
        self.sqlite_reader = SQLiteBirthRecordsReader(
            connect_to_db, output_path, chunk_size, max_workers
        )
        
    def extract_data(self) -> None:
        print("Starting hybrid extraction...")
        start_time = time.time()
        
        print("\nExtracting to CSV...")
        self.csv_reader.extract_data()
        
        print("\nExtracting to SQLite...")
        self.sqlite_reader.extract_data()
        
        total_time = time.time() - start_time
        print(f"\nTotal hybrid extraction time: {total_time/3600:.1f}h")
