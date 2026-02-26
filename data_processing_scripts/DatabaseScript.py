#!/usr/bin/env python3.10
import os
import glob
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import perf_counter
import pandas as pd
import re
import numpy as np

# Find files with specified extensions
def find_files_with_extensions(folder_path, extensions):
    files = []
    for extension in extensions:
        files.extend(glob.glob(os.path.join(folder_path, f'**/*.{extension.lower()}'), recursive=True))
        files.extend(glob.glob(os.path.join(folder_path, f'**/*.{extension.upper()}'), recursive=True))
    return files

# Extract the device name by folder hierarchy from file path
def extract_device(file_path):
    parts = file_path.split(os.path.sep)
    if len(parts) >= 2:
        return parts[1]
    return None

def extract_sample(relative_path):
    parts = relative_path.split(os.path.sep)
    if len(parts) >= 3:
        return parts[2]
    return None

def process_spreadsheet(spreadsheet):
    if ".csv" in spreadsheet.lower():
        df = pd.read_csv(spreadsheet, skiprows=[0])
    elif ".xls" in spreadsheet.lower():
        df = pd.read_excel(spreadsheet, skiprows=[-1])
    df_Vd = pd.DataFrame()
    df_Vg = pd.DataFrame()
    df_Id = pd.DataFrame()
    df_Ig = pd.DataFrame()
    df_BV = pd.DataFrame()
    df_time = pd.DataFrame()
    df_sorted = pd.DataFrame()
    # df.reset_index(inplace=True, drop=True)
    for col in (list(df.columns.values)):
        if col.startswith("V_D"):
            df_Vd = pd.concat([df_Vd, pd.DataFrame({"v_drain": df[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith("V_G"):
            df_Vg = pd.concat([df_Vg, pd.DataFrame({"v_gate": df[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith("I_D"):
            df_Id = pd.concat([df_Id, pd.DataFrame({"i_drain": df[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith("I_G"):
            df_Ig = pd.concat([df_Ig, pd.DataFrame({"i_gate": df[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith("BV"):
            df_BV = pd.concat([df_BV, pd.DataFrame({"bv": df[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith("time"):
            df_time = pd.concat([df_time, pd.DataFrame({"time": df[col]})], axis=0)
    df_sorted = pd.concat([df_Vg, df_Vd, df_Id, df_Ig, df_BV, df_time], axis=1)
    return df_sorted

def find_plateau_average_di(func_values, relative_tolerance=0.0001, max_difference_threshold=1):
    plateaus = []
    current_plateau = []

    for i in range(len(func_values) - 1):
        relative_difference = abs(func_values[i] - func_values[i + 1]) / max(abs(func_values[i]), abs(func_values[i + 1]))

        if relative_difference <= relative_tolerance:
            current_plateau.append(func_values[i])
        else:
            if current_plateau:
                current_plateau.append(func_values[i])
                plateaus.append(current_plateau)
                current_plateau = []

    # Include the last plateau
    if current_plateau:
        current_plateau.append(func_values[-1])
        plateaus.append(current_plateau)

    # Calculate average for each plateau
    plateau_averages = [np.mean(plateau) for plateau in plateaus]

    # Check if the max value of func_values is within the threshold of the max plateau average
    max_func_values = max(func_values)
    max_plateau_averages = max(plateau_averages)

    if abs(max_func_values - max_plateau_averages) <= max_difference_threshold:
        return plateau_averages
    else:
        return []

def process_spreadsheet_cv(spreadsheet, cp_type):
    if ".csv" in spreadsheet.lower():
        df = pd.read_csv(spreadsheet, skiprows=[0, 1, 2, 3])
    elif ".xls" in spreadsheet.lower():
        df = pd.read_excel(spreadsheet, skiprows=[0, 1, 2, 3])
    df_Vd = pd.DataFrame()
    df_Cp = pd.DataFrame()
    df_sorted = pd.DataFrame()
    # df.reset_index(inplace=True, drop=True)
    for col in (list(df.columns.values)):
        if col.startswith("DC"):
            numeric_values = pd.to_numeric(df[col], errors='coerce')
            numeric_rows = df[~np.isnan(numeric_values)]
            df_Vd = pd.concat([df_Vd, pd.DataFrame({"v_drain": numeric_rows[col]})], axis=0)
    for col in (list(df.columns.values)):
        if col.startswith(" Cp"):
            numeric_values = pd.to_numeric(df[col], errors='coerce')
            numeric_rows = df[~np.isnan(numeric_values)]
            df_Cp = pd.concat([df_Cp, pd.DataFrame({"c_p": numeric_rows[col]})], axis=0)
    df_sorted = pd.concat([df_Vd, df_Cp], axis=1)
    return df_sorted

def find_plateau_average(func_values, relative_tolerance=0.02, max_difference_threshold=50):
    plateaus = []
    current_plateau = []

    for i in range(len(func_values) - 1):
        relative_difference = abs(func_values[i] - func_values[i + 1]) / max(abs(func_values[i]), abs(func_values[i + 1]))

        if relative_difference <= relative_tolerance:
            current_plateau.append(func_values[i])
        else:
            if current_plateau:
                current_plateau.append(func_values[i])
                plateaus.append(current_plateau)
                current_plateau = []

    # Include the last plateau
    if current_plateau:
        current_plateau.append(func_values[-1])
        plateaus.append(current_plateau)

    # Calculate average for each plateau
    plateau_averages = [np.mean(plateau) for plateau in plateaus]

    # Check if the max value of func_values is within the threshold of the max plateau average
    max_func_values = max(func_values)
    max_plateau_averages = max(plateau_averages)

    if abs(max_func_values - max_plateau_averages) <= max_difference_threshold:
        return plateau_averages
    else:
        print("relative_tolerance needs to be adjusted.")
        return []

def smooth(y, box_pts):
    box = np.ones(box_pts)/box_pts
    y_smooth = np.convolve(y, box, mode='same')
    return y_smooth

def process_spreadsheet_dpt(spreadsheet):
    df = pd.read_csv(spreadsheet, usecols=[0, 1, 2, 3], names=['time', 'v_drain', 'i_drain', 'v_gate'],
                     header=None)

    time_values = df['time'].values * 1000000
    v_drain_values = df['v_drain'].values
    i_drain_values = df['i_drain'].values
    v_gate_values = df['v_gate'].values

    max_v_drain_index = np.argmax(v_drain_values)
    time_at_max_index = time_values[max_v_drain_index]
    # adapt the cut off time ex. 2
    new_time_zero = time_at_max_index - 2

    new_time_index = np.argmax(time_values >= new_time_zero)
    time_values, v_drain_values, i_drain_values, v_gate_values = (
        time_values[new_time_index:],
        v_drain_values[new_time_index:],
        i_drain_values[new_time_index:],
        v_gate_values[new_time_index:]
    )

    #Calculation of dvdt 10-90 Slope
    average_values_dv = find_plateau_average(v_drain_values)

    max_v_drain_index = np.argmax(v_drain_values)
    time_values_left_dv = time_values[:max_v_drain_index + 1]
    v_drain_values_left = v_drain_values[:max_v_drain_index + 1]
    time_values_right_dv, v_drain_values_right = (time_values[max_v_drain_index + 1:], v_drain_values[max_v_drain_index + 1:])

    plateau_dv = max(average_values_dv)
    closest_index_dv_t1 = np.argmin(np.abs(v_drain_values_left - 0.1 * plateau_dv))
    closest_index_dv_t2 = np.argmin(np.abs(v_drain_values_left - 0.9 * plateau_dv))
    closest_index_dv_t3 = np.argmin(np.abs(v_drain_values_right - 0.1 * plateau_dv))
    closest_index_dv_t4 = np.argmin(np.abs(v_drain_values_right - 0.9 * plateau_dv))

    # Corresponding time index
    left_t1_dv = time_values_left_dv[closest_index_dv_t1], v_drain_values_left[closest_index_dv_t1]
    left_t2_dv = time_values_left_dv[closest_index_dv_t2], v_drain_values_left[closest_index_dv_t2]
    right_t2_dv = time_values_right_dv[closest_index_dv_t3], v_drain_values_right[closest_index_dv_t3]
    right_t1_dv = time_values_right_dv[closest_index_dv_t4], v_drain_values_right[closest_index_dv_t4]

    turn_off_dvdt = (left_t2_dv[1] - left_t1_dv[1]) / (left_t2_dv[0] - left_t1_dv[0])
    turn_on_dvdt = (right_t2_dv[1] - right_t1_dv[1]) / (right_t2_dv[0] - right_t1_dv[0])

    n = 20
    # Calculation of didt 10-90 Slope
    i_drain_dump = i_drain_values * -1 + max(i_drain_values)
    i_drain_values_smooth = smooth(i_drain_dump, 10*n)
    #print(f"Max i_drain_values_smooth: {max(i_drain_values_smooth)}")
    max_i_drain_index = np.argmax(i_drain_values_smooth)
    time_values_left_di = time_values[:max_i_drain_index + 1]
    #print(f"time_values_left_di: {time_values_left_di}")
    i_drain_values_left = i_drain_values_smooth[:max_i_drain_index + 1]
    time_values_right_di, i_drain_values_right = (
    time_values[max_i_drain_index + 1:], i_drain_values_smooth[max_i_drain_index + 1:])

    # time values right müssen zweifach limitiert werden auf das minimum, was der kleine drop ist, mit time values wo gesamter graph maximum ist + 0.5 oder so, auf das neue minimum.

    time_at_max_di = time_values[max_i_drain_index]
    new_time_after_di_right = time_at_max_di + 2
    #print(f"new_time_after_di_right: {new_time_after_di_right}")

    new_time_index_di_right = np.argmin(time_values_right_di <= new_time_after_di_right)
    new_time_values_right_di = time_values_right_di[:new_time_index_di_right]
    #print(f"new_time_values_right_di: {new_time_values_right_di}")
    new_i_drain_values_right = i_drain_values_right[:new_time_index_di_right]

    new_min_i_drain_right_index = np.argmin(new_i_drain_values_right)
    new_time_values_right_di = time_values_right_di[:new_min_i_drain_right_index]
    #print(f"new_time_values_right_di: {new_time_values_right_di}")
    new_i_drain_values_right = i_drain_values_right[:new_min_i_drain_right_index]
    new_i_drain_values_right = new_i_drain_values_right - min(new_i_drain_values_right)

    average_values_di = find_plateau_average_di(i_drain_values_smooth)
    #print(f"average_values_di: {average_values_di}")
    plateau_di = max(average_values_di)-1
    print(f"plateau_di: {plateau_di}")

    closest_index_di_t1 = np.argmin(np.abs(i_drain_values_left - 0.1 * plateau_di))
    closest_index_di_t2 = np.argmin(np.abs(i_drain_values_left - 0.9 * plateau_di))
    closest_index_di_t3 = np.argmin(np.abs(new_i_drain_values_right - 0.1 * plateau_di))
    closest_index_di_t4 = np.argmin(np.abs(new_i_drain_values_right - 0.9 * plateau_di))

    # Corresponding time index
    left_t1_di = time_values_left_di[closest_index_di_t1], i_drain_values_left[closest_index_di_t1]
    left_t2_di = time_values_left_di[closest_index_di_t2], i_drain_values_left[closest_index_di_t2]
    right_t2_di = time_values_right_di[closest_index_di_t3], new_i_drain_values_right[closest_index_di_t3]
    right_t1_di = time_values_right_di[closest_index_di_t4], new_i_drain_values_right[closest_index_di_t4]

    turn_off_didt = -((left_t2_di[1] - left_t1_di[1]) / (left_t2_di[0] - left_t1_di[0]))
    turn_on_didt = -((right_t2_di[1] - right_t1_di[1]) / (right_t2_di[0] - right_t1_di[0]))

    print(turn_off_didt)
    print(turn_on_didt)

    # adapt the cut off time ex. 2
    time_after_turnon = time_values_right_dv[closest_index_dv_t3]
    new_time_after = time_after_turnon + 2

    new_time_index = np.argmin(time_values <= new_time_after)
    time_values, v_drain_values, i_drain_values, v_gate_values = (
        time_values[:new_time_index],
        v_drain_values[:new_time_index],
        i_drain_values[:new_time_index],
        v_gate_values[:new_time_index]
    )

    # Create a new df for DB
    df_sorted = pd.DataFrame({
        'time': time_values,
        'v_drain': v_drain_values,
        'i_drain': i_drain_values,
        'v_gate': v_gate_values
    })
    # Decrease resolution for faster population. Select every n-th row
    df_every_10th = df_sorted.iloc[::n]
    df_every_10th.reset_index(drop=True, inplace=True)
    df_sorted = df_every_10th

    return df_sorted, turn_off_dvdt, turn_on_dvdt, turn_off_didt, turn_on_didt

# Extract the table name by folder hierarchy from file path
def extract_table_name(file_path):
    parts = file_path.split(os.path.sep)
    return os.path.join(*parts[1:-1], os.path.splitext(parts[-1])[0])

# Check for empty rows and delete them (keysight/Keithley)
def drop_empty_rows(cur, table_name):
    check_empty_rows_query = sql.SQL('''
        DELETE FROM {} WHERE "v_drain" = 'NaN' AND "i_drain"  = 'NaN' AND "v_gate"  = 'NaN';
    ''').format(sql.Identifier(table_name))
    cur.execute(check_empty_rows_query)

# Check for empty columns and delete them (keysight/Keithley)
def drop_empty_columns(cur, table_name):
    check_empty_columns_query = sql.SQL('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = {};
    ''').format(sql.Literal(table_name))
    cur.execute(check_empty_columns_query)
    columns = [row[0] for row in cur.fetchall()]
    for column in columns:
        check_empty_query = sql.SQL('''
            SELECT COUNT({}) FROM {} WHERE {} IS NOT NULL;
        ''').format(
            sql.Identifier(column), sql.Identifier(table_name), sql.Identifier(column))
        cur.execute(check_empty_query)
        count = cur.fetchone()[0]
        if count == 0:
            drop_column_query = sql.SQL('''
                ALTER TABLE {} DROP COLUMN {};
            ''').format(
                sql.Identifier(table_name), sql.Identifier(column))
            cur.execute(drop_column_query)

# Drop columns named with a single letter followed by a colon (e.g., A:, I-V measurements)
def drop_letter_colon_columns(cur, table_name):
    check_columns_query = sql.SQL('''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = {};
    ''').format(sql.Literal(table_name))
    cur.execute(check_columns_query)
    columns = [row[0] for row in cur.fetchall()]

    for column in columns:
        if len(column) == 2 and column[1] == ':':
            drop_column_query = sql.SQL('''
                ALTER TABLE {} DROP COLUMN {};
            ''').format(
                sql.Identifier(table_name), sql.Identifier(column))
            cur.execute(drop_column_query)

# Get partial path for naming
def extract_first_part_of_path(file_path):
    parts = file_path.split(os.path.sep)
    if len(parts) >= 2:
        return parts[0]
    return None

abbreviations = {"hysteresis": "HYS", "hyteresis": "HYS", "threshold": "THOLD", "blocking": "BLKG", "repeat": "REP",
                 "zero": "0", "bodydiode": "BD"}

script_directory = os.path.dirname(__file__)

png_extensions, csv_extensions, pdf_extensions, xls_extensions = ['png'], ['csv'], ['pdf'], ['xls']
png_files, pdf_files, csv_files, xls_files = [find_files_with_extensions(script_directory, extensions)
                                              for extensions in
                                              [png_extensions, pdf_extensions, csv_extensions, xls_extensions]]

print(f"{len(png_files)} PNG paths, {len(pdf_files)} PDF, {len(csv_files)} CSV and {len(xls_files)} XLS files found.")

# PostgreSQL connection parameters
db_params = {
    'host': '0.0.0.0',
    'port': 5435,
    'database': 'mosfets',
    'user': 'postgres',
    'password': 'APSLab'
}

# Start the timer, establish connection and create cursor.
start_time = perf_counter()
con = psycopg2.connect(**db_params)
cur = con.cursor()
print("Database connection established.")

# Drop the custom tables before recreation
# drop_table_query = '''
#  DROP TABLE IF EXISTS pngfilepaths, idvd, didvgradient, lookupidentifiers, idvg, datasheetvth, datasheetrdson, cpvd, vthreshold, datasheetidvg;
# '''
# cur.execute(drop_table_query)
# con.commit()

# Query to get all table names from the information schema and drop them. CAUTION: DATA LOSS POSSIBLE IF MEASUREMENT (SPREADSHEET) NOT PRESENT IN FOLDER ANYMORE!
get_tables_query = sql.SQL("""
       SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = 'public'
   """)

cur.execute(get_tables_query)

table_names = cur.fetchall()

# Loop through table names and drop each table
for table_name in table_names:
    drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
        sql.Identifier(table_name[0])
    )
    cur.execute(drop_table_query)

print(f"{len(table_names)} Tables dropped.")

con.commit()

# Create table for PNG references
create_table_query = '''
  CREATE TABLE IF NOT EXISTS pngfilepaths (
      id SERIAL PRIMARY KEY,
      path VARCHAR(255) NOT NULL,
      device VARCHAR(50),
      sample VARCHAR(50)
  );
'''
cur.execute(create_table_query)

# Create table for MOSFET serial number identification by pdf datasheet
create_lookup_table_query = """
  CREATE TABLE IF NOT EXISTS lookupidentifiers (
      id SERIAL PRIMARY KEY,
      device VARCHAR(50),
      sample VARCHAR(255) NOT NULL,
      identifier VARCHAR(255) NOT NULL
  );
"""
cur.execute(create_lookup_table_query)
con.commit()

for png_file in png_files:
    relative_path = os.path.relpath(png_file, script_directory)
    device, sample = extract_device(relative_path), extract_sample(relative_path)
    insert_query = sql.SQL('INSERT INTO pngfilepaths (path, device, sample) VALUES ({}, {}, {})').format(
        sql.Literal(png_file), sql.Literal(device), sql.Literal(sample)
    )
    cur.execute(insert_query)
con.commit()

# Create table for MOSFET serial number identification
create_lookup_table_query = """
  CREATE TABLE IF NOT EXISTS dptslopes (
        id SERIAL PRIMARY KEY,
        device VARCHAR(50), 
        sample VARCHAR(255),
        tablename VARCHAR(255), 
        temperature INT DEFAULT '25', 
        identifier VARCHAR(50),
        turnoffdvdt DOUBLE PRECISION,
        turnondvdt DOUBLE PRECISION,
        turnoffdidt DOUBLE PRECISION,
        turnondidt DOUBLE PRECISION,
        dptvds INT,
        dptids INt
  );
"""
cur.execute(create_lookup_table_query)
con.commit()

# Combine csv and xls files as 'spreadsheets' to handle them similarly. Population every 100 files.
spreadsheets = csv_files + xls_files
i, commit_interval = 0, 100

# Process each spreadsheet file
for spreadsheet in spreadsheets:
    relative_path = os.path.relpath(spreadsheet, script_directory)
    device = extract_device(relative_path)
    sample = extract_sample(relative_path)
    first_part_of_path = extract_first_part_of_path(relative_path)
    relative_path_pdf = os.path.join(script_directory, first_part_of_path, device)
    pdf_files = find_files_with_extensions(relative_path_pdf, pdf_extensions)
    if pdf_files:
        pdf_name = os.path.splitext(os.path.basename(pdf_files[0]))[0]

    # Shorten the table name and replace "/" with "_"
    table_name = extract_table_name(relative_path)
    table_name = table_name.replace("/", "_")
    for word, abbreviation in abbreviations.items():
        table_name = table_name.replace(word, abbreviation)
    table_name = table_name.lower()

    # print(f"Now populating {device} with {sample} and {table_name} from {spreadsheet}")

    # Create the tables
    create_spreadsheet_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            device VARCHAR(50), 
            sample VARCHAR(255),
            tablename VARCHAR(255), 
            temperature INT DEFAULT '25', 
            identifier VARCHAR(50)
        );
    ''').format(sql.Identifier(table_name))
    cur.execute(create_spreadsheet_table_query)

    # Insert device, sample, tablename and identifier
    update_query = sql.SQL('''
        UPDATE {} SET device = {}, sample = {}, tablename = {}, identifier = {};
    ''').format(
        sql.Identifier(table_name), sql.Literal(device), sql.Literal(sample), sql.Literal(table_name),
        sql.Literal(pdf_name))
    cur.execute(update_query)

    # Insert temperature information based on filename
    match = re.search(r'_([0-9]+)c', table_name.lower())
    temperature = int(match.group(1)) if match else 25

    # Process CSV file and insert data into the table
    if "_cv_" in spreadsheet.lower():
        # Check for specific strings and assign cp_type accordingly
        cp_type_candidates = ["cds", "cgd", "cgg", "cgs"]
        cp_type = next((candidate.lower() for candidate in cp_type_candidates if candidate in spreadsheet.lower()),
                       "cp")
        if ".csv" in spreadsheet.lower():
            df_processed = process_spreadsheet_cv(spreadsheet, cp_type)
        elif ".xls" in spreadsheet.lower():
            df_processed = process_spreadsheet_cv(spreadsheet, cp_type)
    elif "dpt" in spreadsheet.lower():
        df_processed, turnoffdvdt, turnondvdt, turnoffdidt, turnondidt = process_spreadsheet_dpt(spreadsheet)
        match = re.search(r'_([0-9]+)v', table_name.lower())
        dptvds = int(match.group(1)) if match else 0
        match = re.search(r'_([0-9]+)a', table_name.lower())
        dptids = int(match.group(1)) if match else 0
        insert_query = sql.SQL('INSERT INTO dptslopes (device, sample, tablename, temperature, identifier, turnoffdvdt, turnondvdt, turnoffdidt, turnondidt, dptvds, dptids) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})').format(
            sql.Literal(device), sql.Literal(sample), sql.Literal(table_name), sql.Literal(temperature), sql.Literal(pdf_name), sql.Literal(turnoffdvdt), sql.Literal(turnondvdt), sql.Literal(turnoffdidt), sql.Literal(turnondidt), sql.Literal(dptvds), sql.Literal(dptids))
        cur.execute(insert_query)
    else:
        df_processed = process_spreadsheet(spreadsheet)

    # Dynamically generate the table columns based on the columns present in the XLS file
    for column in df_processed.columns:
        alter_table_query = sql.SQL('''
                ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} DOUBLE PRECISION;
            ''').format(sql.Identifier(table_name), sql.Identifier(column))
        cur.execute(alter_table_query)

    # Add Transconductance column if tablename contains "idvg"
    if "idvg" in table_name.lower():
        alter_table_query = sql.SQL('''
            ALTER TABLE {} ADD COLUMN IF NOT EXISTS transconductance DOUBLE PRECISION;
        ''').format(sql.Identifier(table_name))
        cur.execute(alter_table_query)

    # Add dptvds,dptids column if tablename contains "dpt"
    if "dpt" in table_name.lower():
        alter_table_query = sql.SQL('''
            ALTER TABLE {} ADD COLUMN IF NOT EXISTS dptvds DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS dptids DOUBLE PRECISION;
        ''').format(sql.Identifier(table_name))
        cur.execute(alter_table_query)

    # Populate data into respective table
    for index, row in df_processed.iterrows():
        columns = [sql.Identifier("device"), sql.Identifier("temperature"), sql.Identifier("sample"),
                   sql.Identifier("tablename"), sql.Identifier("identifier")]
        values = [sql.Literal(device), sql.Literal(temperature), sql.Literal(sample), sql.Literal(table_name),
                  sql.Literal(pdf_name)]
        if "dpt" in spreadsheet.lower():
            columns.extend([sql.Identifier("dptvds"), sql.Identifier("dptids")])
            values.extend([sql.Literal(dptvds),sql.Literal(dptids)])
        for column in df_processed.columns:
            columns.append(sql.Identifier(column))
            values.append(sql.Literal(row[column]))
        insert_data_query = sql.SQL('''
                INSERT INTO {} ({})
                VALUES ({})
                RETURNING id;  -- Add RETURNING id to get the generated id
            ''').format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(columns),
            sql.SQL(', ').join(values)
        )
        cur.execute(insert_data_query)
        inserted_id = cur.fetchone()[0]  # Get the generated id for Transconductance

        # Calculate Transconductance and update the table
        if "idvg" in table_name.lower():
            # Dynamically find column names based on prefixes
            v_gate_col = next((col for col in df_processed.columns if col.startswith("v_g")), None)
            i_drain_col = next((col for col in df_processed.columns if col.startswith("i_d")), None)

            if v_gate_col is not None and i_drain_col is not None:
                delta_id = row[i_drain_col] - df_processed[i_drain_col].shift(1).iloc[index]
                delta_vg = row[v_gate_col] - df_processed[v_gate_col].shift(1).iloc[index]

                # Avoid division by zero
                if delta_vg != 0:
                    transconductance = delta_id / delta_vg
                else:
                    transconductance = np.nan

                update_transconductance_query = sql.SQL('''
                            UPDATE {} SET transconductance = {}
                            WHERE id = {};
                        ''').format(
                    sql.Identifier(table_name),
                    sql.Literal(transconductance),
                    sql.Literal(inserted_id)  # Use the generated id
                )
                cur.execute(update_transconductance_query)

    # Drop empty columns, rows in the table, columns named with a single letter followed by a colon (e.g., A:)
    if "idvd" in table_name.lower() or "idvg" in table_name.lower():
        drop_empty_columns(cur, table_name)
        drop_empty_rows(cur, table_name)
        drop_letter_colon_columns(cur, table_name)

    i += 1
    if i % commit_interval == 0:
        con.commit()
con.commit()

csv_idvd_files = [csv_file for csv_file in csv_files if "idvd" in os.path.basename(csv_file).lower()]

for csv_file in csv_idvd_files:
    relative_path = os.path.relpath(csv_file, script_directory)
    first_part_of_path = extract_first_part_of_path(relative_path)
    sample = extract_sample(relative_path)
    device = extract_device(relative_path)
    relative_path_pdf = script_directory + "/" + first_part_of_path + "/" + device + "/"
    # Find PDF file in the device directory
    pdf_files = find_files_with_extensions(relative_path_pdf, pdf_extensions)
    if pdf_files:  # If there are any PDF files in the directory
        pdf_file = pdf_files[0]  # Take the first PDF file in case there are more
        pdf_name = os.path.splitext(os.path.basename(pdf_file))[0]  # Get the file name without extension

        # Insert the sample and PDF name into the lookup table
        insert_query = sql.SQL('INSERT INTO lookupidentifiers (device, sample, identifier) VALUES ({}, {}, {})').format(
            sql.Literal(device), sql.Literal(sample), sql.Literal(pdf_name)
        )
        cur.execute(insert_query)
con.commit()

# Round the V_Gate values to nearest integer
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%idvd%'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
""")
tables = [row[0] for row in cur.fetchall()]

# Then alter the column type
for table in tables:
    cur.execute(f"""
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '{table}'
      AND column_name ILIKE '%v_gate%';
  """)
    columns = [row[0] for row in cur.fetchall()]
    for column in columns:
        cur.execute(f"""
              ALTER TABLE "{table}"
              ALTER COLUMN "{column}" TYPE integer USING "{column}"::integer;
          """)
con.commit()

# Combine data from all tables and create a new table 'IDVD'.
cur.execute("""
    CREATE TABLE IF NOT EXISTS IDVD AS
    SELECT * FROM c2m_c2m_2_c2m_2_80_idvd_20 WHERE false; -- This is to create an empty table with the same structure
""".format(tables[0]))
con.commit()
for table in tables:
    cur.execute("""
        INSERT INTO IDVD
        SELECT * FROM "{}"
    """.format(table))
con.commit()

# Round the V_Drain values to the next multiple of 5 with a threshold of 1.5
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%idvg%'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
""")
tables = [row[0] for row in cur.fetchall()]

for table in tables:
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND column_name ILIKE '%v_drain%';
    """)
    columns = [row[0] for row in cur.fetchall()]

    # For each column, calculate the average and alter the column to avoid v_drain measurement drops when running into compliance
    for column in columns:
        cur.execute(f"""
            SELECT AVG("{column}") 
            FROM "{table}";
        """)
        avg_value = cur.fetchone()[0]
        rounded_value = avg_value + 1.5 if avg_value % 5.0 >= 1.5 else avg_value - (avg_value % 5.0)
        cur.execute(f"""
            ALTER TABLE "{table}"
            ALTER COLUMN "{column}" TYPE integer USING 
                CASE 
                    WHEN "{column}"::numeric % 5.0 >= 1.5 THEN CEIL("{column}" / 5.0) * 5
                    ELSE FLOOR("{column}" / 5.0) * 5
                END;
        """)

con.commit()

# Combine data from all tables and create a new table 'IDVG'
cur.execute("""
    CREATE TABLE IF NOT EXISTS IDVG AS
    SELECT * FROM c2m_c2m_2_c2m_2_80_idvg_20 WHERE false; -- This is to create an empty table with the same structure
""".format(tables[0]))
con.commit()
for table in tables:
    cur.execute("""
        INSERT INTO IDVG
        SELECT * FROM "{}"
    """.format(table))
con.commit()

from datasheetgm import create_and_populate_datasheetgm

create_and_populate_datasheetgm(cur, con)
#10-90 Transconductance
cur.execute("""
CREATE TABLE IF NOT EXISTS didvgradient AS
WITH drain_values AS (
    SELECT 
        device,
        sample,
        identifier, 
        temperature,
        tablename,
        v_drain,
        MAX((ABS(i_drain)))*0.9 AS Max_I_Drain9,
        MAX((ABS(i_drain)))*0.1 AS Max_I_Drain1
    FROM public.idvg
    GROUP BY device, sample, tablename, identifier, temperature, v_drain
),
closest_high AS (
    SELECT 
        dv.device,
        dv.sample,
        dv.tablename,
        dv.identifier, 
        dv.temperature,
        dv.v_drain,
        (SELECT i_drain 
         FROM public.idvg id 
         WHERE id.device = dv.device AND id.sample = dv.sample AND id.tablename = dv.tablename AND id.v_drain = dv.v_drain AND id.identifier = dv.identifier AND id.temperature = dv.temperature
         ORDER BY ABS(ABS(id.i_drain) - ABS(dv.Max_I_Drain9)) 
         LIMIT 1) AS i_drain
    FROM drain_values dv
),
closest_low AS (
    SELECT 
        dv.device,
        dv.sample,
        dv.tablename,
        dv.identifier, 
        dv.temperature,
        dv.v_drain,
        (SELECT i_drain 
         FROM public.idvg id 
         WHERE id.device = dv.device AND id.sample = dv.sample AND id.tablename = dv.tablename AND id.v_drain = dv.v_drain AND id.identifier = dv.identifier AND id.temperature = dv.temperature
         ORDER BY ABS(ABS(id.i_drain) - ABS(dv.Max_I_Drain1)) 
         LIMIT 1) AS i_drain
    FROM drain_values dv
)
SELECT 
    ch.device,
    ch.sample,
    ch.tablename,
    ch.identifier,
    ch.temperature,
    ch.v_drain,
    ch.i_drain AS closest_high_i_drain,
    id_high.v_gate AS corresponding_high_v_gate,
    cl.i_drain AS closest_low_i_drain,
    id_low.v_gate AS corresponding_low_v_gate
FROM closest_high ch
JOIN public.idvg id_high ON ch.device = id_high.device AND ch.sample = id_high.sample AND ch.tablename = id_high.tablename AND ch.v_drain = id_high.v_drain AND ch.i_drain = id_high.i_drain AND ch.identifier = id_high.identifier AND ch.temperature = id_high.temperature
JOIN closest_low cl ON ch.device = cl.device AND ch.sample = cl.sample AND ch.tablename = cl.tablename AND ch.v_drain = cl.v_drain AND ch.identifier = cl.identifier AND ch.temperature = cl.temperature
JOIN public.idvg id_low ON cl.device = id_low.device AND cl.sample = id_low.sample AND cl.tablename = id_low.tablename AND cl.v_drain = id_low.v_drain AND cl.i_drain = id_low.i_drain AND cl.identifier = id_low.identifier AND cl.temperature =id_low.temperature;
""")
con.commit()

#10-90 resistance
cur.execute("""
CREATE TABLE IF NOT EXISTS rdstenninety AS
WITH drain_values AS (
    SELECT 
        device,
        sample,
        identifier, 
        temperature,
        tablename,
        v_gate,
        MAX((ABS(i_drain)))*0.9 AS Max_I_Drain9,
        MAX((ABS(i_drain)))*0.1 AS Max_I_Drain1
    FROM public.idvd
    GROUP BY device, sample, tablename, identifier, temperature, v_gate
),
closest_high AS (
    SELECT 
        dv.device,
        dv.sample,
        dv.tablename,
        dv.identifier, 
        dv.temperature,
        dv.v_gate,
        (SELECT i_drain 
         FROM public.idvd id 
         WHERE id.device = dv.device AND id.sample = dv.sample AND id.tablename = dv.tablename AND id.v_gate = dv.v_gate AND id.identifier = dv.identifier AND id.temperature = dv.temperature
         ORDER BY ABS(ABS(id.i_drain) - ABS(dv.Max_I_Drain9)) 
         LIMIT 1) AS i_drain
    FROM drain_values dv
),
closest_low AS (
    SELECT 
        dv.device,
        dv.sample,
        dv.tablename,
        dv.identifier, 
        dv.temperature,
        dv.v_gate,
        (SELECT i_drain 
         FROM public.idvd id 
         WHERE id.device = dv.device AND id.sample = dv.sample AND id.tablename = dv.tablename AND id.v_gate = dv.v_gate AND id.identifier = dv.identifier AND id.temperature = dv.temperature
         ORDER BY ABS(ABS(id.i_drain) - ABS(dv.Max_I_Drain1)) 
         LIMIT 1) AS i_drain
    FROM drain_values dv
)
SELECT 
    ch.device,
    ch.sample,
    ch.tablename,
    ch.identifier,
    ch.temperature,
    ch.v_gate,
    ch.i_drain AS closest_high_i_drain,
    id_high.v_drain AS corresponding_high_v_drain,
    cl.i_drain AS closest_low_i_drain,
    id_low.v_drain AS corresponding_low_v_drain
FROM closest_high ch
JOIN public.idvd id_high ON ch.device = id_high.device AND ch.sample = id_high.sample AND ch.tablename = id_high.tablename AND ch.v_gate = id_high.v_gate AND ch.i_drain = id_high.i_drain AND ch.identifier = id_high.identifier AND ch.temperature = id_high.temperature
JOIN closest_low cl ON ch.device = cl.device AND ch.sample = cl.sample AND ch.tablename = cl.tablename AND ch.v_gate = cl.v_gate AND ch.identifier = cl.identifier AND ch.temperature = cl.temperature
JOIN public.idvd id_low ON cl.device = id_low.device AND cl.sample = id_low.sample AND cl.tablename = id_low.tablename AND cl.v_gate = id_low.v_gate AND cl.i_drain = id_low.i_drain AND cl.identifier = id_low.identifier AND cl.temperature =id_low.temperature;
""")
con.commit()

cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%_cv_%'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
""")
tables = [row[0] for row in cur.fetchall()]

# Combine data from all tables and create a new table 'cpvd'
cur.execute("""
    CREATE TABLE IF NOT EXISTS cpvd AS
    SELECT * FROM c2m_c2m_2_c2m_2_80_cv_cgg WHERE false; -- This is to create an empty table with the same structure
""".format(tables[0]))
con.commit()
for table in tables:
    cur.execute("""
        INSERT INTO cpvd
        SELECT * FROM "{}"
    """.format(table))

con.commit()

# Add a new column 'cp_type' to the 'cpvd' table
cur.execute("""
    ALTER TABLE cpvd
    ADD COLUMN cp_type VARCHAR(3);
""")

# Update the 'cp_type' column based on the 'tablename' column
for table in tables:
    cp_type = re.search(r'(cds|cgd|cgg|cgs)', table, flags=re.IGNORECASE).group(1)
    cur.execute("""
        UPDATE cpvd
        SET cp_type = %s
        WHERE tablename ILIKE %s
    """, (cp_type, f'%{table}%'))


con.commit()

cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%_vth%'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
""")

tables = [row[0] for row in cur.fetchall()]
# Combine data from all vth tables and create a new table 'vthreshold'
cur.execute("""
    CREATE TABLE IF NOT EXISTS vthreshold AS
    SELECT * FROM rt_sctkl_2_sctkl_2_80_vth WHERE false; -- This is to create an empty table with the same structure
""".format(tables[0]))
con.commit()
for table in tables:
    cur.execute("""
        INSERT INTO vthreshold
        SELECT * FROM "{}"
    """.format(table))


con.commit()

# Update i_drain to mA for Superset Numerical Range filter.
update_query = """
    UPDATE vthreshold
    SET i_drain = i_drain * 1000;
    """

# Execute the update query
cur.execute(update_query)
con.commit()

cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%_dpt%'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
""")
tables = [row[0] for row in cur.fetchall()]

if tables:
    # Combine data from all dpt tables and create a new table 'dptgraphs'
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dptgraphs AS
        SELECT * FROM c2m_c2m_2_c2m_2_80_dpt_800v_10a_25c WHERE false; -- This is to create an empty table with the same structure
    """.format(tables[0]))
    con.commit()
    for table in tables:
        cur.execute("""
            INSERT INTO dptgraphs
            SELECT * FROM "{}"
        """.format(table))


con.commit()

from datasheetvth import create_and_populate_datasheetvth

create_and_populate_datasheetvth(cur, con)

from datasheetrdson import create_and_populate_rdsondatasheet

create_and_populate_rdsondatasheet(cur, con)

con.commit()

cur.execute("""
CREATE TABLE IF NOT EXISTS rdsonplotds AS
SELECT *,
       NULL AS r_ds_max,
       NULL AS r_ds_graph_or_typ
FROM idvd
UNION ALL
SELECT id,
       device,
       sample,
       sample AS tablename,
       temperature,
       identifier,
       v_gate,
       NULL AS v_drain,
       i_drain,
       NULL AS i_gate,
       NULL AS time,
       r_ds_max,
       r_ds_graph_or_typ
FROM datasheetrdson;
""")
con.commit()

# List of index creation queries
index_queries = [
    "CREATE INDEX IF NOT EXISTS idx_filter_device ON didvgradient (device)",
    "CREATE INDEX IF NOT EXISTS idx_filter_sample ON rdsonplotds (sample)",
    "CREATE INDEX IF NOT EXISTS idx_filter_idvd ON idvd (v_gate, v_drain, i_drain, temperature)",
    "CREATE INDEX IF NOT EXISTS idx_filter_idvg ON idvd (sample, v_gate, v_drain)",
    "CREATE INDEX IF NOT EXISTS idx_filter_cpvd ON cpvd (cp_type, v_drain)",
    "CREATE INDEX IF NOT EXISTS idx_filter_vth ON vthreshold (i_drain)",
    "CREATE INDEX IF NOT EXISTS idx_filter_dpt ON dptgraphs (sample, dptvds, dptids, time)"
]

# Execute index creation queries
for query in index_queries:
    cur.execute(query)


con.commit()


# Stop the timer
end_time = perf_counter()

# Print the total time taken
print("All tables were created. Total operation took ", end_time - start_time, "seconds.")

