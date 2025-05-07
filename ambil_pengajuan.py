import re
import os
from dotenv import load_dotenv
import requests
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import pymysql
from pymysql.err import IntegrityError

load_dotenv()
ssh_host = os.getenv("SSH_HOST")
ssh_port = os.getenv("SSH_PORT")
ssh_user = os.getenv("SSH_USER")
ssh_password = os.getenv("SSH_PASSWORD")

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def helper():
    session = requests.Session()
    username = "bap_jatim"
    password = "pdmjatim"
    info = {"username": username, "password": password}
    url = "https://apps.ban-pdm.id/sispena3/login/proses"

    login = session.post(url, data=info)

    if login.url == "https://apps.ban-pdm.id/sispena3/dashboard":
        print("Login Berhasil")
        return session  # Return session instead of a status code

    print("Login Gagal")
    return None  # Return None if login fails

# Function to fetch pengajuan data


def get_pengajuan(session):
    payload = {
        "start": 0,
        "length": -1}
    url = "https://apps.ban-pdm.id/sispena3/validasi_ajuan_sertifikat/listdata/ajax_list"
    result = session.post(url, data=payload)
    return result.json()

# Function to extract ID from HTML anchor tags


def extract_id(html_text):
    match = re.search(r'<a [^>]*>(\d+)</a>', html_text)
    return match.group(1) if match else None


def extract_href(html_text):
    match = re.search(r'<a href="([^"]+)"', html_text)
    return match.group(1) if match else None


def extract_phone_number(text):
    if not isinstance(text, str):  # Ensure input is a valid string
        return None

    match = re.search(
        r'\b(?:\+62|62|0)?(?:\d{2,4}[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}\b', text)

    if match:
        phone_number = match.group(0)
        formatted_number = re.sub(
            r'^\+?62', '0', phone_number)  # Convert +62/62 to 0
        return formatted_number

    return None



# Function to process API response into a DataFrame


def process_pengajuan_response(data):
    if "data" in data:
        df = pd.DataFrame(data["data"])  # Convert JSON list to DataFrame
        # Select rows 2, 4, and 6 for extraction

        if 'html_column' in df.columns:  # Replace 'html_column' with actual column name
            df_filtered = df.loc[[2]]
            df_filtered['extracted_id'] = df_filtered['html_column'].apply(
                extract_id)
            print(df_filtered)
            return df_filtered
    else:
        print("Invalid response format")
        return None


# Running the process
def pengrapian_data(extract_id, extract_href, extract_phone_number, data):
    df = pd.DataFrame(data["data"])  # Convert JSON list to DataFrame
    #     # Define the column indexes you want to clean
    columns_to_clean = [2]
    #     # Apply the extraction function only to those columns
    df.iloc[:, columns_to_clean] = df.iloc[:,
                                           columns_to_clean].applymap(extract_id)
    # Define the column indexes where we want to extract the URL
    columns_to_extract_href = [11, 12]
    # Apply the URL extraction function only to columns 12 and 13
    df.iloc[:, columns_to_extract_href] = df.iloc[:,
                                                  columns_to_extract_href].applymap(extract_href)
    phone_columns = [8]
    df.iloc[:, phone_columns] = df.iloc[:,
                                        phone_columns].applymap(extract_phone_number)

    column_names = {2: "npsn",
                    3: "nama_lembaga",
                    4: "jenjang",
                    6: "kabkota",
                    8: "no_hp",
                    9: "peringkat_akreditasi",
                    10: "thn_akreditasi",
                    11: "surat_permohonan",
                    12: "sertifikat"}
    print("Before renaming:", df.columns)
    # Rename the columns in your DataFrame
    df.rename(columns=column_names, inplace=True)
    # Display the cleaned DataFrame
    selected_columns = df[list(column_names.values())]
    return column_names, selected_columns


session = helper()


def connection_sql():
    connection = None
    try:
        # Establish SSH tunnel
        # tunnel = SSHTunnelForwarder(
        #     (ssh_host, ssh_port),
        #     ssh_username=ssh_user,
        #     ssh_password=ssh_password,
        #     remote_bind_address=(db_host, 3306)
        # )
        # tunnel.start()

        connection = pymysql.connect(
            host="127.0.0.1", port=3306,
            user="root", password="", database="bansmjatim",
            cursorclass=pymysql.cursors.DictCursor
        )

    except Exception as e:
        print(f"Error: {e}")
        print(f"SSH Tunnel Error: {e}")
        return None

    return connection

def insert_sql(connection, cursor, table, unique_column, new_records):
    if new_records.empty:
        print("No new records to insert.")
        return

    new_records = new_records.reset_index(
        drop=False)  # Ensure 'npsn' is a column

    # for start in range(0, len(new_records), 1000):  # Batch insert
    #     batch_df = new_records.iloc[start:start+1000]

    #     # Remove rows with empty 'npsn'
    #     batch_df = batch_df[batch_df[unique_column].notna() & (
    #         batch_df[unique_column] != '')]

    #     if batch_df.empty:
    #         empty_new_records = new_records[new_records[unique_column].isna() | new_records[unique_column].eq('')]

    #         print(f"Skipping {empty_new_records} records with empty NPSN")
    #         continue
    # Remove rows with empty 'npsn'
    new_records = new_records[new_records[unique_column].notna() & (
        new_records[unique_column] != '')]

    # Count & print skipped records
    empty_new_records = new_records[new_records[
        unique_column].isna(
    ) | new_records[unique_column].eq('')]
    print(
        f"Skipping insert for {len(empty_new_records)} records with empty NPSN")
    columns = ', '.join(new_records.columns)
    placeholders = ', '.join(['%s'] * len(new_records.columns))

    # Use `ON DUPLICATE KEY UPDATE` to handle duplicate entries efficiently
    insert_sql = f""" INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE  {', '.join([f"{col} = VALUES({col})" for col in new_records.columns if col != unique_column])}"""

    values = [tuple(row) for row in new_records.to_numpy()]

    try:
        cursor.executemany(insert_sql, values)
        connection.commit()
        print(f"Inserted/Updated {len(new_records)} records successfully!")
    except IntegrityError as e:
        print(f"IntegrityError: Skipping duplicate entries - {e}")
    except pymysql.MySQLError as e:
        print(f"MySQL Error during insert: {e}")




def update_sql(connection, cursor, table, unique_column, updated_records):
    if updated_records.empty:
        print("No records to update.")
        return
    updated_records = updated_records.reset_index(
        drop=False)  # Ensure 'npsn' stays a column

    for _, row in updated_records.iterrows():
        update_sql = f"""UPDATE {table} SET {', '.join([f"{col} = %s" for col in updated_records.columns if col != unique_column])} WHERE {
            unique_column} = %s """
        try:
            cursor.execute(update_sql, list(
                row.drop(unique_column, errors='ignore')) + [row[unique_column]])
        except pymysql.MySQLError as e:
            print(f"MySQL Error updating record {row[unique_column]}: {e}")

    connection.commit()
    print(f"Updated {len(updated_records)} records successfully!")


def comparing_df(selected_columns, unique_column, existing_df):
    scrapped_df = selected_columns.set_index(unique_column)
    existing_df = existing_df.set_index(unique_column)

    # Drop rows where unique_column is missing
    # scrapped_df = scrapped_df.dropna(subset=[unique_column])
    # existing_df = existing_df.dropna(subset=[unique_column])

    # Ensure both DataFrames contain the same columns for comparison
    matching_columns = scrapped_df.columns.intersection(
        existing_df.columns)
    scrapped_df = scrapped_df[matching_columns]
    existing_df = existing_df[matching_columns]

    # Identify matching records based on index
    matching_existing_df = existing_df.loc[existing_df.index.intersection(
        scrapped_df.index)]

    # Sort indexes to align properly before comparison
    scrapped_df = scrapped_df.sort_index()
    matching_existing_df = matching_existing_df.sort_index()

    # 🔹 FIX: Fill missing values to prevent TypeErrors
    scrapped_df = scrapped_df.fillna("")
    matching_existing_df = matching_existing_df.fillna("")

    # 🔹 FIX: Convert all columns to string type before comparing
    scrapped_df = scrapped_df.astype(str)
    matching_existing_df = matching_existing_df.astype(str)

    # Filter rows where **any column value has changed**
    # Ensure existing_df is not empty and has at least 1 row
    if not existing_df.empty and len(existing_df) > 1:
        updated_records = scrapped_df[scrapped_df.ne(
            matching_existing_df).any(axis=1)]
    else:
        # If existing_df has 0 or 1 row, just return scrapped_df as updates
        updated_records = scrapped_df

        # Identify new rows that do not exist in existing_df
    new_records = scrapped_df.loc[~scrapped_df.index.isin(
        existing_df.index)]

    return updated_records, new_records


if session:
    data = get_pengajuan(session)  # Print the raw data for debugging
    if "data" in data:
        column_names, selected_columns = pengrapian_data(
            extract_id, extract_href, extract_phone_number, data)

        connection = connection_sql()
        cursor = connection.cursor()
        # df.to_csv("pengajuan.csv")
        table = "tb_verifikasi_perpanjangan_paud"
        unique_column = "npsn"
        # Convert column_names dictionary to a list of column names

        # Fetch existing data
        # existing_df = pd.read_sql(f"SELECT * FROM {table}", connection)
        columns = [unique_column, "nama_lembaga", "jenjang", "kabkota", "no_hp", "peringkat_akreditasi", "thn_akreditasi",
                   "surat_permohonan", "sertifikat", "status_permohonan", "status_sertifikat", "approve"]

        sql_data = cursor.execute(f"SELECT * FROM {table}")
        existing = cursor.fetchall()
        existing_df = pd.DataFrame(existing, columns=columns)
        # selected_columns.to_csv("scrapped.csv")
        # existing_df.to_csv("existing.csv")

        updated_records, new_records = comparing_df(
            selected_columns, unique_column, existing_df)

        updated_records.loc[:, ["status_permohonan",
                                "status_sertifikat", "approve"]] = 0
        print(
            """success update DF for column ("status_permohonan", "status_sertifikat", "approve") """)
        new_records.loc[:, ["status_permohonan",
                            "status_sertifikat", "approve"]] = 0
        print(
            """success insert DF for column ("status_permohonan", "status_sertifikat", "approve") """)
        print("Update DF shape:", updated_records.shape)
        print("Insert DF shape:", new_records.shape)
        if not updated_records.empty:

            # Ensure 'npsn' is back as a column
            update_sql(connection, cursor, table,
                       unique_column, updated_records)

        if not new_records.empty:

            insert_sql(connection, cursor, table, unique_column, new_records)
