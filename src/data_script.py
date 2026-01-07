import pandas as pd
import requests
import os
import time

# Config
API_URL = "https://data.iowa.gov/resource/m3tr-qhgy.json"
OUTPUT_FILE = os.path.join("data", "diageo_2021_2025.csv")
BATCH_SIZE = 50000

COLUMN_MAPPING = {
    "invoice_line_no": "invoice_and_item_number",
    "date": "date",
    "store": "store_number",
    "name": "store_name",
    "address": "address",
    "city": "city",
    "zipcode": "zip_code",
    "county": "county",
    "category_name": "category_name",
    "vendor_name": "vendor_name",
    "itemno": "item_number",
    "im_desc": "item_description",
    "pack": "pack",
    "bottle_volume_ml": "bottle_volume_ml",
    "state_bottle_cost": "state_bottle_cost",
    "state_bottle_retail": "state_bottle_retail",
    "sale_bottles": "bottles_sold",
    "sale_dollars": "sale_dollars",
    "sale_liters": "volume_sold_liters"
}

FINAL_COLUMNS = [
    'invoice_and_item_number', 'date', 'store_number', 'store_name', 
    'address', 'city', 'zip_code', 'county', 
    'category_name', 'vendor_name', 'item_number', 'item_description', 
    'pack', 'bottle_volume_ml', 'state_bottle_cost', 'state_bottle_retail', 
    'bottles_sold', 'sale_dollars', 'volume_sold_liters'
]

# Data Cleaning and Transformation
def transform_data(df):
    if df.empty:
        return df
    
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Drop unnecessary columns
    if 'store_location' in df.columns:
        df = df.drop(columns=['store_location'])
    
    if 'vendor_name' in df.columns:
        df = df[df['vendor_name'].astype(str).str.contains('DIAGEO', case=False, na=False)]
    
    # Remove duplicates 
    if 'invoice_and_item_number' in df.columns:
        df = df.drop_duplicates(subset=['invoice_and_item_number'])
    else:
        df = df.drop_duplicates()

    # Standardize date format
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # Convert numeric columns
    numeric_cols = ['sale_dollars', 'bottles_sold', 'volume_sold_liters', 'state_bottle_cost', 'state_bottle_retail', 'pack', 'bottle_volume_ml']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Filter out invalid sales
    if 'sale_dollars' in df.columns:
        df = df.dropna(subset=['sale_dollars'])
        df = df[df['sale_dollars'] > 0]

    text_cols_raw = ['address', 'store_name', 'city', 'vendor_name', 'category_name', 'item_description']
    for col in text_cols_raw:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'[\r\n]+', ' ', regex=True)
            df[col] = df[col].str.upper().str.strip()
    
    # Clean zip codes
    if 'zip_code' in df.columns:
        df['zip_code'] = df['zip_code'].astype(str).str.replace(r'\.0$', '', regex=True)

    cols_to_keep = [c for c in FINAL_COLUMNS if c in df.columns]
    df = df[cols_to_keep]

    if 'invoice_and_item_number' in df.columns:
        filter_mask = df['invoice_and_item_number'].astype(str).str.contains(r'^(INV|S[0-9])', regex=True, case=False, na=False)
        df = df[filter_mask]

    if 'category_name' in df.columns:
        is_number = pd.to_numeric(df['category_name'], errors='coerce').notnull()
        df = df[~is_number]

    if 'vendor_name' in df.columns:
        is_number = pd.to_numeric(df['vendor_name'], errors='coerce').notnull()
        df = df[~is_number]
    
    if 'item_description' in df.columns:
        is_number = df['item_description'].astype(str).str.match(r'^\d+(\.\d+)?$', na=False)
        df = df[~is_number]

    return df

# Data Extraction and Loading
def run():
    needed_columns = list(COLUMN_MAPPING.keys())
    select_query = ", ".join(needed_columns)

    query_params = {
        "$where": "upper(vendor_name) like '%DIAGEO AMERICAS%' AND date >= '2021-01-01T00:00:00' AND date <= '2025-12-31T23:59:59'",
        "$limit": BATCH_SIZE,
        "$order": "date ASC",
        "$offset": 0,
        "$select": select_query
    }

    if os.path.exists(OUTPUT_FILE):
        try:
            os.remove(OUTPUT_FILE)
        except PermissionError:
            print("Gagal menghapus file lama. Tutup Power BI/Notepad lalu coba lagi.")
            return
    
    if not os.path.exists("data"):
        os.makedirs("data")

    print(f"Mengambil data Diageo (2021-2025)...")
    
    offset = 0
    batch_num = 1
    total_rows = 0
    start_time = time.time()

    while True:
        try:
            query_params["$offset"] = offset
            response = requests.get(API_URL, params=query_params)

            if response.status_code != 200:
                print(f"Error fetching data: {response.status_code}")
                break

            data = response.json()

            if not data:
                print("No more data to fetch.")
                break

            df_chunk = pd.DataFrame(data)
            df_clean = transform_data(df_chunk)

            write_header = (offset == 0)
            df_clean.to_csv(OUTPUT_FILE, mode='a', header=write_header, index=False)

            rows_saved = len(df_clean)
            total_rows += rows_saved

            print(f"Batch {batch_num}: Retrieved {len(data)} rows, Saved {rows_saved} cleaned rows. Total saved: {total_rows}")

            offset += BATCH_SIZE
            batch_num += 1
            
            # time.sleep(0.5)

        except Exception as e:
            print(f"An error occurred: {e}")
            break

        end_time = time.time()
        durasi = round((end_time - start_time) / 60, 2)
    
    print(f"Proses selesai dalam {durasi} menit. Total baris disimpan: {total_rows}")
    print(f"Data disimpan di: {OUTPUT_FILE}")

if __name__ == "__main__":
    run()