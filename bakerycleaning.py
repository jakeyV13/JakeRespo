import json
import pandas as pd

print("Script started")
# LOAD JSON FILE
with open("postransactionbalancefrom8-24-2024.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print("JSON loaded")

# HELPER FUNCTION
# Handles MongoDB-style date fields

def extract_date(obj):
    """Extract date value from MongoDB {"$date": ...} format"""
    if isinstance(obj, dict):
        return obj.get("$date")
    return obj


# PROCESS DATA

rows = []

for txn in data:
    # Basic transaction info
    username = txn.get("username")
    branch = txn.get("branch")
    firstname = txn.get("firstName")
    lastname = txn.get("lastName")

    # Extract and convert start time
    start_time_raw = extract_date(txn.get("startTime"))

    # Convert to datetime (UTC → PH time)
    start_dt_utc = pd.to_datetime(start_time_raw, errors="coerce", utc=True)

    if pd.notna(start_dt_utc):
        start_dt_ph = start_dt_utc.tz_convert("Asia/Manila")
        hour_num = start_dt_ph.hour          # Hour (0–23)
        day_name = start_dt_ph.day_name()    # Day name (e.g., Monday)
    else:
        start_dt_ph = None
        hour_num = None
        day_name = None

    # Get register items (can be list or JSON string)
    register_data = txn.get("register", [])

    if isinstance(register_data, str):
        try:
            register_data = json.loads(register_data)
        except json.JSONDecodeError:
            continue  # Skip invalid JSON

    # Flatten register items into rows
    for item in register_data:
        rows.append({
            "username": username,
            "branch": branch,
            "firstname": firstname,
            "lastname": lastname,
            "starttime_utc": start_time_raw,
            "starttime_ph": start_dt_ph,
            "hour_ph": hour_num,
            "day_ph": day_name,
            "productname": item.get("productName"),
            "category": item.get("category"),
            "initialqty": item.get("qty"),
            "remainingqty": item.get("remainingQty"),
            "soldqty": item.get("soldQty"),
            "price": item.get("price"),
            "amount": item.get("amount"),
        })

# CREATE DATAFRAME

df = pd.DataFrame(rows)

print(f"\nTOTAL ROWS: {len(df)}")

# CLEAN & CONVERT DATA TYPES

# Convert numeric columns
numeric_cols = ["initialqty", "remainingqty", "soldqty", "price", "amount", "hour_ph"]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

# Optimize memory using categorical types
categorical_cols = ["branch", "productname", "category", "username", "day_ph"]
for col in categorical_cols:
    if col in df.columns:
        df[col] = df[col].astype("category")

# SAVE OUTPUT
output_file = "bakery_full_cleaned_ph_time.csv"
df.to_csv(output_file, index=False)

print(f"\nSaved: {output_file}")