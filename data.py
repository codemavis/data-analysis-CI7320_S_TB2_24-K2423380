import pandas as pd
import psycopg2
import os
from pathlib import Path

# -----------------------------
# Configuration
# -----------------------------
data_folder = Path("/Users/sujee/Projects/MSc - Kingston University/Databases and Data Management/Coursework 2 - Database Design/Data Set")
output_folder = Path("./Cleaned_Data")
output_folder.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    "host": "localhost",
    "dbname": "CI7320_Coursework_2",
    "user": "postgres",
    "password": "nickky217"
}

# -----------------------------
# Global Master Dimension Tables
# -----------------------------
dim_date_master = pd.DataFrame(columns=['reporting_period', 'period_date', 'date_id'])
dim_airport_master = pd.DataFrame(columns=['reporting_airport', 'airport_id'])
dim_airline_master = pd.DataFrame(columns=['airline_name', 'airline_id'])
dim_route_master = pd.DataFrame(columns=['origin_destination', 'origin_destination_country', 'route_id'])
dim_flight_type_master = pd.DataFrame(columns=['scheduled_charter', 'flight_type_id'])

# -----------------------------
# Helper Function To Assign IDs
# -----------------------------
def assign_ids(df, master_df, key_cols, id_col):
    new_entries = df[key_cols].drop_duplicates()
    new_entries = new_entries[~new_entries.set_index(key_cols).index.isin(master_df.set_index(key_cols).index)]
    new_entries[id_col] = range(len(master_df) + 1, len(master_df) + 1 + len(new_entries))
    master_df = pd.concat([master_df, new_entries], ignore_index=True)
    df = df.merge(master_df, on=key_cols, how='left')
    return df, master_df

# -----------------------------
# Loop Through Each CSV File
# -----------------------------
for file_path in data_folder.glob("*_Punctuality_Statistics_UK_airports.csv"):
    try:
        month_tag = file_path.name[:3]
        print(f"📦 Processing: {file_path.name}")

        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='ISO-8859-1')

        # Add missing columns in 2023 set with dummy values
        required_columns = {
            'actual_flights_unmatched': 0,
            'flights_unmatched_percent': 0.0,
            'previous_year_month_flights_matched': 0,
            'run_date': pd.NaT
        }
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value

        # Drop rows with missing critical values
        df = df.dropna(subset=['reporting_airport', 'airline_name'])

        # Standardise text fields
        df['reporting_airport'] = df['reporting_airport'].str.strip().str.upper()
        df['airline_name'] = df['airline_name'].str.strip().str.upper()
        df['origin_destination'] = df['origin_destination'].str.strip().str.upper()
        df['origin_destination_country'] = df['origin_destination_country'].str.strip().str.upper()
        df['scheduled_charter'] = df['scheduled_charter'].str.strip().str.upper()

        # Convert date columns
        df['period_date'] = pd.to_datetime(df['reporting_period'].astype(str) + '01', format='%Y%m%d')
        df['run_date'] = pd.to_datetime(df['run_date'], errors='coerce')

        # Fill missing delay percentages and delay values with zero
        delay_cols = [col for col in df.columns if 'percent' in col or 'delay' in col]
        df[delay_cols] = df[delay_cols].fillna(0)

        # Assign consistent global IDs
        df, dim_date_master = assign_ids(df, dim_date_master, ['reporting_period', 'period_date'], 'date_id')
        df, dim_airport_master = assign_ids(df, dim_airport_master, ['reporting_airport'], 'airport_id')
        df, dim_airline_master = assign_ids(df, dim_airline_master, ['airline_name'], 'airline_id')
        df, dim_route_master = assign_ids(df, dim_route_master, ['origin_destination', 'origin_destination_country'], 'route_id')
        df, dim_flight_type_master = assign_ids(df, dim_flight_type_master, ['scheduled_charter'], 'flight_type_id')

        # Create fact table
        fact_punctuality = df[[
            'date_id', 'airport_id', 'airline_id', 'route_id', 'flight_type_id',
            'number_flights_matched', 'actual_flights_unmatched', 'number_flights_cancelled',
            'flights_more_than_15_minutes_early_percent',
            'flights_15_minutes_early_to_1_minute_early_percent',
            'flights_0_to_15_minutes_late_percent',
            'flights_between_16_and_30_minutes_late_percent',
            'flights_between_31_and_60_minutes_late_percent',
            'flights_between_61_and_120_minutes_late_percent',
            'flights_between_121_and_180_minutes_late_percent',
            'flights_between_181_and_360_minutes_late_percent',
            'flights_more_than_360_minutes_late_percent',
            'flights_unmatched_percent', 'flights_cancelled_percent',
            'average_delay_mins',
            'previous_year_month_flights_matched',
            'previous_year_month_early_to_15_mins_late_percent',
            'previous_year_month_average_delay'
        ]]

        fact_punctuality.to_csv(output_folder / f"fact_punctuality_{month_tag}.csv", index=False)
        print(f"✅ Saved fact_punctuality_{month_tag}.csv")

    except Exception as e:
        print(f"❌ Error processing {file_path.name}: {e}")

# -----------------------------
# Save Global Dimension Tables
# -----------------------------
dim_date_master.to_csv(output_folder / "dim_date.csv", index=False)
dim_airport_master.to_csv(output_folder / "dim_airport.csv", index=False)
dim_airline_master.to_csv(output_folder / "dim_airline.csv", index=False)
dim_route_master.to_csv(output_folder / "dim_route.csv", index=False)
dim_flight_type_master.to_csv(output_folder / "dim_flight_type.csv", index=False)

print("✅ Global dimension tables saved.")

# -----------------------------
# Insert into PostgreSQL
# -----------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

for _, row in dim_airport_master.iterrows():
    cur.execute("INSERT INTO Dim_Airport (airport_id, airport_name) VALUES (%s, %s)",
                (row['airport_id'], row['reporting_airport']))

for _, row in dim_airline_master.iterrows():
    cur.execute("INSERT INTO Dim_Airline (airline_id, airline_name) VALUES (%s, %s)",
                (row['airline_id'], row['airline_name']))

for _, row in dim_route_master.iterrows():
    cur.execute("INSERT INTO Dim_Route (route_id, origin_destination, origin_destination_country) VALUES (%s, %s, %s)",
                (row['route_id'], row['origin_destination'], row['origin_destination_country']))

for _, row in dim_flight_type_master.iterrows():
    cur.execute("INSERT INTO Dim_Flight_Type (flight_type_id, scheduled_charter) VALUES (%s, %s)",
                (row['flight_type_id'], row['scheduled_charter']))

for _, row in dim_date_master.iterrows():
    cur.execute("INSERT INTO Dim_Date (date_id, full_date, month, year, reporting_period) VALUES (%s, %s, %s, %s, %s)",
                (row['date_id'], row['period_date'], row['period_date'].month, row['period_date'].year, row['reporting_period']))

for _, row in fact_punctuality.iterrows():
    cur.execute("""
        INSERT INTO Fact_Punctuality (
            date_id, airport_id, airline_id, route_id, flight_type_id,
            number_flights_matched, actual_flights_unmatched, number_flights_cancelled,
            flights_more_than_15_minutes_early_percent,
            flights_15_minutes_early_to_1_minute_early_percent,
            flights_0_to_15_minutes_late_percent,
            flights_between_16_and_30_minutes_late_percent,
            flights_between_31_and_60_minutes_late_percent,
            flights_between_61_and_120_minutes_late_percent,
            flights_between_121_and_180_minutes_late_percent,
            flights_between_181_and_360_minutes_late_percent,
            flights_more_than_360_minutes_late_percent,
            flights_unmatched_percent, flights_cancelled_percent,
            average_delay_mins,
            previous_year_month_flights_matched,
            previous_year_month_early_to_15_mins_late_percent,
            previous_year_month_average_delay
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
cur.close()
conn.close()

print(f"✅ Inserted data into PostgreSQL")
