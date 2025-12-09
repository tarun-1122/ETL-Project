import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def load_to_supabase():
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)

    print("Loaded columns:", df.columns.tolist())  # Debug

    # --- Fix timestamp issues ---
    df["time"] = pd.to_datetime(df["time"], errors="coerce")\
                    .dt.strftime("%Y-%m-%dT%H:%M:%S")

    if "extracted_at" not in df.columns:
        print("⚠️ 'extracted_at' column missing — adding current timestamp.")
        df["extracted_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    else:
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce")\
                                .dt.strftime("%Y-%m-%dT%H:%M:%S")

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df), None).to_dict("records")

        values = [
            f"('{r['time']}', {r.get('temperature_c','NULL')}, {r.get('humidity_percent','NULL')}, "
            f"{r.get('wind_speed_kmph','NULL')}, '{r.get('city','Hyderabad')}', '{r['extracted_at']}')"
            for r in batch
        ]

        insert_sql = f"""
        INSERT INTO weather_data(time, temperature_c, humidity_percent, wind_speed_kmph, city, extracted_at)
        VALUES {",".join(values)}
        """

        supabase.rpc("execute_sql", {"query": insert_sql}).execute()
        print(f"Inserted rows {i+1} --- {min(i+batch_size, len(df))}")
        time.sleep(0.5)

    print("Finished Loading Weather data.")

if __name__ == "__main__":
    load_to_supabase()


#7ISd3WuG4oXnvPYMfzwp19koFGk8rFhA40AaZ0mG