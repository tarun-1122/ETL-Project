import pandas as pd
import os
import json,glob

def transform_weather_data():
    os.makedirs("../data/staged",exist_ok=True)

    latest_file=sorted(glob.glob("../data/raw/weather_*.json"))[-1]
    with open(latest_file,"r") as f:
        data=json.load(f)

    hourly=data["hourly"]
    df=pd.DataFrame({
        "time":hourly["time"],
        "temperature_c":hourly["relative_humidity_2m"],
        "wind_speed_kmph":hourly["wind_speed_10m"]
    })

    df["city"]="hyderabad"
    df["extrcted_at"]=pd.Timestamp.now()

    output_path="../data/staged/weather_cleaned.csv"
    df.to_csv(output_path,index=False)

    print(f"transformed {len(df)} weather recoeds saved to : {output_path}")
    return df
if __name__=="__main__":
    transform_weather_data()
