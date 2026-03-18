"""
mobi_clean.py
=============
Part of: mobi-bikeshare-analytics
https://github.com/Azure0708/mobi-bikeshare-analytics

Cleans and joins all Mobi bike-share data files into five Tableau-ready CSVs.

OUTPUT FILES
------------
1. trips_clean.csv          – One row per trip (core fact table)
2. station_stats.csv        – One row per station (demand, flow, e-bike %)
3. od_pairs.csv             – Top origin-destination pairs (flow map)
4. daily_summary.csv        – Daily aggregates joined with weather
5. station_coords.csv       – Station name → lat/lon lookup (edit to add coords)

USAGE
-----
  python mobi_clean.py

Place all Mobi_System_Data_2025-XX.csv files, Vancouver weather CSV,
and bikeways_clean.csv in the same folder as this script, OR edit
the DATA_DIR / WEATHER_FILE / BIKEWAYS_FILE paths below.
"""

import pandas as pd
import glob
import os
import re

# ─── PATHS (edit if your files are elsewhere) ─────────────────────────────────
DATA_DIR     = "."          # folder containing Mobi_System_Data_*.csv
WEATHER_FILE = "Vancouver 2025-01-01 to 2025-09-30.csv"
BIKEWAYS_FILE = "bikeways_clean.csv"
OUT_DIR      = "tableau_data"   # output folder
# ──────────────────────────────────────────────────────────────────────────────

os.makedirs(OUT_DIR, exist_ok=True)

print("=" * 60)
print("mobi-bikeshare-analytics  |  Data Cleaner")
print("=" * 60)


# ─── 1. LOAD & COMBINE ALL MONTHLY TRIP FILES ─────────────────────────────────
print("\n[1/5] Loading monthly trip files...")

def load_month(filepath):
    df = pd.read_csv(filepath, low_memory=False)

    # ── Normalise e-bike column (named differently across months) ──
    if 'Electric bike' in df.columns:
        df['is_ebike'] = df['Electric bike'].astype(str).str.strip().str.upper() == 'TRUE'
    elif 'Electric' in df.columns:
        df['is_ebike'] = df['Electric'].astype(str).str.strip().str.upper() == 'TRUE'
    else:
        df['is_ebike'] = False

    # ── Normalise membership column (May uses 'Formula') ──
    if 'Formula' in df.columns and 'Membership type' not in df.columns:
        df.rename(columns={'Formula': 'Membership type'}, inplace=True)

    # ── Normalise temperature columns (degree symbol differs) ──
    for col in list(df.columns):
        if re.search(r'Departure temperature', col, re.IGNORECASE):
            df.rename(columns={col: 'dep_temp_c'}, inplace=True)
        elif re.search(r'Return temperature', col, re.IGNORECASE):
            df.rename(columns={col: 'ret_temp_c'}, inplace=True)

    # ── Keep only the columns we need ──
    keep = [
        'Departure', 'Return',
        'Bike', 'is_ebike',
        'Departure station', 'Return station',
        'Membership type',
        'Covered distance (m)', 'Duration (sec.)',
        'dep_temp_c', 'ret_temp_c',
    ]
    df = df[[c for c in keep if c in df.columns]]
    return df

files = sorted(glob.glob(os.path.join(DATA_DIR, "Mobi_System_Data_2025-*.csv")))
if not files:
    raise FileNotFoundError(f"No Mobi CSV files found in: {DATA_DIR}")

raw = pd.concat([load_month(f) for f in files], ignore_index=True)
print(f"   Loaded {len(raw):,} raw trips from {len(files)} files.")


# ─── 2. CLEAN TRIPS ───────────────────────────────────────────────────────────
print("\n[2/5] Cleaning trips...")

df = raw.copy()

# Parse datetimes
df['departure_dt'] = pd.to_datetime(df['Departure'], errors='coerce')
df['return_dt']    = pd.to_datetime(df['Return'],    errors='coerce')

# Drop rows with no valid departure time or station
df = df.dropna(subset=['departure_dt', 'Departure station', 'Return station'])

# Remove blank / whitespace-only station names
df = df[df['Departure station'].str.strip() != '']
df = df[df['Return station'].str.strip()   != '']

# Strip leading station ID (e.g. "0069 7th & Granville" → keep full for display,
# but also make a clean name without the numeric prefix)
df['dep_station']       = df['Departure station'].str.strip()
df['ret_station']       = df['Return station'].str.strip()
df['dep_station_name']  = df['dep_station'].str.replace(r'^\d{4}\s*', '', regex=True).str.strip()
df['ret_station_name']  = df['ret_station'].str.replace(r'^\d{4}\s*', '', regex=True).str.strip()
df['dep_station_id']    = df['dep_station'].str.extract(r'^(\d{4})', expand=False)
df['ret_station_id']    = df['ret_station'].str.extract(r'^(\d{4})', expand=False)

# Derived time fields (useful Tableau dimensions)
df['departure_date']    = df['departure_dt'].dt.date
df['departure_hour']    = df['departure_dt'].dt.hour
df['departure_dow']     = df['departure_dt'].dt.day_name()          # Monday … Sunday
df['departure_dow_num'] = df['departure_dt'].dt.dayofweek           # 0=Mon … 6=Sun
df['departure_month']   = df['departure_dt'].dt.month
df['departure_month_name'] = df['departure_dt'].dt.strftime('%B')   # January …
df['departure_week']    = df['departure_dt'].dt.isocalendar().week.astype(int)

# Season
def season(m):
    if m in (12, 1, 2):  return 'Winter'
    if m in (3, 4, 5):   return 'Spring'
    if m in (6, 7, 8):   return 'Summer'
    return 'Fall'
df['season'] = df['departure_month'].apply(season)

# Numeric columns
df['distance_km']   = pd.to_numeric(df['Covered distance (m)'], errors='coerce') / 1000
df['duration_min']  = pd.to_numeric(df['Duration (sec.)'],      errors='coerce') / 60

# Remove obviously invalid trips (negative / zero duration, distance > 200 km)
df = df[(df['duration_min'] > 0) & (df['duration_min'] < 1440)]
df = df[(df['distance_km'] >= 0) & (df['distance_km'] < 200)]

# Membership simplification (for cleaner Tableau legends)
def simplify_membership(m):
    m = str(m).strip()
    if 'Annual' in m or '365' in m:    return 'Annual Pass'
    if 'Pay as you go' in m:           return 'Pay-As-You-Go'
    if 'Pay Per Ride' in m:            return 'Pay-Per-Ride'
    if 'UBC' in m:                     return 'UBC Corporate'
    if 'Community' in m:               return 'Community Pass'
    if 'Corporate' in m or 'Business' in m: return 'Corporate'
    if 'Student' in m:                 return 'Student'
    if 'Monthly' in m:                 return 'Monthly Pass'
    return 'Other'

df['membership_simple'] = df['Membership type'].apply(simplify_membership)

# Round-trip flag
df['is_round_trip'] = df['dep_station'] == df['ret_station']

# Temp columns (fill missing with NaN — will join weather later)
df['dep_temp_c'] = pd.to_numeric(df.get('dep_temp_c', pd.NA), errors='coerce')

# Final column selection for trips_clean
trips = df[[
    'departure_dt', 'return_dt',
    'departure_date', 'departure_hour', 'departure_dow', 'departure_dow_num',
    'departure_month', 'departure_month_name', 'departure_week', 'season',
    'dep_station', 'ret_station',
    'dep_station_id', 'ret_station_id',
    'dep_station_name', 'ret_station_name',
    'is_ebike', 'is_round_trip',
    'distance_km', 'duration_min',
    'dep_temp_c', 'membership_simple',
]].copy()

print(f"   Clean trips: {len(trips):,}  (dropped {len(raw)-len(trips):,} invalid rows)")


# ─── 3. JOIN WEATHER ──────────────────────────────────────────────────────────
print("\n[3/5] Joining weather data...")

weather = pd.read_csv(WEATHER_FILE)
weather['date'] = pd.to_datetime(weather['datetime']).dt.date

weather_cols = [
    'date', 'tempmax', 'tempmin', 'temp', 'feelslike',
    'precip', 'precipprob', 'snow', 'windspeed', 'cloudcover',
    'uvindex', 'conditions', 'icon', 'humidity',
]
weather = weather[[c for c in weather_cols if c in weather.columns]]
weather.rename(columns={
    'temp': 'weather_temp_avg',
    'tempmax': 'weather_temp_max',
    'tempmin': 'weather_temp_min',
    'feelslike': 'weather_feelslike',
    'precip': 'weather_precip_mm',
    'precipprob': 'weather_precip_prob',
    'snow': 'weather_snow',
    'windspeed': 'weather_windspeed',
    'cloudcover': 'weather_cloudcover',
    'uvindex': 'weather_uvindex',
    'humidity': 'weather_humidity',
    'conditions': 'weather_conditions',
    'icon': 'weather_icon',
}, inplace=True)

# Bucket weather conditions for cleaner Tableau filters
def bucket_weather(cond):
    cond = str(cond).lower()
    if 'snow' in cond:             return 'Snow'
    if 'rain' in cond:             return 'Rain'
    if 'overcast' in cond:         return 'Overcast'
    if 'partially cloudy' in cond \
       or 'partly cloudy' in cond: return 'Partly Cloudy'
    if 'clear' in cond:            return 'Clear'
    return 'Other'

weather['weather_bucket'] = weather['weather_conditions'].apply(bucket_weather)

trips = trips.merge(weather, left_on='departure_date', right_on='date', how='left')
trips.drop(columns=['date'], inplace=True)
print(f"   Weather joined. Matched: {trips['weather_temp_avg'].notna().sum():,} trips.")


# ─── 4. STATION STATS ─────────────────────────────────────────────────────────
print("\n[4/5] Building station stats...")

dep_counts = trips.groupby('dep_station').agg(
    departures       = ('dep_station', 'count'),
    avg_distance_km  = ('distance_km', 'mean'),
    avg_duration_min = ('duration_min', 'mean'),
    ebike_trips      = ('is_ebike', 'sum'),
    round_trips      = ('is_round_trip', 'sum'),
).reset_index().rename(columns={'dep_station': 'station'})

ret_counts = trips.groupby('ret_station').agg(
    returns = ('ret_station', 'count'),
).reset_index().rename(columns={'ret_station': 'station'})

station_stats = dep_counts.merge(ret_counts, on='station', how='outer')
station_stats['departures'] = station_stats['departures'].fillna(0).astype(int)
station_stats['returns']    = station_stats['returns'].fillna(0).astype(int)
station_stats['total_trips']   = station_stats['departures'] + station_stats['returns']
station_stats['net_flow']      = station_stats['departures'] - station_stats['returns']
# Positive net_flow = more departures (SOURCE / overflow)
# Negative net_flow = more returns   (SINK / needs rebalancing)
station_stats['flow_type'] = station_stats['net_flow'].apply(
    lambda x: 'High Source' if x > 50 else ('High Sink' if x < -50 else 'Balanced')
)
station_stats['ebike_pct'] = (
    station_stats['ebike_trips'] / station_stats['departures'].replace(0, float('nan')) * 100
).round(1)
station_stats['round_trip_pct'] = (
    station_stats['round_trips'] / station_stats['departures'].replace(0, float('nan')) * 100
).round(1)

# Extract station ID and clean name
station_stats['station_id']   = station_stats['station'].str.extract(r'^(\d{4})', expand=False)
station_stats['station_name'] = station_stats['station'].str.replace(r'^\d{4}\s*', '', regex=True).str.strip()

# ── Coordinates Placeholder ──────────────────────────────────────────────────
# Mobi publishes station coordinates at:
#   https://vancouver.publicbikesystem.net/ube/gbfs/v1/en/station_information
# Download that JSON and run the helper script (see README) to auto-fill coords.
# For now, the lat/lon columns exist so Tableau can use them once populated.
station_stats['latitude']  = None
station_stats['longitude'] = None

print(f"   Stations processed: {len(station_stats):,}")
print(f"   High Source stations: {(station_stats['flow_type']=='High Source').sum()}")
print(f"   High Sink stations:   {(station_stats['flow_type']=='High Sink').sum()}")


# ─── 5. ORIGIN-DESTINATION PAIRS ─────────────────────────────────────────────
print("\n[5/5] Building OD pairs...")

# Exclude round trips from OD analysis
od = trips[~trips['is_round_trip']].copy()
od['od_pair'] = od['dep_station'] + ' → ' + od['ret_station']

od_pairs = od.groupby(['dep_station', 'ret_station', 'od_pair']).agg(
    trip_count       = ('od_pair', 'count'),
    avg_distance_km  = ('distance_km', 'mean'),
    avg_duration_min = ('duration_min', 'mean'),
    ebike_count      = ('is_ebike', 'sum'),
).reset_index()

od_pairs['dep_station_name'] = od_pairs['dep_station'].str.replace(r'^\d{4}\s*', '', regex=True).str.strip()
od_pairs['ret_station_name'] = od_pairs['ret_station'].str.replace(r'^\d{4}\s*', '', regex=True).str.strip()
od_pairs = od_pairs.sort_values('trip_count', ascending=False).reset_index(drop=True)
od_top = od_pairs.head(500)  # Top 500 pairs for Tableau performance
print(f"   Total OD pairs: {len(od_pairs):,}  →  saving top 500")


# ─── 6. DAILY SUMMARY ────────────────────────────────────────────────────────
print("\n[6/5] Building daily summary...")

daily = trips.groupby('departure_date').agg(
    total_trips      = ('dep_station', 'count'),
    ebike_trips      = ('is_ebike', 'sum'),
    avg_distance_km  = ('distance_km', 'mean'),
    avg_duration_min = ('duration_min', 'mean'),
    unique_stations  = ('dep_station', 'nunique'),
    round_trips      = ('is_round_trip', 'sum'),
).reset_index()

# Re-join weather at daily level for standalone daily sheet
daily = daily.merge(weather, left_on='departure_date', right_on='date', how='left')
daily.drop(columns=['date'], inplace=True, errors='ignore')

# Add time dimensions back
daily['departure_date'] = pd.to_datetime(daily['departure_date'])
daily['month']       = daily['departure_date'].dt.month
daily['month_name']  = daily['departure_date'].dt.strftime('%B')
daily['dow']         = daily['departure_date'].dt.day_name()
daily['season']      = daily['month'].apply(season)
daily['ebike_pct']   = (daily['ebike_trips'] / daily['total_trips'] * 100).round(1)
print(f"   Daily rows: {len(daily):,}")


# ─── SAVE ALL OUTPUTS ─────────────────────────────────────────────────────────
print("\n── Saving CSVs to:", OUT_DIR)

trips.to_csv(       f"{OUT_DIR}/trips_clean.csv",     index=False)
station_stats.to_csv(f"{OUT_DIR}/station_stats.csv",  index=False)
od_top.to_csv(       f"{OUT_DIR}/od_pairs.csv",        index=False)
daily.to_csv(        f"{OUT_DIR}/daily_summary.csv",   index=False)

# Save bikeways as-is (already clean)
import shutil
shutil.copy(BIKEWAYS_FILE, f"{OUT_DIR}/bikeways_clean.csv")

print("\n✅  All done! Files written:")
for fname in ['trips_clean.csv', 'station_stats.csv', 'od_pairs.csv', 'daily_summary.csv', 'bikeways_clean.csv']:
    path = f"{OUT_DIR}/{fname}"
    size = os.path.getsize(path) / 1024
    rows = sum(1 for _ in open(path)) - 1
    print(f"   {fname:<30} {rows:>8,} rows   {size:>8.1f} KB")

print("""
─────────────────────────────────────────────────────────────
NEXT STEPS — Adding Station Coordinates
─────────────────────────────────────────────────────────────
station_stats.csv has latitude/longitude columns ready but empty.
To fill them automatically:

  1. Download Mobi station info (free, no login needed):
     https://vancouver.publicbikesystem.net/ube/gbfs/v1/en/station_information

  2. Run this one-liner to merge:
     python -c "
     import pandas as pd, requests
     j = requests.get('https://vancouver.publicbikesystem.net/ube/gbfs/v1/en/station_information').json()
     coords = pd.DataFrame(j['data']['stations'])[['name','lat','lon']]
     s = pd.read_csv('tableau_data/station_stats.csv')
     s = s.merge(coords.rename(columns={'name':'station','lat':'latitude','lon':'longitude'}),
                 on='station', how='left', suffixes=('','_new'))
     s['latitude']  = s['latitude_new'].fillna(s['latitude'])
     s['longitude'] = s['longitude_new'].fillna(s['longitude'])
     s.drop(columns=['latitude_new','longitude_new'], inplace=True)
     s.to_csv('tableau_data/station_stats.csv', index=False)
     print('Done')
     "

─────────────────────────────────────────────────────────────
TABLEAU DATA SOURCE CONNECTIONS
─────────────────────────────────────────────────────────────
Dashboard 1 – Station Demand Map
  Primary:  station_stats.csv  (lat/lon → Map layer)
  Overlay:  bikeways_clean.csv (lat/lon → second Map layer)

Dashboard 2 – Route Corridors
  Primary:  od_pairs.csv       (dep_station + ret_station for flow)
  Join to:  station_stats.csv  on dep_station = station  (for dep coords)
  Join to:  station_stats.csv  on ret_station = station  (for ret coords)

Dashboard 3 – Ride Patterns
  Primary:  trips_clean.csv    (hourly/daily bar charts, e-bike splits)

Dashboard 4 – Weather & Seasonality
  Primary:  daily_summary.csv  (scatter: trips vs temp/precip, season filter)
─────────────────────────────────────────────────────────────
""")