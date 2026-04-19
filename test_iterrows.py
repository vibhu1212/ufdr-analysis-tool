import pandas as pd
import time

# Create a sample DataFrame
df = pd.DataFrame({
    'latitude': range(10000),
    'longitude': range(10000),
    'timestamp': pd.date_range('2023-01-01', periods=10000),
    'accuracy': [10.0] * 10000,
    'altitude': [100.0] * 10000,
    'location_id': range(10000)
})

start = time.time()
res = []
for idx, row in df.iterrows():
    res.append(row['latitude'])
print("iterrows:", time.time() - start)

start = time.time()
res = []
for row in df.itertuples(index=False):
    res.append(row.latitude)
print("itertuples:", time.time() - start)

start = time.time()
heat_data = [[row['latitude'], row['longitude']] for idx, row in df.iterrows()]
print("heat_data iterrows:", time.time() - start)

start = time.time()
heat_data2 = df[['latitude', 'longitude']].values.tolist()
print("heat_data tolist:", time.time() - start)
