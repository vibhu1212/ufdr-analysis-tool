import pandas as pd
import numpy as np
import time

n = 1000000
df = pd.DataFrame({
    'date': np.random.choice(pd.date_range('2023-01-01', '2023-12-31'), n),
    'sender_digits': np.random.randint(1000, 9999, n),
    'timestamp': np.random.randn(n),
    'is_weekend': np.random.choice([True, False], n),
    'is_late_night': np.random.choice([True, False], n)
})

start = time.time()
res1 = df.groupby('date').agg({
    'timestamp': 'count',
    'sender_digits': lambda x: len(set(x)),
    'is_weekend': 'first',
    'is_late_night': 'sum'
}).reset_index()
print(f"Lambda time: {time.time() - start:.4f}s")

start = time.time()
res2 = df.groupby('date').agg({
    'timestamp': 'count',
    'sender_digits': 'nunique',
    'is_weekend': 'first',
    'is_late_night': 'sum'
}).reset_index()
print(f"Nunique time: {time.time() - start:.4f}s")

# Ensure results are the same
assert res1.equals(res2), "Results differ"
print("Results are identical.")
