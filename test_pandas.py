import pandas as pd
import numpy as np
import time

# Create dummy data
np.random.seed(42)
n = 100000
dates = pd.date_range('2023-01-01', periods=10).tolist() * (n // 10)
df = pd.DataFrame({
    'date': dates,
    'sender_digits': np.random.randint(1000, 2000, size=n).astype(str),
    'receiver_digits': np.random.randint(1500, 2500, size=n).astype(str)
})

# Original
start = time.time()
daily_unique_orig = df.groupby('date').apply(
    lambda x: len(set(x['sender_digits']) | set(x['receiver_digits']))
)
print("Original time:", time.time() - start)

# Optimized
start = time.time()
if not df.empty:
    daily_unique_opt = pd.concat([
        df.set_index('date')['sender_digits'],
        df.set_index('date')['receiver_digits']
    ]).groupby('date').nunique()
else:
    daily_unique_opt = pd.Series(dtype=int)
print("Optimized time:", time.time() - start)

print("Equal:", daily_unique_orig.equals(daily_unique_opt))
