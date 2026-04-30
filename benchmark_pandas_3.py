import pandas as pd
import numpy as np
import time

n = 1000000
df = pd.DataFrame({
    'date': np.random.choice(pd.date_range('2023-01-01', '2023-12-31'), n),
    'sender_digits': np.random.randint(1000, 9999, n),
    'receiver_digits': np.random.randint(1000, 9999, n),
})

start = time.time()
res1 = df.groupby('date').apply(
    lambda x: len(set(x['sender_digits']) | set(x['receiver_digits']))
)
print(f"Lambda time: {time.time() - start:.4f}s")

start = time.time()
res2 = pd.concat([
    df[['date', 'sender_digits']].rename(columns={'sender_digits': 'contact'}),
    df[['date', 'receiver_digits']].rename(columns={'receiver_digits': 'contact'})
]).groupby('date')['contact'].nunique()
print(f"Concat/Nunique time: {time.time() - start:.4f}s")

# Ensure results are the same
assert (res1.values == res2.loc[res1.index].values).all(), "Results differ"
print("Results are identical.")
