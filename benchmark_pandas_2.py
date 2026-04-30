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
# alternative: stack columns then nunique
res2 = df.melt(id_vars=['date'], value_vars=['sender_digits', 'receiver_digits']).groupby('date')['value'].nunique()
print(f"Melt/Nunique time: {time.time() - start:.4f}s")

# Ensure results are the same
# The index names might differ slightly, let's just check the values
assert (res1.values == res2.loc[res1.index].values).all(), "Results differ"
print("Results are identical.")
