## Making pandas code faster
import pandas as pd
import numpy as np

def create_data(size=10_000, seed=0) -> pd.DataFrame:
  df = pd.DataFrame()
  np.random.seed(seed)
  df["age"] = np.random.randint(1, 100, size)
  df["favourite_food"] = np.random.choice(["pizza", "pasta", "burger"], size)
  df["hated_food"] = np.random.choice(["tonic", "egg", "broccoli"], size)
  df["time_in_bed"] = np.random.randint(1,11,size)
  df["pct_sleeping"] = np.random.rand(size)
  return df

# Reward calculation
# if time_in_bed > 5 & pct_sleeping > 50%, get favourite food. Otherwise, get hated food. If age > 90, get favourite food regardless.
def reward(row: pd.Series) -> str:
  if row.age > 90:
    return row.favourite_food
  elif row.time_in_bed > 5 and row.pct_sleeping > 0.5:
    return row.favourite_food
  else:
    return row.hated_food
  

df = create_data()
  
# method 1
# for index, row in df.iterrows():
#   df.loc[index, "reward"] = reward(row)
  
# method 2
# df["reward"] = df.apply(reward, axis=1) # axis=1 means apply to each row 
  
# method 3
df['reward'] = df['hated_food']
df.loc[
  ((df['time_in_bed'] > 5) & (df['pct_sleeping'] > 0.5)) | df['age'] > 90,
  'reward'
] = df["favourite_food"]

print(df.head())