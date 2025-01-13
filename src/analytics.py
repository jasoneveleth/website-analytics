import glob
import pandas as pd
import os

# read all csvs in data/ to one dataframe
def main():
    if not os.path.exists('data/df.feather'):
        all_files = glob.glob("data/*.csv")
        df = pd.concat((pd.read_csv(f) for f in all_files))
        df.to_feather('data/df.feather')
    else:
        print('using existing dataframe: data/df.feather')
        df = pd.read_feather('data/df.feather')

    print(df.head())
