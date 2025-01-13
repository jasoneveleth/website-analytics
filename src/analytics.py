import glob
import pandas as pd
import os

# read all csvs in data/ to one dataframe
def main():
    if not os.path.exists('logs/df.feather'):
        all_files = glob.glob("logs/*.csv")
        df = pd.concat((pd.read_csv(f) for f in all_files))
        df.to_feather('logs/df.feather')
    else:
        print('using existing dataframe: logs/df.feather')
        df = pd.read_feather('logs/df.feather')

    print(df.head())

if __name__ == "__main__":
    main()
