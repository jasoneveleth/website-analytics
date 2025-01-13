#%%

import glob
import pandas as pd
import os

#%%

# read all csvs in logs/ to one dataframe
def main():
    column_names = [
        "user_agent", "timestamp", "screen_width", "screen_height", 
        "viewport_width", "viewport_height", "language", 
        "timezone_offset", "referrer", "page", "ip_address"
    ]

    if not os.path.exists('logs/df.feather'):
        all_files = glob.glob("logs/*.csv")
        df = pd.concat((pd.read_csv(f, sep="\t", names=column_names, header=None) for f in all_files))
        df.to_feather('logs/df.feather')
    else:
        print('using existing dataframe: logs/df.feather')
        df = pd.read_feather('logs/df.feather')
    return df

#%%

main()

# %%
