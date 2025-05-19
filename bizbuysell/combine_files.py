import pandas as pd
import os

def combine_files(files):
    dfs = []
    for csv in files:
        dfs.append(pd.read_csv(csv))
    df = pd.concat(dfs)
    df = df.sort_values('Unnamed: 0')
    df = df.reset_index(drop=True)
    df = df.drop('Unnamed: 0', axis=1)
    return(df)

files = os.listdir('/Users/sameermenghani/Desktop/bizbuyselldealscraper')
files = [x for x in files if x[:19] == 'finalbizbuyselldata']
df = combine_files(files)
df.to_csv('bizbuyselldata.csv')
