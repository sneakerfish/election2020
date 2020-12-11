import numpy as np
import pandas as pd

data_dir="data"


def read_r_file(year):
    """Read a CSV file produced from the R politicaldata data set.  We have the following fields:
        state_abb,district,total_votes,dem,other,rep
       and the dem,other, and rep columns are percentage (0,1) with NA sometimes.
    """
    def district(row):
        return "{}-{:02d}".format(row['state_abb'], row['district'])

    def winner(row):
        if (row['dem'] > row['rep']):
            return "D"
        elif (row['rep'] > row['dem']):
            return "R"
        elif (row['dem'] > 0 and np.isnan(row['rep'])):
            return "D"
        elif (row['rep'] > 0 and np.isnan(row['dem'])):
            return "R"
        else:
            return None

    df = pd.read_csv("{}/{}.all.house.csv".format(data_dir, year))
    df.loc[df.district == 0, "district"] = 1
    df[year] = df.apply(winner, axis=1)
    df["district"] = df.apply(district, axis=1)
    df = df.set_index("district")
    return df[[year]]

def read_2020_file():
    """Read the 2020 file that we scraped from politico's website for the latest results.
    Here we have the following fields:
        district-name,district,dem-candidate,gop-candidate,dem-num,gop-num,dem-pct,gop-pct
    In this case, sometimes, the numbers are all zeros and you need to use whether there is
    an asterisk in the candidate name to determine which party won.
    """
    def winner(row):
        if (str(row['dem-candidate']).find("*") > 0) or (row['dem-num'] > row['gop-num']):
            return "D"
        elif str(row['gop-candidate']).find("*") > 0 or (row['gop-num'] > row['dem-num']):
            return "R"
        else:
            return None

    df = pd.read_csv("{}/{}.csv".format(data_dir, "2020-house"))
    df["2020"] = df.apply(winner, axis=1)
    df = df.set_index("district")
    return df[["2020"]]


def combined_df():
    current = read_2020_file()
    for i in ["2018", "2016", "2014", "2012"]:
        d = read_r_file(i)
        current = current.join(d)
    return current
