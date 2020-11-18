import csv
import json
import os.path
import re
import requests
import sys

def fetch_to_csv(url, filename, clobber=False):
    if not clobber and os.path.exists(filename):
        return None
    response = requests.get(url)
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        try:
            for row in response.json():
                csvwriter.writerow(row)
        except:
            print("ERROR downloading: %s" % url)
            return None

# Fetch census data by congressional district
def census_congressional(year, filename):
    census_urlA = "https://api.census.gov/data/"
    census_urlB = "/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*"
    url = f"{census_urlA}{year}{census_urlB}"
    # 2015 does not have congressional data
    if year == 2015:
        return None
    fetch_to_csv(url, filename)

# censuscounty data by congressional district
def census_county(year, filename):
    censuscounty_urlA = "https://api.census.gov/data/"
    censuscounty_urlB = "/acs/acs1/profile?get=NAME,group(DP02)&for=county:*"
    url = f"{censuscounty_urlA}{year}{censuscounty_urlB}"
    fetch_to_csv(url, fname)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("include a year and either 'county' or nothing for congressional");
        exit()
    year = sys.argv[1]
    if not re.match(r"^\d{4}$",year):
        print("That doesn't look like a four-digit year: {}".format(year))
        exit()
    if not int(year) < 2020:
        print("Year must be less than 2020.")
        exit()
    if len(sys.argv) > 2:
        print("County for {}".format(year))
        census_county(year, "data/census_by_county_{}.csv".format(year))
    else:
        print("Congressional district for {}".format(year))
        census_congressional(year, "data/census_by_congress_{}.csv".format(year))
