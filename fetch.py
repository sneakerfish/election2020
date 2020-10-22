import requests
import csv

response = requests.get("https://api.census.gov/data/2019/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*")

with open('census-by-cong.csv', 'w') as csvfile:
    csvwriter = csv.writer(csvfile)
    for row in response.json():
        csvwriter.writerow(row)
