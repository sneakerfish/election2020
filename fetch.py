import csv
import json
import os.path
import re
import requests


data_dir = "data/"

census_url = "https://api.census.gov/data/2019/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*"
census_urlA = "https://api.census.gov/data/"
census_urlB = "/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*"
census_fileA = "census-by-congress_"
census_fileB = ".csv"

censuscounty_url = "https://api.census.gov/data/2019/acs/acs1/profile?get=NAME,group(DP02)&for=county:*"
censuscounty_urlA = "https://api.census.gov/data/"
censuscounty_urlB = "/acs/acs1/profile?get=NAME,group(DP02)&for=county:*"
censuscounty_fileA = "census-by-county_"
censuscounty_fileB = ".csv"

# years desired
census_years = list(range(2009, 2020))

# congress results
# from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2
congress_url = "https://dataverse.harvard.edu/api/access/datafile/3814252?format=original&gbrecs=true"
congress_file = "1976-2018-house2.csv"

# 2016 presidential results by congressional precinct
# from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LYWX3D
presidential_2016_url = "https://dataverse.harvard.edu/api/access/datafile/3345331?format=original&gbrecs=true"
presidential_2016_file = "2016-precinct-president.csv"

# 2000-2016 presidential results by county
# from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ
presidential_county_url = "https://dataverse.harvard.edu/api/access/datafile/3641280?format=original&gbrecs=true"
presidential_county_file = "countypres_2000-2016.csv"

# exit polls from cnn:
# https://www.cnn.com/election/2018/exit-polls
# https://www.cnn.com/election/2016/results/exit-polls/national/president
# https://www.cnn.com/election/2016/results/exit-polls/national/house
# http://edition.cnn.com/ELECTION/2014/
# https://www.cnn.com/election/2012/results/race/president/
# https://www.cnn.com/election/2012/results/race/house/
exitpolls = {'2018h': "https://data.cnn.com/ELECTION/2018November6/US/xpoll/Hfull.json",
             '2016p': "https://data.cnn.com/ELECTION/2016/US/xpoll/Pfull.json",
             '2016h': "https://data.cnn.com/ELECTION/2016/US/xpoll/Hfull.json",
             '2014h': "http://data.cnn.com/jsonp/5s/ELECTION/2014/full/H.full.json?callback=callback_0",
             '2012p': "https://data.cnn.com/jsonp/ELECTION/2012/full/P.full.json?callback=callback_0",
             '2012h': "https://data.cnn.com/jsonp/ELECTION/2012/full/H.full.json?callback=callback_0",
             }



# census data by congressional district
for year in census_years:
    fname = f"{data_dir}/{census_fileA}{year}{census_fileB}"
    url = f"{census_urlA}{year}{census_urlB}"
    # 2015 does not have congressional data
    if year == 2015:
        continue
    if not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            try:
                for row in response.json():
                    csvwriter.writerow(row)
            except:
                print("ERROR downloading: %s" % url)


# censuscounty data by congressional district
for year in census_years:
    fname = f"{data_dir}/{censuscounty_fileA}{year}{censuscounty_fileB}"
    url = f"{censuscounty_urlA}{year}{censuscounty_urlB}"
    if not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            try:
                for row in response.json():
                    csvwriter.writerow(row)
            except:
                print("ERROR downloading: %s" % url)


# congress results
fname = f"{data_dir}/{congress_file}"
url = f"{congress_url}"
if not os.path.exists(fname):
    response = requests.get(url)
    with open(fname, 'w') as outfile:
        outfile.write(response.text)


# presidential results

fname = f"{data_dir}/{presidential_2016_file}"
url = f"{presidential_2016_url}"
if not os.path.exists(fname):
    response = requests.get(url)
    with open(fname, 'w') as outfile:
        outfile.write(response.text)

fname = f"{data_dir}/{presidential_county_file}"
url = f"{presidential_county_url}"
if not os.path.exists(fname):
    response = requests.get(url)
    with open(fname, 'w') as outfile:
        outfile.write(response.text)


# exit polls
for (k,v) in exitpolls.items():
    fname = f"{data_dir}/exitpolls{k}.json"
    url = f"{v}"
    if not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as outfile:
            outfile.write(response.text)

# parse exit polls into more useful data
# want:
#   Poll,Answer,R,D,O
#   eg  Gender,Male,1,2,3
# do not care about non-democrat/republican answers
for (k,v) in exitpolls.items():
    fname = f"{data_dir}/exitpolls{k}.json"
    fout = f"{data_dir}/exitpollsparsed_{k}.csv"
    result = []
    with open(fname, "r") as infile:
        data = json.load(infile)
        if 'polls' in data:
            for p in data['polls']:
                qname = p['question']
                (cD, cR, cO) = (-1, -1, 0)
                for c in p['candidates']:
                    if c['fname'] == 'Democrat':
                        cD = c['id']
                    if c['fname'] == 'Republican':
                        cR = c['id']
                for a in p['answers']:
                    aname = a['answer']
                    ares = [0, 0, 0]
                    for ca in a['candidateanswers']:
                        if not re.match('^[0-9]', ca['pct']):
                            continue
                        if ca['id'] == cD:
                            ares[0] += int(ca['pct'])
                        elif ca['id'] == cR:
                            ares[1] += int(ca['pct'])
                        else:
                            ares[2] += int(ca['pct'])
                    result.append([qname, aname, ares[0], ares[1], ares[2]])
                #print(qname)
                #print(result)

            #print(data['polls'])
        else:
            print("NO polls %s" % fname)

    with open(fout, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        for r in result:
            csvwriter.writerow(r)



# add presidential results (for %4 years) - raw num, and diff
# add voter demographics number raw num and diff (D%-R%)
#result (to train and to test on ) is pct diff from D to R
#(what about indeps?? ignore??)
