## Download all the raw data sources for election models.
## Then process those raw data sources into something to
## be used in models.
##
## - Census ACS district info
## - Congressional results
## - Presidential results by contressional district
## - Exit polls by demographics
##

import csv
import json
import os.path
import re
import requests

# if should re-download everything, else skips existing files
force_redownload = False

# directory to save all results, include tailing /
data_dir = "data/"
if data_dir != "":
    assert os.path.isdir(data_dir) == True, f"Dir {data_dir} MUST exist"


### Global vars for data sources

# ACS https://www.census.gov/data/developers/data-sets/acs-1year.html
# fields: https://api.census.gov/data/2019/acs/acs1/profile/variables.html
census_url = "https://api.census.gov/data/2019/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*"
census_urlA = "https://api.census.gov/data/"
census_urlB = "/acs/acs1/profile?get=NAME,group("
census_urlC = ")&for=congressional%20district:*"
census_fileA = "census-by-congress_"
census_fileB = ".csv"

# census years desired (2020 is not available yet)
census_years = list(range(2009, 2020))
census_groups = ['DP02', 'DP03', 'DP05']


# congress results
# from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2
congress_url = "https://dataverse.harvard.edu/api/access/datafile/3814252?format=original&gbrecs=true"
congress_file = "1976-2018-house2.csv"

# 2016 presidential results by congressional precinct
# from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LYWX3D
presidential_2016_url = "https://dataverse.harvard.edu/api/access/datafile/3345331?format=original&gbrecs=true"
presidential_2016_file = "2016-precinct-president.csv"

## ***NOTE***
## Could not find publically available 2012 presidential-by-district results.
## Obtained from R
presidential_2012_file = "2012-precinct-president.csv"

#### Not using this anymore, using per-district instead of per-county
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
exitpolls = {
             '2020p': 'https://politics-elex-results.data.api.cnn.io/results/exit-poll/2020-PG-XPOLLS-US.json',
             '2020h': 'https://politics-elex-results.data.api.cnn.io/results/exit-poll/2020-HG-XPOLLS-US.json',
             '2018h': "https://data.cnn.com/ELECTION/2018November6/US/xpoll/Hfull.json",
             '2016p': "https://data.cnn.com/ELECTION/2016/US/xpoll/Pfull.json",
             '2016h': "https://data.cnn.com/ELECTION/2016/US/xpoll/Hfull.json",
             '2014h': "http://data.cnn.com/jsonp/5s/ELECTION/2014/full/H.full.json?callback=callback_0",
             '2012p': "https://data.cnn.com/jsonp/ELECTION/2012/full/P.full.json?callback=callback_0",
             '2012h': "https://data.cnn.com/jsonp/ELECTION/2012/full/H.full.json?callback=callback_0",
             }
exitpolls_fileA = "exitpolls_"
exitpolls_fileB = ".json"


# 2020 results, p+h
# https://www.politico.com/2020-election/results/massachusetts/



# 2020 pre-election polls presidential
# https://morningconsult.com/form/2020-u-s-election-tracker/#section-9
poll_2020_pres_link = 'https://assets.morningconsult.com/wp-uploads/2020/11/02093508/11.02-MCPI-National-and-Senate-Data.xlsx from https://morningconsult.com/form/2020-u-s-election-tracker/#section-9'
poll_2020_pres_file = '11.02-MCPI-National-and-Senate-Data.csv'
# ignoring  https://docs.google.com/spreadsheets/d/1cZURJuAP8P5rwmIRqX1Qk2QjXRHN4SeRbd-s51LbxH4/edit#gid=0

# 2020 pre-election polls congress
# https://www.monmouth.edu/polling-institute/reports/monmouthpoll_us_091020/
poll_2020_house_link = 'https://www.monmouth.edu/polling-institute/documents/monmouthpoll_us_091020.pdf from https://www.monmouth.edu/polling-institute/reports/monmouthpoll_us_091020/'
poll_2020_house_file = 'monmouthpoll_us_091020.csv'




### Global vars for parsing
parsed_fileprefix = "parsed_"
exitpolls_parsed_fileB = ".csv"

final_fileprefix = "final_data_"
final_filesuffix = ".csv"



### Download data

# census data by congressional district
def download_census_district():
    for year in census_years:
        for group in census_groups:
            fname = f"{data_dir}{census_fileA}{year}{group}{census_fileB}"
            url = f"{census_urlA}{year}{census_urlB}{group}{census_urlC}"
            # 2015 does not have congressional data
            if year == 2015:
                continue
            if force_redownload or not os.path.exists(fname):
                response = requests.get(url)
                with open(fname, 'w') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    try:
                        for row in response.json():
                            csvwriter.writerow(row)
                    except:
                        print("ERROR downloading: %s" % url)



# congress results
def download_house_results():
    fname = f"{data_dir}{congress_file}"
    url = f"{congress_url}"
    if force_redownload or not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as outfile:
            outfile.write(response.text)


# presidential results
def download_pres_results():
    fname = f"{data_dir}{presidential_2016_file}"
    url = f"{presidential_2016_url}"
    if force_redownload or not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as outfile:
            outfile.write(response.text)

    # Not doing some downloads automatically
    fname = f"{data_dir}{presidential_2012_file}"
    if not os.path.exists(fname):
        print(f"ERROR: don't know how to download {fname}")
        print(f"  please use R go get this")
    elif force_redownload:
        print(f"WARNING: skipping re-download of {fname}")


#### Not using this anymore, using per-district instead of per-county
# presidential results by county
def download_pres_county_results():
    fname = f"{data_dir}{presidential_county_file}"
    url = f"{presidential_county_url}"
    if force_redownload or not os.path.exists(fname):
        response = requests.get(url)
        with open(fname, 'w') as outfile:
            outfile.write(response.text)


# exit polls
def download_exit_polls():
    for (k,v) in exitpolls.items():
        fname = f"{data_dir}{exitpolls_fileA}{k}{exitpolls_fileB}"
        url = f"{v}"
        if force_redownload or not os.path.exists(fname):
            response = requests.get(url)
            with open(fname, 'w') as outfile:
                outfile.write(response.text)


# pre-election 2020 polls for predictions
def download_2020_pres_polls():
    # Not doing some downloads automatically
    fname = f"{data_dir}{poll_2020_pres_file}"
    if not os.path.exists(fname):
        print(f"ERROR: don't know how to download {fname}")
        print(f"  please download {poll_2020_pres_link}")
        print(f"  and save tab National Presidential as csv")
    elif force_redownload:
        print(f"WARNING: skipping re-download of {fname}")


def download_2020_house_polls():
    # Not doing some downloads automatically
    fname = f"{data_dir}{poll_2020_house_file}"
    if not os.path.exists(fname):
        print(f"ERROR: don't know how to download {fname}")
        print(f"  please download {poll_2020_house_link}")
        print(f"  and run through \"pdftotext -layout -f 15 -l 16 monmouthpoll_us_091020.pdf - | tr -d ',' | sed -e s'/   */,/g' > {fname}\"")
    elif force_redownload:
        print(f"WARNING: skipping re-download of {fname}")


# 2020 results
def download_2020_house_results():
    print("@@@TODO - 2020 house results")
    #https://www.politico.com/2020-national-results/house-overall.json
    #https://www.politico.com/2020-national-metadata/house.meta.json


def download_2020_pres_results():
    print("@@@TODO - 2020 pres results")
    #https://www.politico.com/2020-statewide-results/25/potus.json
    #https://www.politico.com/2020-statewide-metadata/25/potus.meta.json
    #https://www.politico.com/2020-statewide-results/25/potus-counties.json



### Parse the data

# parse exit polls into more useful format
# want: the demographic and their Democrat,Republican,Other prefs
#   Poll,Answer,D,R,O
#   eg  Gender,Male,1,2,3
# do not care about non-democrat/republican answers
def parse_exit_polls():
    for (k,v) in exitpolls.items():
        fname = f"{data_dir}{exitpolls_fileA}{k}{exitpolls_fileB}"
        fout = f"{data_dir}{parsed_fileprefix}{exitpolls_fileA}{k}{exitpolls_parsed_fileB}"
        result = []
        with open(fname, "r") as infile:
            data = json.load(infile)
            if 'polls' in data:
                for p in data['polls']:
                    qname = p['question']
                    (cD, cR, cO) = (-1, -1, 0)
                    for c in p['candidates']:
                        if c['fname'] == 'Democrat' or c['party'] == 'D':
                            cD = c['id']
                        if c['fname'] == 'Republican' or c['party'] == 'R':
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
            ## 2020 has a different format
            ##  'questions' instead of 'polls', and other changes
            elif 'questions' in data:
                (cD, cR, cO) = (-1, -1, 0)
                for c in data['candidates']:
                    if c['partyName'] == 'Democratic':
                        cD = c['candidateId']
                    if c['partyName'] == 'Republican':
                        cR = c['candidateId']
                for p in data['questions']:
                    qname = p['question']
                    for a in p['answers']:
                        aname = a['answer']
                        ares = [0, 0, 0]
                        for ca in a['candidateAnswers']:
                            if type(ca['percentage']) != int and \
                               not re.match('^[0-9]', ca['percentage']):
                                continue
                            if ca['candidateId'] == cD:
                                ares[0] += int(ca['percentage'])
                            elif ca['candidateId'] == cR:
                                ares[1] += int(ca['percentage'])
                            else:
                                ares[2] += int(ca['percentage'])
                        result.append([qname, aname, ares[0], ares[1], ares[2]])
                    #print(qname)
                    #print(result)

                #print(data['polls'])
            else:
                print("ERROR: NO polls found in %s" % fname)

        with open(fout, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            for r in result:
                csvwriter.writerow(r)


def state_to_abrv(name):
    ##https://gist.github.com/rogerallen/1583593
    us_state_abbrev = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'American Samoa': 'AS',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'District of Columbia': 'DC',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Guam': 'GU',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Northern Mariana Islands':'MP',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Puerto Rico': 'PR',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virgin Islands': 'VI',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY'
    }
    return us_state_abbrev[name]


def normalize_census_district(name):
    dist, state = (1, "")
    s = re.search(", (.*)$", name)
    if s:
        state = s.group(1)
    else:
        print("ERROR: unable to parse district " % name)
    d = re.search("^Congressional District (\d+) ", name)
    if d:
        dist = d.group(1)
    return (state_to_abrv(state),dist)


def parse_census_districts():
    # create datasets
    # per district
    parse_census_years = [2012,2014,2016,2018,2019]

    # the fields below are from these labels - for easier searching
    # CITIZEN, VOTING AGE POPULATION  or  SEX AND AGE + U.S. CITIZENSHIP STATUS
    # RACE
    # EDUCATIONAL ATTAINMENT
    # SEX AND AGE
    # INCOME AND BENEFITS
    census_fields_by_year = {
        # https://api.census.gov/data/2019/acs/acs1/profile/variables.html
        '2019': {'DP05_0087E': 'voteage_pop',
                 'DP05_0088E': 'voteage_m',
                 'DP05_0089E': 'voteage_f',
                 'DP05_0033E': 'race_pop',
                 'DP05_0037E': 'race_white',
                 'DP05_0065E': 'race_black',
                 'DP05_0067E': 'race_asian',
                 'DP05_0071E': 'race_hisp',
                 'DP02_0059E': 'ed_pop',
                 'DP02_0065E': 'ed_ba',
                 'DP02_0066E': 'ed_grdeg',
                 'DP05_0001E': 'age_pop',
                 'DP05_0009E': 'age_20_24',
                 'DP05_0010E': 'age_25_34',
                 'DP05_0011E': 'age_35_44',
                 'DP05_0012E': 'age_45_54',
                 'DP05_0013E': 'age_55_59',
                 'DP05_0014E': 'age_60_64',
                 'DP05_0021E': 'age_18_plus',
                 'DP05_0022E': 'age_21_plus',
                 'DP05_0024E': 'age_65_plus',
                 'DP03_0051E': 'inc_pop',
                 'DP03_0052E': 'inc_less_10',
                 'DP03_0053E': 'inc_10_14',
                 'DP03_0054E': 'inc_15_24',
                 'DP03_0055E': 'inc_25_34',
                 'DP03_0056E': 'inc_35_49',
                 'DP03_0057E': 'inc_50_74',
                 'DP03_0058E': 'inc_75_99',
                 'DP03_0059E': 'inc_100_149',
                 'DP03_0060E': 'inc_150_199',
                 'DP03_0061E': 'inc_200_plus',
        },
        # https://api.census.gov/data/2018/acs/acs1/profile/variables.html
        '2018': {'DP05_0087E': 'voteage_pop',
                 'DP05_0088E': 'voteage_m',
                 'DP05_0089E': 'voteage_f',
                 'DP05_0033E': 'race_pop',
                 'DP05_0037E': 'race_white',
                 'DP05_0065E': 'race_black',
                 'DP05_0067E': 'race_asian',
                 'DP05_0071E': 'race_hisp',
                 'DP02_0058E': 'ed_pop',
                 'DP02_0064E': 'ed_ba',
                 'DP02_0065E': 'ed_grdeg',
                 'DP05_0001E': 'age_pop',
                 'DP05_0009E': 'age_20_24',
                 'DP05_0010E': 'age_25_34',
                 'DP05_0011E': 'age_35_44',
                 'DP05_0012E': 'age_45_54',
                 'DP05_0013E': 'age_55_59',
                 'DP05_0014E': 'age_60_64',
                 'DP05_0021E': 'age_18_plus',
                 'DP05_0022E': 'age_21_plus',
                 'DP05_0024E': 'age_65_plus',
                 'DP03_0051E': 'inc_pop',
                 'DP03_0052E': 'inc_less_10',
                 'DP03_0053E': 'inc_10_14',
                 'DP03_0054E': 'inc_15_24',
                 'DP03_0055E': 'inc_25_34',
                 'DP03_0056E': 'inc_35_49',
                 'DP03_0057E': 'inc_50_74',
                 'DP03_0058E': 'inc_75_99',
                 'DP03_0059E': 'inc_100_149',
                 'DP03_0060E': 'inc_150_199',
                 'DP03_0061E': 'inc_200_plus',
        },
        # https://api.census.gov/data/2016/acs/acs1/profile/variables.html
        '2016': {'DP05_0082E': 'voteage_pop',
                 'DP05_0083E': 'voteage_m',
                 'DP05_0084E': 'voteage_f',
                 'DP05_0028E': 'race_pop',
                 'DP05_0032E': 'race_white',
                 'DP05_0060E': 'race_black',
                 'DP05_0062E': 'race_asian',
                 'DP05_0066E': 'race_hisp',
                 'DP02_0058E': 'ed_pop',
                 'DP02_0064E': 'ed_ba',
                 'DP02_0065E': 'ed_grdeg',
                 'DP05_0001E': 'age_pop',
                 'DP05_0008E': 'age_20_24',
                 'DP05_0009E': 'age_25_34',
                 'DP05_0010E': 'age_35_44',
                 'DP05_0011E': 'age_45_54',
                 'DP05_0012E': 'age_55_59',
                 'DP05_0013E': 'age_60_64',
                 'DP05_0018E': 'age_18_plus',
                 'DP05_0019E': 'age_21_plus',
                 'DP05_0021E': 'age_65_plus',
                 'DP03_0051E': 'inc_pop',
                 'DP03_0052E': 'inc_less_10',
                 'DP03_0053E': 'inc_10_14',
                 'DP03_0054E': 'inc_15_24',
                 'DP03_0055E': 'inc_25_34',
                 'DP03_0056E': 'inc_35_49',
                 'DP03_0057E': 'inc_50_74',
                 'DP03_0058E': 'inc_75_99',
                 'DP03_0059E': 'inc_100_149',
                 'DP03_0060E': 'inc_150_199',
                 'DP03_0061E': 'inc_200_plus',
        },
        # https://api.census.gov/data/2014/acs/acs1/profile/variables.html
        '2014': {#'DP05_0082E': 'voteage_pop',
                 #'DP05_0083E': 'voteage_m',
                 #'DP05_0084E': 'voteage_f',
                 'DP05_0001E': 'allage_pop',
                 'DP05_0002E': 'allage_m',
                 'DP05_0003E': 'allage_f',
                 'DP05_0018E': 'allage_18plus',
                 'DP02_0095PE': 'allage_citzenpct',
                 'DP05_0028E': 'race_pop',
                 'DP05_0032E': 'race_white',
                 'DP05_0060E': 'race_black',
                 'DP05_0062E': 'race_asian',
                 'DP05_0066E': 'race_hisp',
                 'DP02_0058E': 'ed_pop',
                 'DP02_0064E': 'ed_ba',
                 'DP02_0065E': 'ed_grdeg',
                 'DP05_0001E': 'age_pop',
                 'DP05_0008E': 'age_20_24',
                 'DP05_0009E': 'age_25_34',
                 'DP05_0010E': 'age_35_44',
                 'DP05_0011E': 'age_45_54',
                 'DP05_0012E': 'age_55_59',
                 'DP05_0013E': 'age_60_64',
                 'DP05_0018E': 'age_18_plus',
                 'DP05_0019E': 'age_21_plus',
                 'DP05_0021E': 'age_65_plus',
                 'DP03_0051E': 'inc_pop',
                 'DP03_0052E': 'inc_less_10',
                 'DP03_0053E': 'inc_10_14',
                 'DP03_0054E': 'inc_15_24',
                 'DP03_0055E': 'inc_25_34',
                 'DP03_0056E': 'inc_35_49',
                 'DP03_0057E': 'inc_50_74',
                 'DP03_0058E': 'inc_75_99',
                 'DP03_0059E': 'inc_100_149',
                 'DP03_0060E': 'inc_150_199',
                 'DP03_0061E': 'inc_200_plus',
        },
        # https://api.census.gov/data/2012/acs/acs1/profile/variables.html
        '2012': {#'DP05_0082E': 'voteage_pop',
                 #'DP05_0083E': 'voteage_m',
                 #'DP05_0084E': 'voteage_f',
                 'DP05_0001E': 'allage_pop',
                 'DP05_0002E': 'allage_m',
                 'DP05_0003E': 'allage_f',
                 'DP05_0018E': 'allage_18plus',
                 'DP02_0095PE': 'allage_citzenpct',
                 'DP05_0028E': 'race_pop',
                 'DP05_0032E': 'race_white',
                 'DP05_0060E': 'race_black',
                 'DP05_0062E': 'race_asian',
                 'DP05_0066E': 'race_hisp',
                 'DP02_0058E': 'ed_pop',
                 'DP02_0064E': 'ed_ba',
                 'DP02_0065E': 'ed_grdeg',
                 'DP05_0001E': 'age_pop',
                 'DP05_0008E': 'age_20_24',
                 'DP05_0009E': 'age_25_34',
                 'DP05_0010E': 'age_35_44',
                 'DP05_0011E': 'age_45_54',
                 'DP05_0012E': 'age_55_59',
                 'DP05_0013E': 'age_60_64',
                 'DP05_0018E': 'age_18_plus',
                 'DP05_0019E': 'age_21_plus',
                 'DP05_0021E': 'age_65_plus',
                 'DP03_0051E': 'inc_pop',
                 'DP03_0052E': 'inc_less_10',
                 'DP03_0053E': 'inc_10_14',
                 'DP03_0054E': 'inc_15_24',
                 'DP03_0055E': 'inc_25_34',
                 'DP03_0056E': 'inc_35_49',
                 'DP03_0057E': 'inc_50_74',
                 'DP03_0058E': 'inc_75_99',
                 'DP03_0059E': 'inc_100_149',
                 'DP03_0060E': 'inc_150_199',
                 'DP03_0061E': 'inc_200_plus',
        },
    }

    census_groups = {'DP05', 'DP02', 'DP03'} # any group mentioned above

    for year in parse_census_years:
        result = {}
        census_fields = census_fields_by_year[str(year)]
        for group in census_groups:
            fname = f"{data_dir}{census_fileA}{year}{group}{census_fileB}"
            field_map = {}
            with open(fname, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    # header
                    if row[0] == "NAME":
                        for i in range(0, len(row)):
                            if row[i] in census_fields:
                                field_map[census_fields[row[i]]] = i
                                #print("Found %s" % row[i])
                        continue
                    st,d = normalize_census_district(row[0])
                    dname = f"{st}-{d}"
                    if dname not in result:
                        result[dname] = {'state': st, 'district': d}
                    for k,v in field_map.items():
                        result[dname][k] = row[v]


        fout = f"{data_dir}{parsed_fileprefix}{census_fileA}{year}{census_fileB}"
        with open(fout, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            header = ['year', 'state','district']
            for k,v in census_fields.items():
                header.append(v)
            csvwriter.writerow(header)
            for d in sorted(result.keys()):
                r = [year, result[d]['state'], result[d]['district']]
                for k,v in census_fields.items():
                    if v in result[d]:
                        r.append(result[d][v])
                    else:
                        r.append(0)
                        print("Missing %s for %s,%s" % (v, d, year))

                csvwriter.writerow(r)



def parse_2020_files():
    print("@@@TODO - parse 2020 files")



# now for a single record with everything for house elections
def join_house_data():
    cong_years = [2010, 2012, 2014, 2016, 2018]
    historical_votes = {}
    for year in cong_years:
        # get voting results from 1976-2018-house2.csv
        # get census from parsed_census-by-congress_2018.csv
        # get exit polls from parsed_exitpolls_2018h.csv

        votingh = []
        votingd = {}
        fname = f"{data_dir}{congress_file}"
        with open(fname, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if row[0] == "year":
                    votingh = row
                    continue
                if row[0] != str(year): continue
                # ignore primaries/runoffs
                if row[votingh.index('stage')] != "gen": continue
                st = row[votingh.index('state_po')]
                d = row[votingh.index('district')]
                if d == "0": d = "1"
                p = row[votingh.index('party')]
                t = row[votingh.index('totalvotes')]
                pvote = row[votingh.index('candidatevotes')]
                key = f"{st}-{d}"
                if key not in votingd:
                    votingd[key] = [year, st, d, 0, 0, t, "", "", 0, 0]
                if p == "democrat" or p == "democratic-farmer-labor":
                    votingd[key][3] += int(pvote)
                elif p == 'republican':
                    votingd[key][4] += int(pvote)
                #record person with most votes and party
                if int(pvote) > votingd[key][8]:
                    votingd[key][6] = row[votingh.index('candidate')]
                    votingd[key][7] = p[0] if p != "" else ""
                    votingd[key][8] = int(pvote)
                    # current candidate matches last winner -> incumbent
                    if str(year-2) in historical_votes and \
                       key in historical_votes[str(year-2)] and \
                       historical_votes[str(year-2)][key][6] == row[votingh.index('candidate')]:
                        votingd[key][9] = 1

        # save for future lookups
        historical_votes[str(year)] = votingd
        if year == 2010:
            continue


        censush = []
        censusd = {}
        fname = f"{data_dir}{parsed_fileprefix}{census_fileA}{year}{census_fileB}"
        with open(fname, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if row[0] == "year":
                    censush = row
                    continue
                censusd[f"{row[1]}-{row[2]}"] = row

        demoh = []
        demod = {}
        # Have the following limited demographic data for predictions
        #  Gender: Male, Female
        #  Age: 18-34,35-49,50-64,65+
        #  Income: <$50K,$50-100K,$100K+
        #  Race: Whte non-Hisp,Hsp-Blk-Asn-Oth
        #  Education: No degree,4 yr degree
        # These exit poll columns are selected to be close to those
        democols = ['Gender,Male','Gender,Female',
                    'Age,18-29',
                    'Age,30-39',
                    'Age,40-49',
                    'Age,50-64',
                    'Age,65 and Older',
                    'Income,Less Than $50K',
                    'Income,$50-100K',
                    'Income,$100K or More',
                    'Race,White','Race,Black','Race,Latino',
                    'Race,Asian','Race,Other',
                    'Education,HS or less',
                    "Education,College Graduate",
                    'Education,Postgraduate',
        ]
        democolsrepl = {
            "Men" : "Male",
            "Women" : "Female",
            "African-American" : "Black",
            "Other race" : "Other",
            "Bachelor's degree" : "College Graduate",
            "College graduate" : "College Graduate",
            "Advanced degree" : "Postgraduate",
            "65 and older" : "65 and Older",
            "Under $50K" : "Less Than $50K",
            "$50K-$100K" : "$50-100K",
            "$100K or more" : "$100K or More",
        }
        demofcolrepl = {
            "Are you a college graduate?,No" : "Education,HS or less",
            "Are You a College Graduate?,No" : "Education,HS or less",
        }
        with open(f"{data_dir}{parsed_fileprefix}{exitpolls_fileA}{year}h{exitpolls_parsed_fileB}", 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                # data cleanup
                row[0] = row[0].replace('Vote by ', '')
                if row[1] in democolsrepl: row[1] = democolsrepl[row[1]]
                k = f"{row[0]},{row[1]}"
                if k in demofcolrepl:
                    (row[0], row[1]) = demofcolrepl[k].split(",")
                    k = f"{row[0]},{row[1]}"
                #print("KEY is =%s=" % k)
                if k in democols:
                    demrepdiff = int(row[2]) - int(row[3])
                    demod[k.replace(",", "_")] = demrepdiff
                    #print("FOUND DEM %s = %s" % (k, demrepdiff) )

        # write final data
        fout = f"{data_dir}{final_fileprefix}{year}h{final_filesuffix}"
        with open(fout, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            censush = censush[0:3] +['dem', 'rep', 'tot', 'incumbent', 'prevparty'] + censush[3:]
            censush.extend(democols)
            csvwriter.writerow(censush)
            for d in sorted(censusd.keys()):
                row = censusd[d][0:3]
                if d in votingd:
                    row.extend(votingd[d][3:6])
                    incumbent = votingd[d][9]
                    prevp = ""
                    if d in historical_votes[str(year-2)]:
                        prevp = historical_votes[str(year - 2)][d][7]
                    row.extend([incumbent,prevp])
                else:
                    row.extend([0,0,0,0,""])
                    if d != "DC-1" and d != "PR-1":
                        print("NO voting results for %s %s" % (year,d))
                row.extend(censusd[d][3:])
                for c in democols:
                    k = c.replace(",", "_")
                    if k in demod:
                        row.append(demod[k])
                    else:
                        row.append("")
                csvwriter.writerow(row)



# now for a single record with everything for pres elections
def join_pres_data():
    print("@@@TODO - join pres data")
    return

"""
pres_years = [2012]
for year in pres_years:
    exitpollsparsed_2012p.csv
"""


if __name__ == '__main__':
    download_census_district()
    download_house_results()
    download_pres_results()
    #### Not using this anymore, using per-district instead of per-county
    ##download_pres_county_results()
    download_exit_polls()
    download_2020_pres_polls()
    download_2020_house_polls()
    #@@@TODO
    download_2020_house_results()
    #@@@TODO
    download_2020_pres_results()

    parse_exit_polls()
    parse_census_districts()
    parse_2020_files()
    join_house_data()
    join_pres_data()


#add per-district presidential data (join_pres_data)
#add unopposed/incumbent/party fields
#add more demographics

#parse 2020 polls  - for Tues
#above todos for 2020 results

# add presidential results (for %4 years) - raw num, and diff
# add voter demographics number raw num and diff (D%-R%)
#result (to train and to test on ) is pct diff from D to R
#(what about indeps?? ignore??)


        ## have the following limited demographic data for predictions
        # Gender: Male, Female
        # Age: 18-34,35-49,50-64,65+
        # Income: <$50K,$50-100K,$100K+
        # Race: Whte non-Hisp,Hsp-Blk-Asn-Oth
        # Education: No degree,4 yr degree
