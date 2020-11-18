import csv
import json
import os.path
import re
import requests


data_dir = "data/"

# ACS https://www.census.gov/data/developers/data-sets/acs-1year.html
# fields: https://api.census.gov/data/2019/acs/acs1/profile/variables.html
census_url = "https://api.census.gov/data/2019/acs/acs1/profile?get=NAME,group(DP02)&for=congressional%20district:*"
census_urlA = "https://api.census.gov/data/"
census_urlB = "/acs/acs1/profile?get=NAME,group("
census_urlC = ")&for=congressional%20district:*"
census_fileA = "census-by-congress_"
census_fileB = ".csv"

# census years desired
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



assert os.path.isdir(data_dir) == True, f"Dir {data_dir} MUST exist"


# census data by congressional district
for year in census_years:
    for group in census_groups:
        fname = f"{data_dir}/{census_fileA}{year}{group}{census_fileB}"
        url = f"{census_urlA}{year}{census_urlB}{group}{census_urlC}"
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




# create datasets
# per district
parse_census_years = [2012,2014,2016,2018,2019]

# the fields below are from these labels - for easier searching
# CITIZEN, VOTING AGE POPULATION  or  SEX AND AGE + U.S. CITIZENSHIP STATUS
# RACE
# EDUCATIONAL ATTAINMENT
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
             'DP02_0060E': 'ed_prehs',
             'DP02_0061E': 'ed_nohs',
             'DP02_0062E': 'ed_hs',
             'DP02_0067E': 'ed_hsplus',
             'DP02_0068E': 'ed_baplus',
             'DP02_0066E': 'ed_grad',
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
             'DP02_0059E': 'ed_prehs',
             'DP02_0060E': 'ed_nohs',
             'DP02_0061E': 'ed_hs',
             'DP02_0066E': 'ed_hsplus',
             'DP02_0067E': 'ed_baplus',
             'DP02_0065E': 'ed_grad',
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
             'DP02_0059E': 'ed_prehs',
             'DP02_0060E': 'ed_nohs',
             'DP02_0061E': 'ed_hs',
             'DP02_0066E': 'ed_hsplus',
             'DP02_0067E': 'ed_baplus',
             'DP02_0065E': 'ed_grad',
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
             'DP02_0059E': 'ed_prehs',
             'DP02_0060E': 'ed_nohs',
             'DP02_0061E': 'ed_hs',
             'DP02_0066E': 'ed_hsplus',
             'DP02_0067E': 'ed_baplus',
             'DP02_0065E': 'ed_grad',
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
             'DP02_0059E': 'ed_prehs',
             'DP02_0060E': 'ed_nohs',
             'DP02_0061E': 'ed_hs',
             'DP02_0066E': 'ed_hsplus',
             'DP02_0067E': 'ed_baplus',
             'DP02_0065E': 'ed_grad',
    },
}

old_census_fields = {'DP05_0087E': 'voteage_pop',
                 'DP05_0088E': 'voteage_m',
                 'DP05_0089E': 'voteage_f',
                 'DP05_0033E': 'race_pop',
                 'DP05_0037E': 'race_white',
                 'DP05_0065E': 'race_black',
                 'DP05_0067E': 'race_asian',
                 'DP05_0071E': 'race_hisp',
                 'DP02_0061E': 'ed_nohs',
                 'DP02_0062E': 'ed_hs',
                 'DP02_0067E': 'ed_hsplus',
                 'DP02_0068E': 'ed_baplus',
                 'DP02_0066E': 'ed_grad',
                 }
census_groups = {'DP05', 'DP02'} # any group mentioned above

for year in parse_census_years:
    result = {}
    census_fields = census_fields_by_year[str(year)]
    for group in census_groups:
        fname = f"{data_dir}/{census_fileA}{year}{group}{census_fileB}"
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


    fout = f"{data_dir}/parsed_{census_fileA}{year}{census_fileB}"
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




# now for a single record with everything
cong_years = [2012, 2014, 2016, 2018]
for year in cong_years:
    # get census from parsed_census-by-congress_2018.csv
    # get voting results from 1976-2018-house2.csv
    # get exit polls from exitpollsparsed_2018h.csv

    censush = []
    censusd = {}
    fname = f"{data_dir}/parsed_{census_fileA}{year}{census_fileB}"
    with open(fname, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if row[0] == "year":
                censush = row
                continue
            censusd[f"{row[1]}-{row[2]}"] = row

    votingh = []
    votingd = {}
    fname = f"{data_dir}/{congress_file}"
    with open(fname, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if row[0] == "year":
                votingh = row
                continue
            if row[0] != str(year): continue
            if row[votingh.index('stage')] != "gen": continue
            st = row[votingh.index('state_po')]
            d = row[votingh.index('district')]
            if d == "0": d = "1"
            p = row[votingh.index('party')]
            t = row[votingh.index('totalvotes')]
            pvote = row[votingh.index('candidatevotes')]
            key = f"{st}-{d}"
            if key not in votingd:
                votingd[key] = [year, st, d, 0, 0, t]
            if p == "democrat":
                votingd[key][3] += int(pvote)
            elif p == 'republican':
                votingd[key][4] += int(pvote)

    demoh = []
    demod = {}
    democols = ['Gender,Male','Gender,Female',
                'Race,White','Race,Black','Race,Latino',
                'Race,Asian','Race,Other',
                'Education,HS or less',
                "Education,College Graduate",
                'Education,Postgraduate']
    democolsrepl = {
        "Men" : "Male",
        "Women" : "Female",
        "African-American" : "Black",
        "Other race" : "Other",
        "Bachelor's degree" : "College Graduate",
        "Advanced degree" : "Postgraduate",
    }
    with open(f"{data_dir}/exitpollsparsed_{year}h.csv", 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            # data cleanup
            row[0] = row[0].replace('Vote by ', '')
            if row[1] in democolsrepl: row[1] = democolsrepl[row[1]]
            k = f"{row[0]},{row[1]}"
            #print("KEY is =%s=" % k)
            if k in democols:
                demrepdiff = int(row[2]) - int(row[3])
                demod[k.replace(",", "_")] = demrepdiff
                #print("FOUND DEM %s = %s" % (k, demrepdiff) )

    # write final data
    fout = f"{data_dir}/fin{year}h.csv"
    with open(fout, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        censush = censush[0:3] +['dem', 'rep', 'tot'] + censush[3:]
        censush.extend(democols)
        csvwriter.writerow(censush)
        for d in sorted(censusd.keys()):
            row = censusd[d][0:3]
            if d in votingd:
                row.extend(votingd[d][3:6])
            else:
                row.extend([0,0,0])
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

"""
pres_years = [2012]
for year in pres_years:
    exitpollsparsed_2012p.csv
"""



# add presidential results (for %4 years) - raw num, and diff
# add voter demographics number raw num and diff (D%-R%)
#result (to train and to test on ) is pct diff from D to R
#(what about indeps?? ignore??)
