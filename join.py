import pandas as pd
import re

f = pd.read_csv("MA.2016.csv")
f["distabbr"] = f["state_abb"] + f["district"].map("{:02d}".format)
f.set_index("distabbr")

c = pd.read_csv("census-by-cong.csv")

# Column 1: "Congressional District 3 (116th Congress), California"
test = "Congressional District 3 (116th Congress), California"
match = re.match(r"(\w+) (\w+) (\d+) \((\d+)th Congress\)\, (.*)", test)

states = {'Alabama': 'AL',
          'Alaska': 'AK',
          'Arizona': 'AZ',
          'Arkansas': 'AR',
          'California': 'CA',
          'Colorado': 'CO',
          'Connecticut': 'CT',
          'Delaware': 'DE',
          'Florida': 'FL',
          'Georgia': 'GA',
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
          'Ohio': 'OH',
          'Oklahoma': 'OK',
          'Oregon': 'OR',
          'Pennsylvania': 'PA',
          'Rhode Island': 'RI',
          'South Carolina': 'SC',
          'South Dakota': 'SD',
          'Tennessee': 'TN',
          'Texas': 'TX',
          'Utah': 'UT',
          'Vermont': 'VT',
          'Virginia': 'VA',
          'Washington': 'WA',
          'West Virginia': 'WV',
          'Wisconsin': 'WI',
          'Wyoming': 'WY',
          'American Samoa': 'AS',
          'District of Columbia': 'DC',
          'Federated States of Micronesia': 'FM',
          'Guam': 'GU',
          'Marshall Islands': 'MH',
          'Northern Mariana Islands': 'MP',
          'Palau': 'PW',
          'Puerto Rico': 'PR',
          'Virgin Islands': 'VI'}

for i in range(len(c)):
    name = c["NAME"][i]
    m = re.match(r"(\w+ \w+( \w+)?) (\d+|\(at Large\)) \((\d+)th Congress\)\, (.*)", name)
    if m:
        state = m.group(5)
        district = m.group(3)
        if district == "(at Large)":
            district = 1
        state_abb = states[state]
        if state_abb:
            dist_abb = "{}{:02d}".format(state_abb, int(district))
            print("{}".format(dist_abb))
        else:
            print("State not found: {}".format(state))
            print("Full name: {}".format(name))
    else:
        print("no match: {}".format(name))
