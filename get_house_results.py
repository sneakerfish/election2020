import os, csv, re, requests, time
from bs4 import BeautifulSoup

def states():
    return { 'Alabama': 'AL',
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

def parse_district_name(district_name):
    # Return state-code and number (or 1 for At-Large)
    m = re.match("(.*)'s? At-large district", district_name)
    state, district = None, None
    if m:
        state = states()[m.group(1)]
        district = 1
    else:
        m = re.match("(.*)'s? (\d+)(\w+) district", district_name)
        if m:
            state = states()[m.group(1)]
            district = m.group(2)
    return state, district

def house_one_state(state_name, csvwriter):
    url = "https://www.politico.com/2020-election/results/{}/house/".format(state_name)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tables = soup.find_all(class_="jsx-3350511208 leaderboard")
    for i in range(len(tables)):
        table = tables[i]
        district = table.find(class_="heading").text
        rows = table.find_all("tr")

        # The first row is a header, or it is missing.
        if len(rows) == 1:
            row_range = [0]
        else:
            row_range = list(range(1,len(rows)))

        vals = {}
        for j in row_range:
            cols = rows[j].find_all("td")
            # There are always two columns, candidate name/party and percent raw votes
            lname = cols[0].find(class_="candidate-short-name").text
            party = cols[0].find(class_="party-label").text
            pct = cols[1].find(class_="candidate-percent-only").text
            num = cols[1].find(class_="candidate-votes-next-to-percent").text
            # Some of these races are single party.  Only keep the winner, which
            # is the first listed.
            if "{}:num".format(party) not in vals:
                vals["{}:candidate".format(party)] = lname
                vals["{}:num".format(party)] = int(num.replace(",", ""))
                vals["{}:pct".format(party)] = float(pct.replace("%", ""))
        state, distno = parse_district_name(district)
        csvwriter.writerow([district, "{}-{:02d}".format(state, int(distno)),
                            vals.setdefault("dem:candidate", ""),
                            vals.setdefault("gop:candidate", ""),
                            vals.setdefault("dem:num", 0),
                            vals.setdefault("gop:num", 0),
                            vals.setdefault("dem:pct", 0),
                            vals.setdefault("gop:pct", 0)])


if __name__ == "__main__":
    fname = "data/2020-house.csv"
    with open(fname, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["district-name", "district", "dem-candidate", "gop-candidate",
                            "dem-num", "gop-num", "dem-pct", "gop-pct"])
        for state in states().keys():
            time.sleep(0.5)
            print("State: {}".format(state))
            if state == "District of Columbia":
                state = "Washington DC"
            state_url = state.lower().replace(" ", "-")
            house_one_state(state_url, csvwriter)
