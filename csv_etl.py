import json
import requests
import pandas as pd
import wget
import os


infected_detailed_url = 'https://api.apify.com/v2/key-value-stores/qAEsnylzdjhCCyZeS/records/LATEST?disableRedirect=true'
infected_detailed_target = 'C:/Users/marti/OneDrive - Operátor ICT, a.s/projekty/covid19/nakazeni.csv'
url_tests = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv'
tests_target = 'C:/Users/marti/OneDrive - Operátor ICT, a.s/projekty/covid19/tests.csv'
infected_url = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/nakaza.csv'
infected_target = 'C:/Users/marti/OneDrive - Operátor ICT, a.s/projekty/covid19/infected_count.csv'

response = requests.get(infected_detailed_url)
data = response.text
parsed = json.loads(data)
data = parsed["data"]
data_list = []

for i in data.items():
    for values in i[1]:
        values.insert(0, i[0])
        data_list.append(values)

df = pd.DataFrame(data_list, columns=['Date',  'Age', 'Gender', 'ImportedFrom', 'Region'])
df.to_csv(infected_detailed_target)

if os.path.isfile(tests_target):
    os.remove(tests_target)
wget.download(url_tests, tests_target)

if os.path.isfile(infected_target):
    os.remove(infected_target)
wget.download(infected_url, infected_target)
