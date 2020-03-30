import json
import requests
import pandas as pd
import wget
import os
import psycopg2


infected_detailed_url = 'https://api.apify.com/v2/key-value-stores/qAEsnylzdjhCCyZeS/records/LATEST?disableRedirect=true'
infected_detailed_target = 'data_files/nakazeni.csv'
url_tests = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv'
tests_target = 'data_files/tests.csv'
infected_url = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/nakaza.csv'
infected_target = 'data_files/infected_count.csv'

response = requests.get(infected_detailed_url)
data = response.text
parsed = json.loads(data)
data = parsed["data"]
data_list = []

for i in data.items():
    for values in i[1]:
        values.insert(0, i[0])
        data_list.append(values)

df = pd.DataFrame(data_list, columns=['report_date', 'age', 'gender', 'imported_from_country', 'region'])
df.to_csv(infected_detailed_target, index=False, header=False)

if os.path.isfile(tests_target):
    os.remove(tests_target)
wget.download(url_tests, tests_target)

if os.path.isfile(infected_target):
    os.remove(infected_target)
wget.download(infected_url, infected_target)

conn = psycopg2.connect(host='10.228.1.81',
                        database='sandbox',
                        user='postgres',
                        password='pass')

cur = conn.cursor()

df2 = pd.read_csv(tests_target, sep=',', header=0)
df2.to_csv(tests_target, index=False, header=False)
df3 = pd.read_csv(infected_target, sep=',', header=0)
df3.to_csv(infected_target, index=False, header=False)

cur.execute('TRUNCATE TABLE public.covid19_tested_counts_daily')
cur.execute('TRUNCATE TABLE public.covid19_infected_counts_daily')
conn.commit()

with open(tests_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_tested_counts_daily', sep=',')
with open(infected_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_infected_counts_daily', sep=',')
with open(infected_detailed_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_infected_counts_detailed', sep=',',
                  columns=('report_date', 'age', 'gender', 'imported_from_country', 'region'))
conn.commit()

cur.close()
conn.close()
