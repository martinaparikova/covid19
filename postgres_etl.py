import json
import requests
import pandas as pd
import wget
import os
import psycopg2
import dotenv
from bs4 import BeautifulSoup
import time


data_url = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19'
infected_detailed_url = 'https://api.apify.com/v2/key-value-stores/qAEsnylzdjhCCyZeS/records/LATEST?disableRedirect=true'
infected_detailed_target = 'data_files/nakazeni.csv'
url_tests = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/testy.csv'
tests_target = 'data_files/tests.csv'
infected_url = 'https://onemocneni-aktualne.mzcr.cz/api/v1/covid-19/nakaza.csv'
infected_target = 'data_files/infected_count.csv'
data_refresh_times_target = 'data_files/data_refresh_times_target.csv'

response = requests.get(data_url)
html = BeautifulSoup(response.text, 'html.parser')
j = 0
for p in html.select('p'):
    if p.text.startswith('Aktuální k'):
        j = j + 1
        if j == 1:
            tested_last_refresh = p.text.replace('Aktuální k ', '')
        if j == 2:
            infected_last_refresh = p.text.replace('Aktuální k ', '')
        if j == 3:
            details_last_refresh = p.text.replace('Aktuální k ', '')
time.sleep(1)


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

env_values = dotenv.dotenv_values()

conn = psycopg2.connect(host=env_values['DB_HOSTNAME'],
                        database=env_values['DB_DATABASE'],
                        user=env_values['DB_USERNAME'],
                        password=env_values['DB_PASSWORD'])

cur = conn.cursor()

df2 = pd.read_csv(tests_target, sep=',', header=0)
df2.to_csv(tests_target, index=False, header=False)
df3 = pd.read_csv(infected_target, sep=',', header=0)
df3.to_csv(infected_target, index=False, header=False)
data_refresh_times = [[tested_last_refresh, infected_last_refresh, details_last_refresh]]
df4 = pd.DataFrame(data_refresh_times, columns=['tested_last_refresh', 'infected_last_refresh', 'details_last_refresh'])
df4.to_csv(data_refresh_times_target, index=False, header=False)

cur.execute('TRUNCATE TABLE public.covid19_tested_counts_daily')
cur.execute('TRUNCATE TABLE public.covid19_infected_counts_daily')
cur.execute('TRUNCATE TABLE public.covid19_infected_counts_detailed')
cur.execute('TRUNCATE TABLE public.covid19_data_refresh_times')
conn.commit()

with open(tests_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_tested_counts_daily', sep=',',
                  columns=('report_date', 'tested_count', 'tested_count_cumulative'))
with open(infected_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_infected_counts_daily', sep=',',
                  columns=('report_date', 'infected_count', 'infected_count_cumulative'))
with open(infected_detailed_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_infected_counts_detailed', sep=',',
                  columns=('report_date', 'age', 'gender', 'imported_from_country', 'region'))
with open(data_refresh_times_target, 'r', encoding='utf-8') as f:
    cur.copy_from(f, 'public.covid19_data_refresh_times', sep=',',
                  columns=('tested_last_refresh', 'infected_last_refresh', 'details_last_refresh'))
conn.commit()

cur.close()
conn.close()
