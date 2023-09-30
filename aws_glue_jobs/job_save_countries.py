import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.worlddata.info/countrycodes.php"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
countries_table = soup.find("table")
countries = countries_table.findAll("tr")
data = pd.DataFrame([{"data": f"INSERT INTO DIM_COUNTRIES (name, alpha2_code, alpha3_code, flag) VALUES "}])

for i in range (1, len(countries)):
    columns = countries[i].findAll("td")
    insert = pd.DataFrame([{"data": f"('{columns[0].text}', '{columns[1].text}', '{columns[2].text}', 'https://cdn.worlddata.info/pics/flags3d/{columns[2].text}.png'),"}])
    data = pd.concat([data, insert])

data.to_csv("insert_countries.csv", index=False)