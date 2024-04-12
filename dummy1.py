## ダミーデータ出力コード①
import json
from random import randint, uniform

data = {}
for year in range(2021, 2024):
    for month in range(1, 13):
        year_month = f"{year}{month:02d}"
        data[year_month] = {
            "freq": {
                "once": {"avg": randint(100, 200), "pct": uniform(10, 20), "gr": uniform(-5, 5)},
                "twice": {"avg": randint(150, 250), "pct": uniform(15, 25), "gr": uniform(-5, 5)},
                "thrice": {"avg": randint(200, 300), "pct": uniform(20, 30), "gr": uniform(-5, 5)},
                "four": {"avg": randint(250, 350), "pct": uniform(25, 35), "gr": uniform(-5, 5)},
                "five_plus": {"avg": randint(300, 400), "pct": uniform(30, 40), "gr": uniform(-5, 5)}
            }
        }


with open('usage_frequency_data.json', 'w') as file:
    json.dump(data, file, indent=4)