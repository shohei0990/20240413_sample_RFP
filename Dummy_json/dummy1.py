## ダミーデータ出力コード①
import json
from random import randint, uniform

data = {}
for year in range(2021, 2025):
    for month in range(1, 13):
        year_month = f"{year}{month:02d}"
        data[year_month] = {
            "freq": {
                "once": {"avg": randint(100, 200), "pct": round(uniform(30, 40), 1), "gr": round(uniform(-3, 3), 1)},
                "twice": {"avg": randint(50, 100), "pct": round(uniform(25, 35), 1), "gr": round(uniform(-6, 6), 1)},
                "thrice": {"avg": randint(30, 80), "pct": round(uniform(10, 20), 1), "gr": round(uniform(-8, 8), 1)},
                "four": {"avg": randint(25, 35), "pct": round(uniform(5, 10), 1), "gr": round(uniform(-10, 10), 1)},
                "five_plus": {"avg": randint(30, 40), "pct": round(uniform(5, 10), 1), "gr": round(uniform(-10, 10), 1)}
            }
        }

with open('usage_frequency_data.json', 'w') as file:
    json.dump(data, file, indent=4)