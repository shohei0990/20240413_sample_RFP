## ダミーデータ出力コード②
import json
import random

# 店舗データ
stores = [
    {"store_id": 1, "store": "品川本社"},
    {"store_id": 2, "store": "札幌支社"},
    {"store_id": 3, "store": "静岡支社"},
    {"store_id": 4, "store": "名古屋支社"},
    {"store_id": 5, "store": "恵比寿支社"},
    {"store_id": 6, "store": "仙台支社"},
    {"store_id": 7, "store": "広島支社"},
    {"store_id": 8, "store": "福岡支社"},
    {"store_id": 9, "store": "浅草支社"},
]

# 年代データ
age_groups = ["20代", "30代", "40代", "50代", "60代以上"]

# ダミーデータの生成
data = {}

# 指定された期間内の各月に対してデータを生成
for year in range(2022, 2024):
    for month in range(1, 13):
        year_month = f"{year}{month:02d}"
        data[year_month] = {
            "store_data": [],
            "age_group_data": []
        }

        # 各店舗に対してダミーデータを生成
        for store in stores:
            data[year_month]["store_data"].append({
                "store_id": store["store_id"],
                "store": store["store"],
                "total_users": random.randint(50, 200),  # 50から200の間でランダムな数
                "total_meals": random.randint(100, 400),  # 100から400の間でランダムな数
            })

        # 各年代に対してダミーデータを生成
        for age_group in age_groups:
            data[year_month]["age_group_data"].append({
                "age_group": age_group,
                "total_users": random.randint(100, 500),  # 100から500の間でランダムな数
                "total_meals": random.randint(200, 1000),  # 200から1000の間でランダムな数
            })

# ダミーデータをJSONファイルに保存
with open('group_data.json', 'w') as file:
    json.dump(data, file, indent=4)