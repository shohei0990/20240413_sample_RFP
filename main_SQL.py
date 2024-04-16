# main.py : FAST API サンプルコード
# uvicorn main:app --reload で起動
# http://127.0.0.1:8000/docs で仕様確認
# 年月をstr 数値6桁で GET送信, 各レスポンスが返ってくるアプリ

import json
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta

# CORSを許可するオリジンのリスト
origins = [
    "https://www.example.com",
    "https://api.example.com"
]

app = FastAPI()
engine = create_engine('sqlite:///pop-make-up_DB_add.db')

# CORSミドルウェアの設定
app.add_middleware(
    CORSMiddleware,
    allow_origins="*",  # 全てのオリジンを許可する場合は ["*"]
    allow_credentials=True,
    allow_methods=["*"],  # または特定のHTTPメソッド ['GET', 'POST', 'PUT']
    allow_headers=["*"],  # または特定のヘッダー ['X-Custom-Header']
)


# データフレームの辞書化
def execute_query(query: str) -> Optional[dict]:
    """SQLクエリを実行し、結果を辞書形式で返す関数"""
    df = pd.read_sql_query(query, engine)
    if df.empty:
        return None
    return df.to_dict(orient='records')



## API①
def build_monthly_summary_query(formatted_year_month: str) -> str:
    """月間サマリーのSQLクエリを構築する関数"""
    return f"""
    SELECT
        strftime('%Y-%m', DATE) AS Month,
        COUNT(DISTINCT user_id) AS Total_Users,
        COUNT(stock_id) AS Total_Stocks_Used
    FROM final_combined_data
    WHERE strftime('%Y-%m', DATE) = '{formatted_year_month}'
    GROUP BY strftime('%Y-%m', DATE)
    """

## API②
def get_total_users_count() -> int:
    """ユーザーの総数を取得する関数"""
    query = "SELECT COUNT(*) AS TotalUserCount FROM users"
    result = execute_query(query)
    if result:
        return result[0]['TotalUserCount']
    else:
        return 0

def build_usage_frequency_query(formatted_year_month: str) -> str:
    """使用頻度に関するSQLクエリを構築する関数。週利用回数が0のユーザー数も正確に計算する。"""
    total_users_count = get_total_users_count()  # ユーザー総数を取得
    return f"""
    WITH UserWeeklyUsage AS (
        SELECT
            user_id,
            COUNT(DISTINCT strftime('%W', DATE)) AS WeeksUsed,
            COUNT(*) AS TotalUsage
        FROM
            final_combined_data
        WHERE
            strftime('%Y-%m', DATE) = '{formatted_year_month}'
        GROUP BY
            user_id
    ), UsageCategory AS (
        SELECT
            user_id,
            CASE
                WHEN WeeksUsed = 0 THEN 'zero'
                WHEN TotalUsage / WeeksUsed < 2 THEN 'once'
                WHEN TotalUsage / WeeksUsed < 3 THEN 'twice'
                WHEN TotalUsage / WeeksUsed < 4 THEN 'thrice'
                WHEN TotalUsage / WeeksUsed < 5 THEN 'four'
                ELSE 'five_plus'
            END AS Category
        FROM
            UserWeeklyUsage
    ), CategoryCounts AS (
        SELECT
            Category,
            COUNT(*) AS UsersCount
        FROM
            UsageCategory
        GROUP BY
            Category
    )
    SELECT
        Category AS UsageCategory,
        UsersCount
    FROM
        CategoryCounts
    UNION ALL
    SELECT
        'zero' AS UsageCategory,
        {total_users_count} - (SELECT SUM(UsersCount) FROM CategoryCounts) AS UsersCount
    """

def get_previous_month(year_month: str) -> str:
    """指定された年月の前月を計算する"""
    year, month = int(year_month[:4]), int(year_month[4:6])
    previous_month_date = datetime(year, month, 1) - timedelta(days=1)
    return previous_month_date.strftime('%Y%m')



def calculate_growth_rate(current_count: int, previous_count: Optional[int]) -> float:
    """成長率を計算する関数。前月のカウントがNoneの場合は0として扱う。"""
    # previous_countがNoneの場合、0として扱う
    previous_count = previous_count or 0
    if previous_count > 0:
        growth_rate = ((current_count - previous_count) / previous_count) * 100
    else:
        growth_rate = "N/A"  # 前月のデータがない場合は"N/A"とする
    return growth_rate


def format_usage_frequency_result(year_month: str, current_result: list, previous_result: list) -> dict:
    # 先月のデータをカテゴリごとにマッピング
    previous_data_map = {item['UsageCategory']: item for item in previous_result}

    # カテゴリのリスト
    categories = ['zero', 'once', 'twice', 'thrice', 'four', 'five_plus']

    # 全ユーザー数の合計を計算
    total_users = sum(item['UsersCount'] for item in current_result)

    formatted_data = {year_month: {"freq": {}}}

    for category in categories:
        current_item = next((item for item in current_result if item['UsageCategory'] == category), None)
        previous_item = previous_data_map.get(category, None)

        current_count = current_item['UsersCount'] if current_item else 0

        # 成長率を計算
        if previous_item is not None:
            growth_rate = calculate_growth_rate(current_count, previous_item['UsersCount'])
        else:
            growth_rate = "N/A"

        # 各カテゴリのユーザー数に対する割合を計算し、小数点以下第一位で丸める
        pct = round((current_count / total_users * 100), 1) if total_users > 0 else 0

        # 成長率が数値の場合、小数点以下第一位で丸める
        if isinstance(growth_rate, float):
            growth_rate = round(growth_rate, 1)

        formatted_data[year_month]["freq"][category] = {
            "avg": current_count,
            "pct": pct,
            "gr": growth_rate
        }

    return formatted_data




## API③
def build_age_group_query(formatted_year_month: str) -> str:
    """年齢グループのSQLクエリを構築する関数"""
    return f"""
    SELECT
        strftime('%Y-%m', DATE) AS Month,
        age_group,
        COUNT(DISTINCT user_id) AS Users_Per_Age_Group,
        COUNT(stock_id) AS Stocks_Used_Per_Age_Group
    FROM
        final_combined_data
    WHERE
        strftime('%Y-%m', DATE) = '{formatted_year_month}'
    GROUP BY
        strftime('%Y-%m', DATE), age_group
    """

def build_store_summary_query(formatted_year_month: str) -> str:
    """店舗サマリーのSQLクエリを構築する関数"""
    return f"""
    SELECT
        strftime('%Y-%m', DATE) AS Month,
        STORE,
        STORE_ID,
        COUNT(DISTINCT user_id) AS Users_Per_Store,
        COUNT(stock_id) AS Stocks_Used_Per_Store
    FROM
        final_combined_data
    WHERE
        strftime('%Y-%m', DATE) = '{formatted_year_month}'
    GROUP BY
        strftime('%Y-%m', DATE), STORE
    """



# API①  
@app.get("/")
async def main():
    return {"message": "Hello World"}

@app.get("/monthly-summary/{year_month}")
async def get_monthly_summary(year_month: str):
    # year_monthを 'YYYY-MM' 形式に変換
    formatted_year_month = f"{year_month[:4]}-{year_month[4:6]}"
    query = build_monthly_summary_query(formatted_year_month)
    result = execute_query(query)
    
    if not result:
        return {"error": "No data found for the specified year_month."}
    
    formatted_result = {
                            year_month: {
                            "total_users": result[0]['Total_Users'],
                            "total_stocks_used":result[0]['Total_Stocks_Used']
                            }
                        }
    return formatted_result


# API②   
@app.get("/usage-frequency/{year_month}")
async def get_usage_frequency(year_month: str):
    """
    指定された年月の使用頻度データを取得し、整形して返すエンドポイント。
    
    Args:
        year_month (str): 'YYYYMM'形式の年月。
    
    Returns:
        dict: 整形された使用頻度データ。
    """
    # 年月を 'YYYY-MM' 形式に変換
    formatted_year_month = f"{year_month[:4]}-{year_month[4:6]}"
    # 前月を計算
    prev_year_month = get_previous_month(year_month)
    formatted_prev_year_month = f"{prev_year_month[:4]}-{prev_year_month[4:6]}"
    
    # 現在の月と前月のクエリを構築
    current_query = build_usage_frequency_query(formatted_year_month)
    prev_query = build_usage_frequency_query(formatted_prev_year_month)
    
    # クエリを実行して結果を取得
    current_result = execute_query(current_query)
    previous_result = execute_query(prev_query)

    # 前月のデータがない場合でも処理を続行
    formatted_result = format_usage_frequency_result(year_month, current_result, previous_result if previous_result else [])
    return formatted_result


# API③
@app.get("/usage-group/{year_month}")
async def get_usage_group(year_month: str):
    formatted_year_month = f"{year_month[:4]}-{year_month[4:6]}"

    # 年齢グループのクエリを構築
    age_group_query = build_age_group_query(formatted_year_month)
    # 店舗サマリーのクエリを構築
    store_summary_query = build_store_summary_query(formatted_year_month)

    # クエリを実行
    age_group_result = execute_query(age_group_query)
    store_summary_result = execute_query(store_summary_query)

    if not age_group_result and not store_summary_result:
        return {"error": "No data found for the specified year_month."}

    # 結果を group_data.json と同じ形式で整形
    formatted_result = {
        year_month: {
            "store_data": [
                {
                    "store_id": item["STORE_ID"],
                    "store": item["STORE"],
                    "total_users": item["Users_Per_Store"],
                    "total_meals": item["Stocks_Used_Per_Store"]
                }
                for item in store_summary_result
            ],
            "age_group_data": [
                {
                    "age_group": item["age_group"],
                    "total_users": item["Users_Per_Age_Group"],
                    "total_meals": item["Stocks_Used_Per_Age_Group"]
                }
                for item in age_group_result
            ]
        }
    }

    return formatted_result
