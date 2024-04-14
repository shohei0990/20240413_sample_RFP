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

## API②
def user_weekly_usage():
    return """
    SELECT
        user_id,
        strftime('%Y-%m', DATE) AS Month,
        COUNT(DISTINCT strftime('%W', DATE)) AS WeeksUsed,
        COUNT(*) AS TotalUsage
    FROM
        final_combined_data
    GROUP BY
        user_id, Month
    """

def average_weekly_usage_rounded():
    return """
    SELECT
        Month,
        user_id,
        ROUND(TotalUsage * 1.0 / WeeksUsed) AS RoundedAvgWeeklyUsage
    FROM
        UserWeeklyUsage
    """

def usage_count_per_category():
    return """
    SELECT
        Month,
        CASE
            WHEN RoundedAvgWeeklyUsage = 0 THEN 'zero'
            WHEN RoundedAvgWeeklyUsage = 1 THEN 'once'
            WHEN RoundedAvgWeeklyUsage = 2 THEN 'twice'
            WHEN RoundedAvgWeeklyUsage = 3 THEN 'thrice'
            WHEN RoundedAvgWeeklyUsage = 4 THEN 'four'
            WHEN RoundedAvgWeeklyUsage >= 5 THEN 'five_plus'
        END AS UsageCategory,
        COUNT(user_id) AS UsersCount
    FROM
        AverageWeeklyUsageRounded
    GROUP BY
        Month, UsageCategory
    """

def total_users_per_month():
    return """
    SELECT
        Month,
        SUM(UsersCount) AS TotalUsers
    FROM
        UsageCountPerCategory
    GROUP BY
        Month
    """

def build_usage_frequency_query(formatted_year_month: str, formatted_prev_year_month: str) -> str:
    """使用頻度に関するSQLクエリを構築する関数"""
    return f"""
    WITH UserWeeklyUsage AS (
        SELECT
            user_id,
            strftime('%Y-%m', DATE) AS Month,
            COUNT(DISTINCT strftime('%W', DATE)) AS WeeksUsed,
            COUNT(*) AS TotalUsage
        FROM
            final_combined_data
        WHERE
            Month IN ('{formatted_year_month}', '{formatted_prev_year_month}')
        GROUP BY
            user_id, Month
    ),
    AverageWeeklyUsageRounded AS (
        SELECT
            Month,
            user_id,
            ROUND(TotalUsage * 1.0 / WeeksUsed) AS RoundedAvgWeeklyUsage
        FROM
            UserWeeklyUsage
    ),
    UsageCountPerCategory AS (
        SELECT
            Month,
            CASE
                WHEN RoundedAvgWeeklyUsage = 0 THEN 'zero'
                WHEN RoundedAvgWeeklyUsage = 1 THEN 'once'
                WHEN RoundedAvgWeeklyUsage = 2 THEN 'twice'
                WHEN RoundedAvgWeeklyUsage = 3 THEN 'thrice'
                WHEN RoundedAvgWeeklyUsage = 4 THEN 'four'
                WHEN RoundedAvgWeeklyUsage >= 5 THEN 'five_plus'
            END AS UsageCategory,
            COUNT(user_id) AS UsersCount
        FROM
            AverageWeeklyUsageRounded
        GROUP BY
            Month, UsageCategory
    ),
    TotalUsersPerMonth AS (
        SELECT
            Month,
            SUM(UsersCount) AS TotalUsers
        FROM
            UsageCountPerCategory
        GROUP BY
            Month
    )
    SELECT
        U.Month,
        U.UsageCategory,
        U.UsersCount,
        ROUND((U.UsersCount * 100.0 / T.TotalUsers), 1) AS PercentageOfTotalUsers
    FROM
        UsageCountPerCategory U
        JOIN TotalUsersPerMonth T ON U.Month = T.Month
    ORDER BY
        U.Month, U.UsageCategory
    """

def format_usage_frequency_result(year_month: str, current_result: list, previous_result: list) -> dict:
    # 先月のデータをカテゴリごとにマッピング
    previous_data_map = {item['UsageCategory']: item for item in previous_result}

    # カテゴリのリスト
    categories = ['zero', 'once', 'twice', 'thrice', 'four', 'five_plus']

    formatted_data = {year_month: {"freq": {}}}

    for category in categories:
        current_item = next((item for item in current_result if item['UsageCategory'] == category), None)
        previous_item = previous_data_map.get(category, None)

        current_count = current_item['UsersCount'] if current_item else 0
        previous_count = previous_item['UsersCount'] if previous_item else 0

        # 成長率を計算
        growth_rate = calculate_growth_rate(current_count, previous_count)

        formatted_data[year_month]["freq"][category] = {
            "avg": current_count,
            "pct": current_item['PercentageOfTotalUsers'] if current_item else 0,
            "gr": growth_rate
        }

    return formatted_data

def calculate_growth_rate(current_count, previous_count):
    """成長率を計算するヘルパー関数"""
    if previous_count > 0:
        return ((current_count - previous_count) / previous_count) * 100
    else:
        return "N/A"


def get_previous_month_data(year_month: str):
    # 年月から先月の年月を計算
    year, month = int(year_month[:4]), int(year_month[4:6])
    if month == 1:
        previous_month = f"{year-1}12"
        prev_prev_month = f"{year-1}11"  # 前々月
    elif month == 2:
        previous_month = f"{year}01"
        prev_prev_month = f"{year-1}12"  # 前々月
    else:
        previous_month = f"{year}{month-1:02d}"
        prev_prev_month = f"{year}{month-2:02d}"  # 前々月

    # 先月のデータを取得するクエリを構築
    formatted_previous_month = f"{previous_month[:4]}-{previous_month[4:6]}"
    formatted_prev_prev_month = f"{prev_prev_month[:4]}-{prev_prev_month[4:6]}"
    previous_month_query = build_usage_frequency_query(formatted_previous_month, formatted_prev_prev_month)
    
    # クエリを実行
    previous_month_result = execute_query(previous_month_query)
    return previous_month_result

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

    
@app.get("/usage-frequency/{year_month}")
async def get_usage_frequency(year_month: str):
    formatted_year_month = f"{year_month[:4]}-{year_month[4:6]}"
    
    # 前月の年月を計算
    year, month = int(year_month[:4]), int(year_month[4:6])
    if month == 1:
        prev_year_month = f"{year-1}12"
    else:
        prev_year_month = f"{year}{month-1:02d}"
    formatted_prev_year_month = f"{prev_year_month[:4]}-{prev_year_month[4:6]}"
    
    current_query = build_usage_frequency_query(formatted_year_month, formatted_prev_year_month)
    current_result = execute_query(current_query)
    
    if not current_result:
        return {"error": "No data found for the specified year_month."}

    # 先月のデータを取得
    previous_result = get_previous_month_data(year_month)

    # 成長率を含めて結果を整形
    formatted_result = format_usage_frequency_result(year_month, current_result, previous_result)
    return formatted_result


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
