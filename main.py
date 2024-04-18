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
    "http://localhost:3000",
    "https://api.example.com"
]


app = FastAPI()

# CORSミドルウェアの設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # 全てのオリジンを許可する場合は ["*"]
    allow_credentials=True,
    allow_methods=["*"],  # または特定のHTTPメソッド ['GET', 'POST', 'PUT']
    allow_headers=["*"],  # または特定のヘッダー ['X-Custom-Header']
)

# JSONファイルからダミーデータを読み込む
with open('dummy_data.json', 'r') as file:
    dummy_data = json.load(file)

# JSONファイルから使用頻度データを読み込む
with open('usage_frequency_data.json', 'r') as file:
    usage_frequency_data = json.load(file)

# ダミーデータの読み込み
with open('group_data.json', 'r') as file:
    group_data = json.load(file)

@app.get("/monthly-summary/{year_month}")
async def get_usage_summary(year_month: str):
    # year_monthがダミーデータに存在するかチェック
    if year_month in dummy_data:
        # ダミーデータから情報を取得して返す
        return dummy_data[year_month]
    else:
        # 該当するデータがない場合はエラーメッセージを返す
        return {"error": "Data not found for the specified year_month."}
    

@app.get("/usage-frequency/{year_month}")
async def get_usage_frequency(year_month: str):
    # year_monthが使用頻度データに存在するかチェック
    if year_month in usage_frequency_data:
        # 使用頻度データから情報を取得して返す
        return usage_frequency_data[year_month]
    else:
        # 該当するデータがない場合はエラーメッセージを返す
        return {"error": "Data not found for the specified year_month."}
    
@app.get("/usage-group/{year_month}")
async def get_usage_group(year_month: str):
    # year_monthがダミーデータに存在するかチェック
    if year_month in group_data:
        # ダミーデータから情報を取得して返す
        return group_data[year_month]
    else:
        # 該当するデータがない場合はエラーメッセージを返す
        return {"error": "Data not found for the specified year_month."}