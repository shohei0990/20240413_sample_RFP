import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import sqlite3

# Streamlitアプリのタイトルを設定
st.title('SQLite Database Viewer')

# データベースに接続
conn = sqlite3.connect('pop-make-up_DB.db')

# テーブル一覧を取得
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# ここからstocksテーブルのDATE_IDを更新する処理を追加
# stocksテーブルを読み込む
stocks_df = pd.read_sql('SELECT * FROM stocks', conn)

# 既存のデータ数を取得
existing_data_count = len(stocks_df)

# 追加するデータ数を計算
additional_data_count = 5000 - existing_data_count

# 最後のIDを取得
last_id = stocks_df['ID'].max()

# 追加するデータを生成
additional_data = pd.DataFrame({
    'ID': range(last_id + 1, last_id + additional_data_count + 1),
    'DATE_ID': np.random.randint(1, 366, size=additional_data_count),
    'STORE_ID': np.random.randint(1, 10, size=additional_data_count),
    'PRD_ID': np.random.randint(1, 100, size=additional_data_count)
})

# 既存のデータと追加データを結合
updated_stocks_df = pd.concat([stocks_df, additional_data], ignore_index=True)

# 更新されたデータフレームをデータベースに保存
updated_stocks_df.to_sql('stocks', con=conn, if_exists='replace', index=False)

# ここまでがstocksテーブルのDATE_IDを更新する処理

# reservationsテーブルの処理を続ける
st.subheader(f"Table: {'stocks'}")
query = f"SELECT * FROM {'stocks'}"
df = pd.read_sql_query(query, conn)
st.dataframe(df)

# 以降の処理...

# 接続を閉じる
conn.close()

print("データベースが更新されました。")