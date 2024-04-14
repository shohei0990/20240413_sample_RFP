import streamlit as st
import sqlite3
import pandas as pd

# Streamlitアプリのタイトルを設定
st.title('SQLite Database Viewer')

# データベースに接続
conn = sqlite3.connect('pop-make-up_DB.db')

# テーブル一覧を取得
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# 各テーブルのデータをデータフレームに読み込み、それを表示
for table_name in tables:
    table_name = table_name[0]
    st.subheader(f"Table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    st.dataframe(df)

# 接続を閉じる
conn.close()