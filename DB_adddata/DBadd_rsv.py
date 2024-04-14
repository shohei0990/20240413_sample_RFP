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

st.subheader(f"Table: {'reservations'}")
query = f"SELECT * FROM {'reservations'}"
df = pd.read_sql_query(query, conn)
st.dataframe(df)

# 既存のデータを読み込む（データ型指定を省略）
existing_reservations_df = df

# データのクリーニング: 空文字列を 0 に置き換え
existing_reservations_df.replace('', 0, inplace=True)

# ランダムなデータ生成と新規DataFrameの作成、データベースへの追加処理
np.random.seed(0)
num_entries = 4999

user_ids = np.random.randint(1, 80, num_entries)
stock_ids = np.random.choice(range(1, 5000), num_entries, replace=False)
my_coupon_ids = np.random.randint(1000, 9999, num_entries)
rsv_times = [(datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_entries)]
met_values = np.random.choice([0, 1], num_entries)

new_reservations_df = pd.DataFrame({
    'ID': range(int(existing_reservations_df['ID'].max()) + 1, int(existing_reservations_df['ID'].max()) + num_entries+ 1),
    'USER_ID': user_ids,
    'STOCK_ID': stock_ids,
    'MY_COUPON_ID': my_coupon_ids,
    'RSV_TIME': rsv_times,
    'MET': met_values
})

combined_df = pd.concat([existing_reservations_df, new_reservations_df], ignore_index=True)

# 結合したデータフレームをデータベースに保存する前に、日付と時刻の列を文字列に変換
combined_df['RSV_TIME'] = pd.to_datetime(combined_df['RSV_TIME']).dt.strftime('%Y-%m-%d')

# 結合したデータフレームをデータベースに保存
combined_df.to_sql('reservations', con=conn, if_exists='replace', index=False)

# Streamlitでデータフレームを表示
st.write("結合したデータフレーム:", combined_df)

# 接続を閉じる
conn.close()

print("データベースが更新されました。")