# データベースに接続
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import sqlite3

conn = sqlite3.connect('pop-make-up_DB_add.db')

cursor = conn.cursor()
# データベース内のusersテーブルからuser_idが100より大きいレコードを削除
cursor.execute("DELETE FROM users WHERE id > 100")
conn.commit()

print("user_idが100より大きいレコードを削除しました。")

# 削除操作後のレコード数を確認
cursor.execute("SELECT COUNT(*) FROM users")
count_after_deletion = cursor.fetchone()[0]
print(f"削除後のレコード数: {count_after_deletion}")