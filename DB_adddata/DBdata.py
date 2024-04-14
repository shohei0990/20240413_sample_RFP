import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from datetime import datetime

# データベースへの接続
engine = create_engine('sqlite:///pop-make-up_DB_add.db')

# テーブルをDataFrameとして読み込む
reservations = pd.read_sql('SELECT * FROM reservations', engine)
users = pd.read_sql('SELECT * FROM users', engine)
employee = pd.read_sql('SELECT * FROM employee', engine)
stocks = pd.read_sql('SELECT * FROM stocks', engine)
stores = pd.read_sql('SELECT * FROM stores', engine)
dates = pd.read_sql('SELECT * FROM dates', engine)

# reservationsとusersを結合し、不要なIDカラムを削除
reservations_users = pd.merge(reservations, users, left_on='USER_ID', right_on='ID').drop('ID_y', axis=1)
reservations_users.drop(['MY_COUPON_ID'], axis=1, inplace=True)
reservations_users.drop(['RSV_TIME'], axis=1, inplace=True)
reservations_users.drop(['MET'], axis=1, inplace=True)
reservations_users.drop(['USER_NAME'], axis=1, inplace=True)
reservations_users.drop(['PASSWORD'], axis=1, inplace=True)
reservations_users.drop(['IS_ACTIVE'], axis=1, inplace=True)

st.title('reservations/users 結合')
st.write(reservations_users)

# usersとemployeeを結合し、不要なIDカラムを削除
reservations_users_employee = pd.merge(reservations_users, employee, left_on='employee_ID', right_on='employee_ID').drop('employee_ID', axis=1)
reservations_users_employee.drop(['LastName'], axis=1, inplace=True)
reservations_users_employee.drop(['FirstName'], axis=1, inplace=True)
reservations_users_employee.drop(['HireDate'], axis=1, inplace=True)
st.title('reservations_users/employee 結合')
st.write(reservations_users_employee)

# 現在の年を取得
current_year = datetime.now().year

# birthdayを年代別に変換し、不要なbirthdayカラムを削除
def calculate_age_group(row):
    birth_year = pd.to_datetime(row['birthday']).year
    age = current_year - birth_year
    if age < 30:
        return '20代'
    elif age < 40:
        return '30代'
    elif age < 50:
        return '40代'
    elif age < 60:
        return '50代'
    else:
        return '60代以上'

reservations_users_employee['age_group'] = reservations_users_employee.apply(calculate_age_group, axis=1)
reservations_users_employee.drop(['birthday'], axis=1, inplace=True)

# Streamlitアプリのタイトル
st.title('ユーザー情報✖️予約情報の結合結果）')

# データフレームを表示
st.write(reservations_users_employee)

# stocksとstoresを結合し、不要なIDカラムを削除
stocks_stores = pd.merge(stocks, stores, left_on='STORE_ID', right_on='ID').drop('ID_y', axis=1)
stocks_stores.drop(['LOT'], axis=1, inplace=True)
stocks_stores.drop(['BEST_BY_DAY'], axis=1, inplace=True)
stocks_stores.drop(['PIECES'], axis=1, inplace=True)
st.title('stocks/stores 結合')
st.write(stocks_stores)

# stocksとdatesを結合し、不要なIDカラムを削除
stocks_stores_dates = pd.merge(stocks_stores, dates, left_on='DATE_ID', right_on='ID').drop(['ID', 'DATE_ID'], axis=1)
st.title('stocks_stores/dates 結合')
st.write(stocks_stores_dates)

# 最終的なデータフレームを作成し、不要なSTOCK_IDカラムを削除
final_df = pd.merge(reservations_users_employee, stocks_stores_dates, left_on='STOCK_ID', right_on='ID_x').drop('ID_x_y', axis=1)

# 最終的なデータフレームをStreamlitで表示
st.title('結合データ')
st.write(final_df)

final_df.drop(['EMAIL'], axis=1, inplace=True)
final_df.drop(['Department'], axis=1, inplace=True)

# 最終的なデータフレームのカラム名 'ID_x_x' を 'RSV_ID' に変更
final_df.rename(columns={'ID_x_x': 'RSV_ID'}, inplace=True)


# 最終的なデータフレームをStreamlitで表示
st.title('最終結合データ')
st.write(final_df)