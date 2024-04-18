import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# データベースへの接続
engine = create_engine('sqlite:///pop-make-up_DB_add.db')



# SQLクエリを使用して必要な結合とカラムの選択を行う。まずはユーザ情報の結合
query_1 = """
SELECT
    r.id AS RSV_ID,
    r.user_id,
    r.stock_id,
    -- u.employee_ID,
    -- e.birthday,
    e.Gender,
    -- e.Department,
    CASE 
        WHEN strftime('%Y', 'now') - strftime('%Y', e.birthday) < 30 THEN '20代'
        WHEN strftime('%Y', 'now') - strftime('%Y', e.birthday) < 40 THEN '30代'
        WHEN strftime('%Y', 'now') - strftime('%Y', e.birthday) < 50 THEN '40代'
        WHEN strftime('%Y', 'now') - strftime('%Y', e.birthday) < 60 THEN '50代'
        ELSE '60代以上'
    END AS age_group
FROM reservations r
JOIN users u ON r.user_id = u.id
JOIN employee e ON u.employee_ID = e.employee_ID
"""

# SQLクエリの結果をデータフレームとして読み込む
df_1 = pd.read_sql_query(query_1, engine)

# Streamlitアプリのタイトル
st.title('ユーザー情報✖️予約情報の結合結果（年代別変換後）')

# データフレームをStreamlitで表示
st.write(df_1)

# 処理②：お店情報、STOCK情報との結合のSQLクエリ
query_2 = """
SELECT
    df_1.*,
    -- st.PRD_ID,
    s.STORE,
    st.STORE_ID,
    -- st.DATE_ID,
    d.DATE,
    d.WEEK
FROM
    ({query_1}) df_1
JOIN stocks st ON df_1.stock_id = st.ID
JOIN stores s ON st.STORE_ID = s.ID
JOIN dates d ON st.DATE_ID = d.ID
""".format(query_1=query_1)

# SQLクエリの結果をデータフレームとして読み込む
df_2 = pd.read_sql_query(query_2, engine)
# 最終結合データをデータベースに保存
df_2.to_sql('final_combined_data', con=engine, if_exists='replace', index=False)

# Streamlitアプリのタイトル（処理②の結果）
st.title('最終結合データ')

# データフレームをStreamlitで表示
st.write(df_2)

# 結合後のデータテーブルからダッシュボード用に
# 計算処理を行う。

# Streamlitアプリのタイトル（処理②の結果）
st.title('ダッシュボード用計算処理:確認目的')

# ① 月毎の総利用者数と総利用食数のクエリ
# 月毎の総利用者数と総利用食数のクエリ
monthly_summary_query = """
SELECT
    strftime('%Y-%m', DATE) AS Month,        -- DATEカラムから年と月を抽出して、Monthという名前のカラムに格納します。
    COUNT(DISTINCT user_id) AS Total_Users,  -- user_idの異なる値の数をカウントし、それをTotal_Usersとして集計します。これにより、各月のユニークな利用者数が求められます。
    COUNT(stock_id) AS Total_Stocks_Used     -- stock_idの総数をカウントし、Total_Stocks_Usedとして集計します。これにより、各月の総利用食数が求められます。
FROM 
    final_combined_data      -- final_combined_dataテーブルからデータを取得します。
GROUP BY
    strftime('%Y-%m', DATE)  -- DATEカラムから抽出した年月でグループ化し、月毎のデータを集計します。
"""
# もし日付ごとにグループ化する場合は、strftime関数のフォーマット文字列を変更して、年月日（'%Y-%m-%d'）を指定
# SQLクエリを実行し、結果をPandasのデータフレームに読み込みます。
df_monthly_summary = pd.read_sql_query(monthly_summary_query, engine)  
st.write(" ①：月毎の総利用者数と総利用食数", df_monthly_summary)  

usage_count_and_percentage_rounded_query = """
WITH UserWeeklyUsage AS (
    SELECT
        user_id,
        strftime('%Y-%m', DATE) AS Month,
        COUNT(DISTINCT strftime('%W', DATE)) AS WeeksUsed,
        COUNT(*) AS TotalUsage
    FROM
        final_combined_data
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
            WHEN RoundedAvgWeeklyUsage = 0 THEN '週0回'
            WHEN RoundedAvgWeeklyUsage = 1 THEN '週1回'
            WHEN RoundedAvgWeeklyUsage = 2 THEN '週2回'
            WHEN RoundedAvgWeeklyUsage = 3 THEN '週3回'
            WHEN RoundedAvgWeeklyUsage = 4 THEN '週4回'
            WHEN RoundedAvgWeeklyUsage >= 5 THEN '週5回以上'
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

df_usage_count_and_percentage_rounded = pd.read_sql_query(usage_count_and_percentage_rounded_query, engine)
st.write("②月毎の週利用回数（四捨五入後の平均）ごとのユーザー数とその合計に対する割合", df_usage_count_and_percentage_rounded)





# 年代別の利用者数と利用食数のクエリ
age_group_summary_query = """
SELECT
    strftime('%Y-%m', DATE) AS Month,
    age_group,
    COUNT(DISTINCT user_id) AS Users_Per_Age_Group,
    COUNT(stock_id) AS Stocks_Used_Per_Age_Group
FROM
    final_combined_data
GROUP BY
    strftime('%Y-%m', DATE), age_group
"""
df_age_group_summary = pd.read_sql_query(age_group_summary_query, engine)
st.write("③：年代別の利用者数と利用食数", df_age_group_summary)

# 店舗別の利用者数と利用食数のクエリ
store_summary_query = """
SELECT
    strftime('%Y-%m', DATE) AS Month,
    STORE,
    COUNT(DISTINCT user_id) AS Users_Per_Store,
    COUNT(stock_id) AS Stocks_Used_Per_Store
FROM
    final_combined_data
GROUP BY
    strftime('%Y-%m', DATE), STORE
"""
df_store_summary = pd.read_sql_query(store_summary_query, engine)
st.write("③：店舗別の利用者数と利用食数", df_store_summary)