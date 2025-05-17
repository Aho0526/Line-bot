import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# 環境変数から認証情報を読み込み
credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
gc = gspread.authorize(credentials)

# users用スプレッドシートに接続
USERS_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1wZR1Tdupldp0RVOm00QAbE9-muz47unt_WhxagdirFA/edit"
users_spreadsheet = gc.open_by_url(USERS_SPREADSHEET_URL)
users_ws = users_spreadsheet.worksheet("users")  # 認証情報シート

# database用スプレッドシートに接続
DATABASE_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/11ZlpV2yl9aA3gxpS-JhBxgNniaxlDP1NO_4XmpGvg54/edit"
database_spreadsheet = gc.open_by_url(DATABASE_SPREADSHEET_URL)
data_ws = database_spreadsheet.worksheet("database")  # 記録データシート

# 指定ユーザーの認証チェック
def check_credentials(name, key):
    records = users_ws.get_all_records()
    for row in records:
        if row['name'] == name and row['key'] == key:
            return True
    return False

# 最終ログイン時間を更新
def update_login_time(name):
    cell = users_ws.find(name)
    if cell:
        users_ws.update_cell(cell.row, 3, datetime.now().isoformat())

# 最終ログイン時間を取得
def get_last_login_time(name):
    cell = users_ws.find(name)
    if cell:
        return users_ws.cell(cell.row, 3).value
    return None
