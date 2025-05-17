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

# スプレッドシートに接続
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1wZR1Tdupldp0RVOm00QAbE9-muz47unt_WhxagdirFA/edit"
spreadsheet = gc.open_by_url(SPREADSHEET_URL)

# 各ワークシートへの参照
users_ws = spreadsheet.worksheet("users")     # 認証情報
data_ws = spreadsheet.worksheet("データ")     # 記録データなど

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
