import os
import re
import time
from datetime import datetime
from flask import Flask, request, abort
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# 環境変数の取得
CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# Google Sheets 認証
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = {
    "type": os.environ["GS_TYPE"],
    "project_id": os.environ["GS_PROJECT_ID"],
    "private_key_id": os.environ["GS_PRIVATE_KEY_ID"],
    "private_key": os.environ["GS_PRIVATE_KEY"].replace('\\n', '\n'),
    "client_email": os.environ["GS_CLIENT_EMAIL"],
    "client_id": os.environ["GS_CLIENT_ID"],
    "auth_uri": os.environ["GS_AUTH_URI"],
    "token_uri": os.environ["GS_TOKEN_URI"],
    "auth_provider_x509_cert_url": os.environ["GS_AUTH_PROVIDER_CERT_URL"],
    "client_x509_cert_url": os.environ["GS_CLIENT_CERT_URL"]
}
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# シート接続
SHEET_KEY = os.environ["GS_SHEET_KEY"]
sheet = client.open_by_key(SHEET_KEY)
users_ws = sheet.worksheet("users")

# ユーザー状態管理
user_states = {}

def get_user_record_map():
    records = users_ws.get_all_records()
    return {rec['name']: rec for rec in records if 'name' in rec and 'key' in rec}

def update_last_auth(name):
    records = users_ws.get_all_records()
    for idx, rec in enumerate(records, start=2):
        if rec['name'] == name:
            users_ws.update_cell(idx, users_ws.find('last_auth').col, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers["X-Line-Signature"]
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
    reply_text = ""

    # ステータス取得
    state = user_states.get(user_id, {"status": "idle", "attempts": 0})

    if state["status"] == "idle":
        if text.lower() == "login":
            user_states[user_id] = {"status": "waiting_name", "attempts": 0}
            reply_text = "名前を入力してください："
        else:
            reply_text = "コマンドが認識されませんでした。loginと入力してください。"

    elif state["status"] == "waiting_name":
        user_states[user_id]["name"] = text
        user_states[user_id]["status"] = "waiting_grade"
        reply_text = "学年を入力してください（例：1, 2, 3）："

    elif state["status"] == "waiting_grade":
        if text.isdigit():
            user_states[user_id]["grade"] = int(text)
            user_states[user_id]["status"] = "waiting_key"
            reply_text = "認証キーを入力してください："
        else:
            reply_text = "学年は数字で入力してください："

    elif state["status"] == "waiting_key":
        key = text
        name = state.get("name")
        grade = state.get("grade")
        user_record_map = get_user_record_map()
        record = user_record_map.get(name)

        if record and record["key"] == key:
            try:
                update_last_auth(name)
                row_idx = next((i+2 for i, r in enumerate(users_ws.get_all_records()) if r.get("name") == name), None)
                if row_idx:
                    users_ws.update_cell(row_idx, users_ws.find("user_id").col, user_id)
            except Exception as e:
                print(f"update_last_auth error: {e}")

            user_states[user_id] = {
                "status": "logged_in",
                "attempts": 0,
                "last_auth_time": time.time(),
                "name": name,
                "key": key,
                "grade": grade
            }
            reply_text = f"認証成功しました。{name}さん、ようこそ！"

        else:
            if name not in user_record_map:
                try:
                    headers = users_ws.row_values(1)
                    new_row = [""] * len(headers)
                    new_row[headers.index("name")] = name
                    new_row[headers.index("grade")] = grade
                    new_row[headers.index("key")] = key
                    new_row[headers.index("user_id")] = user_id
                    new_row[headers.index("last_auth")] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    users_ws.append_row(new_row)

                    user_states[user_id] = {
                        "status": "logged_in",
                        "attempts": 0,
                        "last_auth_time": time.time(),
                        "name": name,
                        "key": key,
                        "grade": grade
                    }
                    reply_text = f"初回登録が完了しました！{name}さん、ようこそ！"
                except Exception as e:
                    print(f"初回登録失敗: {e}")
                    reply_text = "登録中にエラーが発生しました。管理者に連絡してください。"
            else:
                user_states[user_id]["attempts"] += 1
                if user_states[user_id]["attempts"] >= 3:
                    user_states[user_id] = {"status": "idle", "attempts": 0}
                    reply_text = "認証に3回失敗しました。最初からやり直してください。"
                else:
                    remaining = 3 - user_states[user_id]["attempts"]
                    reply_text = f"認証に失敗しました。残り{remaining}回です。"

    elif state["status"] == "logged_in":
        reply_text = f"{state.get('name')}さん、すでにログイン済みです。"

    else:
        reply_text = "エラーが発生しました。最初からやり直してください。"

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))

