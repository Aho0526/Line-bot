import os
import json
import datetime
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# 環境変数からLINEの設定を取得
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# Google認証情報を環境変数から読み込む
credentials_json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON")
if credentials_json_str is None:
    raise ValueError("GOOGLE_CREDENTIALS_JSON が設定されていません。")

credentials_info = json.loads(credentials_json_str)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)

# スプレッドシート名を環境変数から取得（なければ "user_database" を使う）
spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "user_database")

# スプレッドシートに接続
gc = gspread.authorize(creds)
spreadsheet = gc.open(spreadsheet_name)
worksheet = spreadsheet.sheet1

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

    if text.lower().startswith("login "):
        try:
            _, name, grade, key = text.split(" ")
        except ValueError:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\nlogin 名前 学年 キー の形式で入力してください。")
            )
            return

        users = worksheet.get_all_values()
        if not users:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="シートが空です。管理者に連絡してください。")
            )
            return

        header = users[0]
        data = users[1:]

        # 必要な列名が存在するかチェック
        required_columns = ["name", "grade", "key", "user_id", "last_auth"]
        for col in required_columns:
            if col not in header:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"シートに '{col}' 列がありません。管理者に連絡してください。")
                )
                return

        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        last_auth_col = header.index("last_auth")

        for i, row in enumerate(data, start=2):  # ヘッダーが1行目なので2行目から
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                if row[user_id_col] == "":
                    worksheet.update_cell(i, user_id_col + 1, user_id)
                    worksheet.update_cell(i, last_auth_col + 1, str(datetime.datetime.now()))
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="認証成功！ユーザー情報を登録しました。")
                    )
                elif row[user_id_col] == user_id:
                    worksheet.update_cell(i, last_auth_col + 1, str(datetime.datetime.now()))
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ログイン成功！ようこそ。")
                    )
                else:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="別の端末から登録済みです。再認証が必要です。")
                    )
                return

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="認証失敗。名前・学年・キーを確認してください。")
        )

    else:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="ログインするには\nlogin 名前 学年 キー\nの形式で送信してください。")
        )

if __name__ == "__main__":
    app.run()
