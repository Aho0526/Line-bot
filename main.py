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
spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "users")

# スプレッドシートに接続
gc = gspread.authorize(creds)
spreadsheet = gc.open(users)
worksheet = spreadsheet.sheet1

# ログイン待ち状態管理用セット（メモリ管理）
login_waiting_users = set()

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

    # 「login」だけ受けた場合、ログイン待ち状態にする
    if text.lower() == "login":
        login_waiting_users.add(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=(
                "ログインモードに入りました。\n"
                "名前、学年、キーをスペース区切りで送信してください。\n"
                "例）太郎 2 abc123"
            ))
        )
        return

    # ログイン待ち状態のユーザーの入力を処理
    if user_id in login_waiting_users:
        parts = text.split()
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=(
                    "形式が正しくありません。\n"
                    "名前 学年 キー の3つをスペース区切りで送信してください。"
                ))
            )
            return

        name, grade, key = parts

        # 学年は半角数字であることをチェック
        if not grade.isdigit():
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="学年は半角数字で入力してください。")
            )
            return

        # ユーザー情報をシートから取得
        users = worksheet.get_all_values()
        if not users:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ユーザーデータがありません。管理者に連絡してください。")
            )
            login_waiting_users.discard(user_id)
            return

        header = users[0]
        data = users[1:]

        required_columns = ["name", "grade", "key", "user_id", "last_auth"]
        for col in required_columns:
            if col not in header:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"シートに '{col}' 列がありません。管理者に連絡してください。")
                )
                login_waiting_users.discard(user_id)
                return

        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        last_auth_col = header.index("last_auth")

        # 認証処理
        for i, row in enumerate(data, start=2):  # 2行目からデータ
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                if row[user_id_col] == "":
                    # user_id未登録なので登録する
                    worksheet.update_cell(i, user_id_col + 1, user_id)
                    worksheet.update_cell(i, last_auth_col + 1, str(datetime.datetime.now()))
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="認証成功！ユーザー情報を登録しました。")
                    )
                elif row[user_id_col] == user_id:
                    # すでに同じuser_idでログイン済み
                    worksheet.update_cell(i, last_auth_col + 1, str(datetime.datetime.now()))
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ログイン成功！ようこそ。")
                    )
                else:
                    # 別端末で登録済み
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="別の端末から登録済みです。再認証が必要です。")
                    )
                login_waiting_users.discard(user_id)
                return

        # 一致データなし＝認証失敗
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="認証失敗。名前・学年・キーを確認してください。")
        )
        login_waiting_users.discard(user_id)
        return

    # 通常メッセージ（ログインモード以外）
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="ログインするには「login」と送信してください。")
    )

if __name__ == "__main__":
    app.run()
