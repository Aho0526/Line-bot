import os
import json
import datetime
import random
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
spreadsheet = gc.open(spreadsheet_name)
worksheet = spreadsheet.sheet1

# 一時的なユーザー状態を保存（メモリ上のみ。サーバ再起動で消える）
user_states = {}  # user_id: {'mode': 'login', 'step': 1/2/3, 'login_data': {}}
otp_store = {}    # user_id: {'otp': 6桁コード, 'requester_id': str, 'timestamp': datetime}

def generate_otp():
    return str(random.randint(100000, 999999))

def now_str():
    return str(datetime.datetime.now())

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

    # ログインモードに入るためのコマンド
    if text.lower() == "login":
        user_states[user_id] = {'mode': 'login', 'step': 1, 'login_data': {}}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="ログインするには、名前、学年、キーの順で入力してください。\n例: 太郎 2 tarou123"
            )
        )
        return

    # ログインモードの処理
    if user_id in user_states and user_states[user_id].get('mode') == 'login':
        # 名前 学年 キーの入力受付
        parts = text.split(" ")
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\n名前 学年 キー の順でスペース区切りで入力してください。\n例: 太郎 2 tarou123")
            )
            return

        name, grade, key = parts
        users = worksheet.get_all_values()
        if not users:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="シートが空です。管理者に連絡してください。")
            )
            user_states.pop(user_id)
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
                user_states.pop(user_id)
                return

        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        last_auth_col = header.index("last_auth")

        found = False
        for i, row in enumerate(data, start=2):  # 2行目から
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found = True
                registered_user_id = row[user_id_col]
                if registered_user_id == "":
                    # 新規認証
                    worksheet.update_cell(i, user_id_col + 1, user_id)
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="認証成功！ユーザー情報を登録しました。")
                    )
                    user_states.pop(user_id)
                elif registered_user_id == user_id:
                    # すでに自分の端末で認証済み
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ログイン成功！ようこそ。")
                    )
                    user_states.pop(user_id)
                else:
                    # 別端末からログイン要求
                    otp = generate_otp()
                    otp_store[registered_user_id] = {
                        "otp": otp,
                        "requester_id": user_id,
                        "name": name,
                        "timestamp": datetime.datetime.now()
                    }
                    # 要求元への案内
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(
                            text="このアカウントはすでに別の端末からログインを済ましています。\nこの操作があなたのものであれば元の端末に対して確認コードを送信しているのでコードを確認しログインを完了してください。"
                        )
                    )
                    # 元のアカウント所持者へOTP送信
                    line_bot_api.push_message(
                        registered_user_id,
                        TextSendMessage(
                            text=f"{name}があなたのアカウントに対しログインを試みています。\nこの操作があなたのものであれば以下のコードをログインを試みている端末に入力し、ログインを完了してください。\nもしもあなたの操作でない場合はキーが漏れている可能性があるので直ちに変更してください。\n\n確認コード: {otp}"
                        )
                    )
                    user_states[user_id]["step"] = 2  # OTP待ち状態に
                return

        if not found:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="認証失敗。名前・学年・キーを確認してください。")
            )
            user_states.pop(user_id)
        return

    # OTP認証の流れ
    if user_id in user_states and user_states[user_id].get("step") == 2:
        # OTP入力を期待
        input_otp = text.strip()
        # どのアカウントのOTPか判定
        for owner_id, otp_info in otp_store.items():
            if otp_info["requester_id"] == user_id:
                if otp_info["otp"] == input_otp:
                    # 認証成功→user_idをシートに登録
                    users = worksheet.get_all_values()
                    header = users[0]
                    data = users[1:]
                    name_col = header.index("name")
                    grade_col = header.index("grade")
                    key_col = header.index("key")
                    user_id_col = header.index("user_id")
                    last_auth_col = header.index("last_auth")
                    # 入力時のname, grade, keyを取得
                    name = otp_info["name"]
                    for i, row in enumerate(data, start=2):
                        if row[name_col] == name and row[user_id_col] == owner_id:
                            worksheet.update_cell(i, user_id_col + 1, user_id)
                            worksheet.update_cell(i, last_auth_col + 1, now_str())
                            break

                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="OTP認証に成功しました。ログインが完了しました。")
                    )
                    line_bot_api.push_message(
                        owner_id,
                        TextSendMessage(text="確認コードが正しく入力され、端末が切り替わりました。")
                    )
                    otp_store.pop(owner_id)
                    user_states.pop(user_id)
                    return
                else:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="確認コードが正しくありません。再度入力してください。")
                    )
                    return

    # 通常のメッセージ
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="「login」と送信するとログインモードになります。")
    )

if __name__ == "__main__":
    app.run()
