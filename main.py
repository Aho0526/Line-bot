import os
import re
import json
from datetime import datetime
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# LINE API 認証情報
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# Google Sheets API 認証情報（環境変数から読み込み）
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 環境変数から認証情報を取得
google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Google credentialsが読み込まれているか確認
print("GOOGLE_CREDENTIALS_JSON:", google_credentials_json)

if google_credentials_json:
    credentials_info = json.loads(google_credentials_json)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_info, scope)
    gc = gspread.authorize(credentials)
    sheet = gc.open("LineBot").sheet1  # スプレッドシート名を適宜変更
else:
    print("Google credentials are missing.")
    sheet = None

@app.route("/", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

def calculate_idt(message_text):
    try:
        match = re.match(r'(\d{1,2}):(\d{2})\.(\d)\s+(\d{2,3}\.\d)\s+(m|w)', message_text.lower())
        if not match:
            return "形式が正しくありません。再確認してください。"

        mi = int(match.group(1))
        se = int(match.group(2))
        sed = int(match.group(3))
        weight = float(match.group(4))
        gender = match.group(5)

        time_sec = mi * 60 + se + sed * 0.1

        if gender == "m":
            idt = ((101 - weight) * (20.9 / 23.0) + 333.07) / time_sec * 100
        else:
            idt = ((100 - weight) * 1.40 + 357.80) / time_sec * 100

        return f"あなたのIDTは {idt:.2f}% です。"
    except Exception as e:
        return f"エラーが発生しました: {str(e)}"

def write_weight_record(name, weight):
    try:
        if sheet is not None:
            now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            sheet.append_row([name, weight, now])
            return "記録しました。"
        else:
            return "Google Sheetsの接続に失敗しました。"
    except Exception as e:
        return f"記録に失敗しました: {str(e)}"

# Google Sheets API 認証情報の確認（動作テスト）
def test_google_sheets():
    try:
        if sheet is not None:
            # データの書き込み確認
            sheet.append_row(["テストデータ", "2025-05-11", "テスト"])

            # データの読み込み確認
            data = sheet.get_all_records()
            print(data)  # 取得したデータを表示
        else:
            print("Google Sheetsの接続に失敗しました。")
    except Exception as e:
        print(f"Google Sheets APIエラー: {str(e)}")

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_text = event.message.text.strip()

    if "cal idt" in user_text.lower():
        reply_text = (
            "IDTの計算をするには以下の数値が揃っているか確認してください。\n\n"
            "エルゴタイム:m:ss.s (分:秒.ミリ秒)\n"
            "体重:xx.x\n\n"
            "距離は2000mで計算されます。\n"
            "2000TTのタイムとその時の体重を入力してください。\n\n"
            "数値は以下の表記通りに入力してください。\n"
            "また、性別はm/w(男性=m/女性=w)として入力してください。\n\n"
            "m:ss.s xx.x m/w\n\n"
            "記入例:タイム7:32.8、体重56.3kg、男性の場合:7:32.8 56.3 m\n"
            "空白やコロンの使い分けにご注意ください"
        )
    elif user_text.lower().startswith("make "):
        try:
            _, name, weight = user_text.split()
            weight = float(weight)
            reply_text = write_weight_record(name, weight)
        except ValueError:
            reply_text = "形式が正しくありません。\n例: make yoshiaki 60.5"
    else:
        reply_text = calculate_idt(user_text)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=reply_text)
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

    # Google Sheets API 動作確認をここで呼び出し
    test_google_sheets()
