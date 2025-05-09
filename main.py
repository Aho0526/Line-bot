import os
import re
import base64
from datetime import datetime
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

if not os.path.exists("credentials.json"):
    cred_data = os.environ.get("GOOGLE_CREDENTIALS")
    if cred_data:
        with open("credentials.json", "wb") as f:
            f.write(base64.b64decode(cred_data))

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(credentials)
sheet = client.open("LineBot").sheet1

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
        now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        sheet.append_row([name, weight, now])
        return "記録しました。"
    except Exception as e:
        return f"記録に失敗しました: {str(e)}"

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
