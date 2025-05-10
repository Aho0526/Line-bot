import os
import re
import requests
from datetime import datetime
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

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

def get_tide_info_specific(message_text):
    try:
        match = re.match(r'(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2})', message_text)
        if not match:
            return "日付と時間の形式が正しくありません。例: 2025/05/26 07:00"

        yyyy, mm, dd, hh, min = match.groups()
        date_str = f"{yyyy}-{mm}-{dd}"
        time_str = f"{hh}:{min}"
        dt_target = f"{date_str}T{time_str}:00+09:00"

        url = "https://www.data.jma.go.jp/gmd/kaiyou/data/db/tide/suisan/txt/2024/tosa.txt"  # 例: 種崎（高知県）の潮位データ（要確認）
        response = requests.get(url)
        if response.status_code != 200:
            return "潮位データの取得に失敗しました。"

        # プレーンテキストを行ごとに処理
        lines = response.text.splitlines()
        for line in lines:
            if time_str in line:
                return f"{yyyy}/{mm}/{dd} {hh}:{min} の潮位データ:\n{line.strip()}"

        return f"{yyyy}/{mm}/{dd} {hh}:{min} の潮位データが見つかりませんでした。"

    except Exception as e:
        return f"潮位取得時にエラーが発生しました: {str(e)}"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_text = event.message.text.lower().strip()

    if "cal idt" in user_text:
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
    elif "tide info-specific" in user_text:
        reply_text = (
            "日と時間を指定後に送信すると指定された時間の潮位を確認することができます。\n"
            "観測場所は種崎海水浴場で、気象庁から提供されているデータを元に潮位をお知らせします。\n\n"
            "形式: 20yy/mm/dd hh:mm\n"
            "例: 2025/05/26 07:00\n"
            "※10分間隔で入力してください。"
        )
    elif re.match(r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}", user_text):
        reply_text = get_tide_info_specific(user_text)
    else:
        reply_text = calculate_idt(user_text)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=reply_text)
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
