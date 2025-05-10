import os
import re
import csv
from datetime import datetime
import requests
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

def parse_datetime(input_text):
    match = re.match(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s+(\d{1,2}):(\d{2})', input_text)
    if not match:
        return None
    try:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        return datetime(year, month, day, hour, minute)
    except ValueError:
        return None

def get_tide_height_at(dt):
    # 高知県 種崎の潮位予測データ（10分間隔、CSV形式）
    url = "https://www.data.jma.go.jp/kaiyou/db/tide/suisan/txt/2024/txt/AS3_2024_Kochi_Tanezaki.csv"
    try:
        response = requests.get(url)
        response.encoding = 'shift_jis'
        lines = response.text.splitlines()
        reader = csv.reader(lines)
        next(reader)  # ヘッダーをスキップ

        for row in reader:
            if len(row) < 2:
                continue
            try:
                timestamp = datetime.strptime(row[0], "%Y/%m/%d %H:%M")
                if timestamp == dt:
                    return f"{timestamp.strftime('%Y/%m/%d %H:%M')} の潮位は {row[1]} cm です。"
            except ValueError:
                continue

        return "指定された日時の潮位データは見つかりませんでした。10分単位で入力されているか確認してください。"
    except Exception as e:
        return f"潮位データの取得中にエラーが発生しました: {str(e)}"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_text = event.message.text.strip()

    # IDTガイド
    if user_text.lower() == "cal idt":
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
    # 潮位ガイド
    elif user_text.lower() == "tide info-specific":
        reply_text = (
            "観測場所は種崎海水浴場で、気象庁から提供されているデータを元に潮位をお知らせします。\n\n"
            "20yy/mm/dd hh:mm\n\n"
            "yyには年, mmには月, ddには日を入れてください。\n"
            "また、hh:mmには時間を入力してください（10分間隔で指定）。\n"
            "例: 2025/05/26 07:00"
        )
    # 潮位指定の日時とみなして処理
    elif re.match(r'\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\d{1,2}:\d{2}', user_text):
        dt = parse_datetime(user_text)
        if dt:
            reply_text = get_tide_height_at(dt)
        else:
            reply_text = "日時の形式が正しくありません。例に沿って入力してください（例: 2025/05/26 07:00）"
    # IDTの計算処理
    else:
        reply_text = calculate_idt(user_text)

    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=reply_text)
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
