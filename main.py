import os
import re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# 環境変数からチャネルシークレットとアクセストークンを取得
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

@app.route("/", methods=['POST'])
def callback():
    # LINEからの署名を取得
    signature = request.headers['X-Line-Signature']

    # リクエストボディを取得
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

def calculate_idt(message_text):
    try:
        # 正規表現でマッチング
        match = re.match(r'(\d{1,2}):(\d{2})\.(\d)\s+(\d{2,3}\.\d)\s+(m|w)', message_text.lower())
        if not match:
            return "形式が正しくありません。再確認してください。"

        mi = int(match.group(1))  # 分
        se = int(match.group(2))  # 秒
        sed = int(match.group(3)) # ミリ秒 (1桁)
        weight = float(match.group(4))  # 体重
        gender = match.group(5)        # m or w

        time_sec = mi * 60 + se + sed * 0.1

        if gender == "m":
            idt = ((101 - weight) * (20.9 / 23.0) + 333.07) / time_sec * 100
        else:
            idt = ((100 - weight) * 1.40 + 357.80) / time_sec * 100

        return f"あなたのIDTは {idt:.2f}% です。"

    except Exception as e:
        return f"エラーが発生しました: {str(e)}"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_text = event.message.text.lower()

    # 「cal idt」のリクエストがあればIDT計算のガイドを表示
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
    # それ以外のメッセージはIDT計算を実行
    else:
        reply_text = calculate_idt(user_text)

    # 応答を返す
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=reply_text)
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
