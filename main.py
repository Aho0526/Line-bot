import os
import re
import json
import logging
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import unicodedata

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET")

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    logging.error("LINE_CHANNEL_ACCESS_TOKEN or LINE_CHANNEL_SECRET is not set.")
    exit(1)

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ユーザーデータの保存用（例としてメモリ上に保持）
user_data = {}

def is_valid_grade(grade_str):
    # 全角→半角変換
    norm_str = unicodedata.normalize('NFKC', grade_str)
    logging.info(f"Normalized grade input: {norm_str}")

    # 半角数字のみで1〜4の範囲かチェック
    if re.fullmatch(r"[1-4]", norm_str):
        return True, norm_str
    else:
        return False, norm_str

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    logging.info(f"Request body: {body}")

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()

    # 例：ログイン処理の流れの一部として学年を受け取る想定
    if user_id in user_data and user_data[user_id].get("awaiting_grade"):
        valid, normalized_grade = is_valid_grade(text)
        if valid:
            user_data[user_id]["grade"] = normalized_grade
            user_data[user_id]["awaiting_grade"] = False
            reply = f"学年を「{normalized_grade}年」として登録しました。"
        else:
            reply = "学年は半角数字（1～4）で入力してください。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
        return

    # それ以外のメッセージ処理（例）
    if text.lower() == "login":
        user_data[user_id] = {"awaiting_grade": True}
        reply = "ログインを開始します。学年を半角数字（1～4）で入力してください。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
        return

    # 既存機能の呼び出しなどはここに

    # デフォルト応答
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="コマンドが認識できません。"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

