import os
import time
from datetime import datetime
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET')
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
credentials = ServiceAccountCredentials.from_json_keyfile_dict(eval(credentials_json), scope)
gc = gspread.authorize(credentials)
spreadsheet = gc.open('users')
users_ws = spreadsheet.worksheet('users')

user_states = {}  # { user_id: {'status': 'idle'/'awaiting_credentials'/'logged_in', 'attempts': int, 'last_auth_time': float, 'name': str, 'key': str, 'grade': int} }
AUTH_TIMEOUT = 600  # 10分

def is_logged_in(user_id):
    state = user_states.get(user_id)
    if not state:
        return False
    if state.get('status') != 'logged_in':
        return False
    if time.time() - state.get('last_auth_time', 0) > AUTH_TIMEOUT:
        user_states[user_id] = {'status': 'idle', 'attempts': 0}
        return False
    return True

def get_user_key_map():
    records = users_ws.get_all_records()
    return {rec['name']: rec['key'] for rec in records if 'name' in rec and 'key' in rec}

def update_last_auth(name):
    records = users_ws.get_all_records()
    for idx, rec in enumerate(records, start=2):
        if rec.get('name') == name:
            users_ws.update_cell(idx, users_ws.find('last_auth').col, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
    state = user_states.get(user_id, {'status': 'idle', 'attempts': 0})

    if text.lower() == "login":
        user_states[user_id] = {'status': 'awaiting_credentials', 'attempts': 0}
        reply_text = ("ログインを開始します。\n"
                      "名前、学年、キーを半角スペース区切りで入力してください。\n"
                      "例）サブ 2 sub0526\n"
                      "学年は1～4の半角数字で入力してください。")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    if state["status"] == "awaiting_credentials":
        user_states[user_id]['attempts'] += 1
        parts = text.split()
        if len(parts) != 3:
            reply_text = ("形式が正しくありません。\n"
                          "名前、学年、キーを半角スペース区切りで入力してください。\n"
                          "例）サブ 2 sub0526")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            return
        name, grade_str, key = parts

        # 学年検証
        if not grade_str.isdigit():
            reply_text = "学年は1～4の半角数字で入力してください。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            return
        grade = int(grade_str)
        if grade < 1 or grade > 4:
            reply_text = "学年は1～4の半角数字で入力してください。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            return

        user_key_map = get_user_key_map()
        if name in user_key_map and user_key_map[name] == key:
            try:
                update_last_auth(name)
            except Exception as e:
                print(f"update_last_auth error: {e}")
            user_states[user_id] = {
                'status': 'logged_in',
                'attempts': 0,
                'last_auth_time': time.time(),
                'name': name,
                'key': key,
                'grade': grade
            }
            reply_text = f"認証成功しました。{name}さん、ようこそ！"
        else:
            if state["attempts"] >= 3:
                user_states[user_id] = {'status': 'idle', 'attempts': 0}
                reply_text = "認証に3回失敗しました。最初からやり直してください。"
            else:
                reply_text = f"認証に失敗しました。残り{3 - state['attempts']}回です。"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    if is_logged_in(user_id):
        reply_text = "ログイン済みの機能です。"
    else:
        reply_text = '「login」と入力して認証を開始してください。'

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
