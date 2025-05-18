import os
import time
from datetime import datetime
from auth_state import start_auth, reset_auth, increment_attempts, get_state
from sheet_handler import get_user_key_map, update_last_auth
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# --- LINE API設定 ---
LINE_CHANNEL_ACCESS_TOKEN = os.getenv('LINE_CHANNEL_ACCESS_TOKEN')
LINE_CHANNEL_SECRET = os.getenv('LINE_CHANNEL_SECRET')
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# --- Google Sheets認証 ---
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
credentials = ServiceAccountCredentials.from_json_keyfile_dict(eval(credentials_json), scope)
gc = gspread.authorize(credentials)
spreadsheet = gc.open('users')
users_ws = spreadsheet.worksheet('users')

# --- ユーザー認証状態管理 ---
user_states = {}  # { user_id: {'status': 'idle'/'auth_waiting'/'logged_in', 'try_count': int, 'last_auth_time': float, 'name': str, 'key': str} }
AUTH_TIMEOUT = 600  # 10分(秒)

def is_logged_in(user_id):
    state = user_states.get(user_id)
    if not state:
        return False
    if state.get('status') != 'logged_in':
        return False
    if time.time() - state.get('last_auth_time', 0) > AUTH_TIMEOUT:
        user_states[user_id] = {'status': 'idle', 'try_count': 0}
        return False
    return True

def check_user_credentials(name, key):
    try:
        records = users_ws.get_all_records()
        for rec in records:
            if rec['name'] == name and rec['key'] == key:
                return True
        return False
    except Exception as e:
        print(f"Error accessing Google Sheets: {e}")
        return False

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
    state = get_state(user_id)

    # --- 認証関連処理 ---
    if text.lower() == "login":
        start_auth(user_id)
        reply_text = "ログインを開始します。名前とキーを「名前 キー」の形式で入力してください。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    if state and state["status"] == "awaiting_credentials":
        increment_attempts(user_id)
        try:
            name, key = text.split()
        except ValueError:
            reply_text = "形式が正しくありません。「名前 キー」の形式で入力してください。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            return

        user_key_map = get_user_key_map()
        if name in user_key_map and user_key_map[name] == key:
            update_last_auth(name)
            reset_auth(user_id)
            user_states[user_id] = {
                'status': 'logged_in',
                'try_count': 0,
                'last_auth_time': time.time(),
                'name': name,
                'key': key
            }
            reply_text = f"認証に成功しました。{name}さん、ようこそ！"
        else:
            if state["attempts"] >= 3:
                reset_auth(user_id)
                reply_text = "認証に3回失敗しました。最初からやり直してください。"
            else:
                reply_text = f"認証に失敗しました。残り{3 - state['attempts']}回まで試行できます。"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    # --- ログイン済みユーザーのみ、IDT/体重記録/ガイド対応 ---
    if is_logged_in(user_id):
        if "cal idt" in text.lower():
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
        elif text.lower().startswith("make "):
            try:
                _, name, weight = text.split()
                weight = float(weight)
                from idt_module import write_weight_record  # 仮モジュール名
                reply_text = write_weight_record(name, weight)
            except Exception:
                reply_text = "形式が正しくありません。\n例: make yoshiaki 60.5"
        else:
            try:
                from idt_module import calculate_idt  # 仮モジュール名
                reply_text = calculate_idt(text)
            except Exception:
                reply_text = "IDTの計算に失敗しました。形式を確認してください。"
    else:
        reply_text = "「login」と送信して認証を開始してください。"

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))

# --- 毎年5月1日の学年更新と卒業者削除 ---
def update_grades_and_cleanup():
    try:
        today = datetime.today()
        if today.month == 5 and today.day == 1:
            records = users_ws.get_all_records()
            headers = users_ws.row_values(1)
            updated_records = []
            for rec in records:
                try:
                    grade = int(rec.get('grade', 0))
                except ValueError:
                    continue
                if grade >= 4:
                    continue  # 卒業対象（削除）
                rec['grade'] = grade + 1
                updated_records.append(rec)

            users_ws.clear()
            users_ws.append_row(headers)
            for rec in updated_records:
                row = [rec.get(h, "") for h in headers]
                users_ws.append_row(row)

            print("🎓 学年更新と卒業生削除が完了しました")
        else:
            print("🗓 本日は5月1日ではありません。学年更新はスキップされました。")
    except Exception as e:
        print(f"⚠️ 学年更新処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    update_grades_and_cleanup()
    app.run(host="0.0.0.0", port=5000)
