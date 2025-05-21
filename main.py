import os
import json
import datetime
import pytz
import random
import re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

credentials_json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON")
if credentials_json_str is None:
    raise ValueError("GOOGLE_CREDENTIALS_JSON が設定されていません。")

credentials_info = json.loads(credentials_json_str)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)

spreadsheet_name = os.environ.get("SPREADSHEET_NAME", "user_database")
gc = gspread.authorize(creds)
spreadsheet = gc.open(spreadsheet_name)
worksheet = spreadsheet.sheet1

IDT_RECORD_URL = "https://docs.google.com/spreadsheets/d/11ZlpV2yl9aA3gxpS-JhBxgNniaxlDP1NO_4XmpGvg54/edit"
idt_record_sheet = gc.open_by_url(IDT_RECORD_URL).sheet1

ADMIN_RECORD_URL = os.environ.get("ADMIN_RECORD_URL")  # 追加: 管理者用記録用スプレッドシートURL（環境変数で管理）
if ADMIN_RECORD_URL:
    admin_record_sheet = gc.open_by_url(ADMIN_RECORD_URL).sheet1
else:
    admin_record_sheet = None

user_states = {}
otp_store = {}
idt_memory = {}

# 停止ユーザー情報
suspended_users = set()

# 日本時間で日付取得
def today_jst_ymd():
    jst = pytz.timezone('Asia/Tokyo')
    now = datetime.datetime.now(jst)
    return now.strftime("%Y/%m/%d")

def generate_otp():
    return str(random.randint(100000, 999999))

def now_str():
    return str(datetime.datetime.now())

def parse_idt_input(text):
    """
    入力例: 7:32.8 56.3 m
    タイム 体重 性別
    """
    match = re.match(
        r"^(\d{1,2}:[0-5]?\d(?:\.\d)?)\s+(\d{1,3}\.\d)\s+([mwMW])$",
        text.strip(), re.I)
    if not match:
        return None
    time_str, weight_str, gender_str = match.groups()
    return time_str, float(weight_str), gender_str.lower()

def parse_time_str(time_str):
    match = re.match(r"^(\d{1,2}):([0-5]?\d)(?:\.(\d))?$", time_str)
    if not match:
        return None
    min_str, sec_str, secd_str = match.groups()
    mi = int(min_str)
    se = int(sec_str)
    sed = int(secd_str) if secd_str else 0
    return mi, se, sed

def calc_idt(mi, se, sed, wei, gend):
    # gend: 0.0=男, 1.0=女
    ergo = mi * 60.0 + se + sed * 0.1
    idtm = ((101.0 - wei) * (20.9 / 23.0) + 333.07) / ergo * 100.0
    idtw = ((100.0 - wei) * (1.40) + 357.80) / ergo * 100.0
    score = idtm * (1.0 - gend) + idtw * gend
    return score

IDT_GUIDE = (
    "タイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n"
    "例: 7:32.8 56.3 m\n"
    "性別は 男性=m、女性=w です。\n"
    "空白やコロンの使い分けにご注意ください。\n"
    "モード終了の場合は「end」と入力してください。"
)

HELP_GUIDE = (
    "“login”でログインができます(記録の記入時に必須)\n"
    "“cal idt”でIDTの計算ができます(ログイン不要)\n"
    "“add idt”でIDTの記録を入力できます(ログイン必須)\n"
    "“admin login”で管理者としてログイン\n"
    "“admin add”で選手記録を管理者として追加\n"
    "“suspend <ユーザー名>”でアカウント一時停止（管理者専用）"
)

# 管理者判定用
def is_admin(user_id):
    """user_idが管理者か判定する"""
    users = worksheet.get_all_values()
    if not users:
        return False
    header = users[0]
    if "admin" not in header:
        return False
    user_id_col = header.index("user_id")
    admin_col = header.index("admin")
    for row in users[1:]:
        if len(row) > admin_col and row[user_id_col] == user_id and row[admin_col] == "1":
            return True
    return False

# 管理者を10人制限 (管理者追加コマンド用)
def admin_count():
    users = worksheet.get_all_values()
    if not users:
        return 0
    header = users[0]
    if "admin" not in header:
        return 0
    admin_col = header.index("admin")
    count = 0
    for row in users[1:]:
        if len(row) > admin_col and row[admin_col] == "1":
            count += 1
    return count

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

    # サスペンド中なら何もできない
    if user_id in suspended_users:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="このアカウントは一時停止中です。管理者にお問い合わせください。")
        )
        return

    # endコマンドでモード強制終了
    if text.lower() == "end" and user_id in user_states:
        user_states.pop(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="入力モードを終了しました。")
        )
        return

    # helpコマンド
    if text.lower() == "help":
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=HELP_GUIDE)
        )
        return

    # cal idtコマンド案内
    if text.lower() == "cal idt":
        user_states[user_id] = {'mode': 'idt'}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=IDT_GUIDE)
        )
        return

    # cal idt用: 入力受付 (mm:ss.s xx.x m/w)
    if user_id in user_states and user_states[user_id].get('mode') == 'idt':
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\n" + IDT_GUIDE)
            )
            return
        time_str, wei, gstr = result
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。7:32.8 のように入力してください。")
            )
            return
        if gstr not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m（男性） か w（女性）で入力してください。")
            )
            return
        gend = 0.0 if gstr == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        idt_memory[user_id] = {
            "mi": mi, "se": se, "sed": sed, "wei": wei, "gend": gend,
            "score": score_disp, "time_str": f"{mi}:{se:02d}.{sed if sed else 0}"
        }
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"あなたのIDTは{score_disp:.2f}%です。"
            )
        )
        user_states.pop(user_id)
        return

    # loginコマンド
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
        admin_col = header.index("admin") if "admin" in header else None

        found = False
        for i, row in enumerate(data, start=2):
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found = True
                registered_user_id = row[user_id_col]
                if registered_user_id == "":
                    worksheet.update_cell(i, user_id_col + 1, user_id)
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="認証成功！ユーザー情報を登録しました。")
                    )
                    user_states.pop(user_id)
                elif registered_user_id == user_id:
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ログイン成功！ようこそ。")
                    )
                    user_states.pop(user_id)
                else:
                    otp = generate_otp()
                    otp_store[registered_user_id] = {
                        "otp": otp,
                        "requester_id": user_id,
                        "name": name,
                        "timestamp": datetime.datetime.now()
                    }
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(
                            text="このアカウントはすでに別の端末からログインを済ましています。\nこの操作があなたのものであれば元の端末に対して確認コードを送信しているのでコードを確認しログインを完了してください。"
                        )
                    )
                    line_bot_api.push_message(
                        registered_user_id,
                        TextSendMessage(
                            text=f"{name}があなたのアカウントに対しログインを試みています。\nこの操作があなたのものであれば以下のコードをログインを試みている端末に入力し、ログインを完了してください。\nもしもあなたの操作でない場合はキーが漏れている可能性があるので直ちに変更してください。\n\n確認コード: {otp}"
                        )
                    )
                    user_states[user_id]["step"] = 2
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
        input_otp = text.strip()
        for owner_id, otp_info in otp_store.items():
            if otp_info["requester_id"] == user_id:
                if otp_info["otp"] == input_otp:
                    users = worksheet.get_all_values()
                    header = users[0]
                    data = users[1:]
                    name_col = header.index("name")
                    grade_col = header.index("grade")
                    key_col = header.index("key")
                    user_id_col = header.index("user_id")
                    last_auth_col = header.index("last_auth")
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

    # add idtコマンド
    if re.match(r"^add idt($|[\s　])", text, re.I):
        # ログインしているか判定(worksheetにuser_idが存在するか)
        users = worksheet.get_all_values()
        header = users[0]
        data = users[1:]
        user_id_col = header.index("user_id")
        is_logged_in = any(row[user_id_col] == user_id for row in data)
        if not is_logged_in:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="IDT記録の入力にはログインが必要です。“login”でログインしてください。")
            )
            return
        if user_id in idt_memory:
            user_states[user_id] = {"mode": "add_idt_memory"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="IDTの記録を追加します。タイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n例: 7:32.8 56.3 m")
            )
        else:
            user_states[user_id] = {"mode": "add_idt_direct"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="IDTの記録が直近のやり取りで行われていないようです。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n例: 7:32.8 56.3 m"
                )
            )
        return

    # add idt直後: 直近IDT計算あり
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_memory":
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n例: 7:32.8 56.3 m")
            )
            return
        time_str, wei, gstr = result
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。7:32.8 のように入力してください。")
            )
            return
        if gstr not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m（男性） か w（女性）で入力してください。")
            )
            return
        gend = 0.0 if gstr == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        record_time = today_jst_ymd()
        row = [record_time, time_str, wei, score_disp]
        try:
            idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=f"IDTの記録を{record_time}の日付で登録しました。\n今回のIDTは{score_disp:.2f}%でした。"
                )
            )
            user_states.pop(user_id)
            idt_memory.pop(user_id)
        except Exception as e:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"記録に失敗しました。{e}")
            )
        return

    # add idt直後: 直近IDT計算なし
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_direct":
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n例: 7:32.8 56.3 m")
            )
            return
        time_str, wei, gstr = result
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。7:32.8 のように入力してください。")
            )
            return
        if gstr not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m（男性） か w（女性）で入力してください。")
            )
            return
        gend = 0.0 if gstr == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        record_time = today_jst_ymd()
        row = [record_time, f"{mi}:{se:02d}.{sed if sed else 0}", wei, score_disp]
        try:
            idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=f"IDTの記録を{record_time}の日付で登録しました。\n今回のIDTは{score_disp:.2f}%でした。"
                )
            )
            user_states.pop(user_id)
        except Exception as e:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"記録に失敗しました。{e}")
            )
        return

    # ---------- 管理者専用機能 ----------

    # 管理者ログインコマンド
    if text.lower() == "admin login":
        user_states[user_id] = {'mode': 'admin_login'}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="管理者としてログインするには、選手として登録済みの名前・学年・キーを入力してください。\n例: 太郎 2 tarou123"
            )
        )
        return

    # 管理者ログインモードの処理
    if user_id in user_states and user_states[user_id].get('mode') == 'admin_login':
        parts = text.split(" ")
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\n名前 学年 キー の順でスペース区切りで入力してください。")
            )
            return

        name, grade, key = parts
        users = worksheet.get_all_values()
        if not users:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="利用者データが空です。")
            )
            user_states.pop(user_id)
            return

        header = users[0]
        data = users[1:]
        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        admin_col = header.index("admin") if "admin" in header else None

        # 既に10人管理者いたら追加不可
        if admin_col is not None and admin_count() >= 10:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="管理者の人数上限に達しています。")
            )
            user_states.pop(user_id)
            return

        found = False
        for i, row in enumerate(data, start=2):
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found = True
                # admin列追加
                if admin_col is not None:
                    worksheet.update_cell(i, admin_col + 1, "1")
                else:
                    worksheet.add_cols(1)
                    worksheet.update_cell(1, len(header)+1, "admin")
                    worksheet.update_cell(i, len(header)+1, "1")
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="管理者権限を付与しました。")
                )
                user_states.pop(user_id)
                return

        if not found:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="認証失敗。名前・学年・キーを確認してください。")
            )
            user_states.pop(user_id)
        return

    # 管理者による記録追加(admin add)
    if text.lower() == "admin add":
        if not is_admin(user_id):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="管理者権限がありません。")
            )
            return
        user_states[user_id] = {'mode': 'admin_add', 'step': 1}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="管理者記録追加モードです。選手の「名前 性別(m/w) 結果(タイム) 体重」を半角スペース区切りで入力してください。\n例: 太郎 m 7:32.8 56.3"
            )
        )
        return

    if user_id in user_states and user_states[user_id].get('mode') == 'admin_add':
        if admin_record_sheet is None:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="管理者記録用スプレッドシートが設定されていません。")
            )
            user_states.pop(user_id)
            return
        parts = text.split(" ")
        if len(parts) != 4:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\n名前 性別(m/w) タイム 体重 の順でスペース区切りで入力してください。")
            )
            return
        name, gender, time_str, weight = parts
        if gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m か w で入力してください。")
            )
            return
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。例: 7:32.8")
            )
            return
        try:
            weight = float(weight)
        except Exception:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="体重は数値で入力してください。")
            )
            return
        gend = 0.0 if gender.lower() == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        record_time = today_jst_ymd()
        # 既存の選手として登録されているユーザーの追加は不可
        users = worksheet.get_all_values()
        header = users[0]
        name_col = header.index("name")
        if any(row[name_col] == name for row in users[1:]):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="既に選手として追加済みのユーザー名です。管理者からの記録追加はできません。")
            )
            user_states.pop(user_id)
            return
        row = [record_time, name, gender, time_str, weight, score_disp]
        try:
            admin_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"管理者として{record_time}日付で記録を登録しました。\nIDT: {score_disp:.2f}%")
            )
            user_states.pop(user_id)
        except Exception as e:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"記録に失敗しました。{e}")
            )
        return

    # 管理者によるアカウント一時停止
    if text.lower().startswith("suspend "):
        if not is_admin(user_id):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="管理者権限がありません。")
            )
            return
        target_name = text[len("suspend "):].strip()
        users = worksheet.get_all_values()
        header = users[0]
        name_col = header.index("name")
        user_id_col = header.index("user_id")
        for row in users[1:]:
            if row[name_col] == target_name:
                target_user_id = row[user_id_col]
                if target_user_id:
                    suspended_users.add(target_user_id)
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"{target_name}のアカウントを一時停止しました。")
                    )
                else:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ユーザーIDが未登録のため一時停止できません。")
                    )
                return
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="該当するユーザーが見つかりません。")
        )
        return

    # 何も該当しなければ何も返さない
    return

if __name__ == "__main__":
    app.run()

if __name__ == "__main__":
    app.run()
