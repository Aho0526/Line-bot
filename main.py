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

IDT_RECORD_URL = os.environ.get("IDT_RECORD_URL", "https://docs.google.com/spreadsheets/d/11ZlpV2yl9aA3gxpS-JhBxgNniaxlDP1NO_4XmpGvg54/edit")
idt_record_sheet = gc.open_by_url(IDT_RECORD_URL).sheet1

ADMIN_RECORD_URL = os.environ.get("ADMIN_RECORD_URL")
if ADMIN_RECORD_URL:
    admin_record_sheet = gc.open_by_url(ADMIN_RECORD_URL).sheet1
else:
    admin_record_sheet = None

SUSPEND_SHEET_NAME = os.environ.get("SUSPEND_SHEET_NAME", "suspend_list")
try:
    suspend_sheet = gc.open(spreadsheet_name).worksheet(SUSPEND_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    suspend_sheet = gc.open(spreadsheet_name).add_worksheet(title=SUSPEND_SHEET_NAME, rows=100, cols=4)
    suspend_sheet.append_row(["user_id", "until", "reason"])

# 追加: admin request提出履歴を保存するシート名と管理関数
ADMIN_REQUEST_BAN_SHEET = "admin_request_ban"
try:
    admin_request_ban_sheet = gc.open(spreadsheet_name).worksheet(ADMIN_REQUEST_BAN_SHEET)
except gspread.exceptions.WorksheetNotFound:
    admin_request_ban_sheet = gc.open(spreadsheet_name).add_worksheet(title=ADMIN_REQUEST_BAN_SHEET, rows=100, cols=3)
    admin_request_ban_sheet.append_row(["user_id", "until", "last_request_date"])

def get_admin_request_ban(user_id):
    rows = admin_request_ban_sheet.get_all_values()
    if len(rows) < 2:
        return None
    header = rows[0]
    user_id_col = header.index("user_id")
    until_col = header.index("until")
    for row in rows[1:]:
        if row[user_id_col] == user_id:
            try:
                until_date = datetime.datetime.strptime(row[until_col], "%Y/%m/%d").replace(tzinfo=pytz.timezone('Asia/Tokyo'))
                return until_date
            except Exception:
                return None
    return None

def set_admin_request_ban(user_id, days=14):
    until = (jst_now() + datetime.timedelta(days=days)).strftime("%Y/%m/%d")
    now_ymd = today_jst_ymd()
    rows = admin_request_ban_sheet.get_all_values()
    header = rows[0]
    user_id_col = header.index("user_id")
    for i, row in enumerate(rows[1:], start=2):
        if row[user_id_col] == user_id:
            admin_request_ban_sheet.update_cell(i, header.index("until")+1, until)
            admin_request_ban_sheet.update_cell(i, header.index("last_request_date")+1, now_ymd)
            return
    admin_request_ban_sheet.append_row([user_id, until, now_ymd])

user_states = {}
otp_store = {}
idt_memory = {}
admin_request_store = {}

def today_jst_ymd():
    jst = pytz.timezone('Asia/Tokyo')
    now = datetime.datetime.now(jst)
    return now.strftime("%Y/%m/%d")

def jst_now():
    return datetime.datetime.now(pytz.timezone('Asia/Tokyo'))

def generate_otp():
    return str(random.randint(100000, 999999))

def now_str():
    return str(datetime.datetime.now())

def parse_idt_input(text):
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
    ergo = mi * 60.0 + se + sed * 0.1
    idtm = ((101.0 - wei) * (20.9 / 23.0) + 333.07) / ergo * 100.0
    idtw = ((100.0 - wei) * (1.40) + 357.80) / ergo * 100.0
    score = idtm * (1.0 - gend) + idtw * gend
    return score

def get_user_name_grade(user_id):
    users = worksheet.get_all_values()
    if not users:
        return None, None
    header = users[0]
    user_id_col = header.index("user_id")
    name_col = header.index("name")
    grade_col = header.index("grade")
    for row in users[1:]:
        if row[user_id_col] == user_id:
            return row[name_col], row[grade_col]
    return None, None

def ensure_header():
    header = worksheet.row_values(1)
    required = ["name", "grade", "key", "user_id", "last_auth", "admin"]
    for col in required:
        if col not in header:
            worksheet.update_cell(1, len(header) + 1, col)
            header.append(col)
    return worksheet.row_values(1)

def get_admin_number_to_userid():
    users = worksheet.get_all_values()
    header = users[0]
    user_id_col = header.index("user_id")
    admin_col = header.index("admin")
    number_to_userid = {}
    for row in users[1:]:
        if len(row) > admin_col and row[admin_col].isdigit():
            number_to_userid[int(row[admin_col])] = row[user_id_col]
    return number_to_userid

def get_next_admin_number():
    users = worksheet.get_all_values()
    header = users[0]
    admin_col = header.index("admin")
    nums = {int(row[admin_col]) for row in users[1:] if row[admin_col].isdigit()}
    n = 1
    while n in nums:
        n += 1
    return n

def get_user_row_by_name(name):
    users = worksheet.get_all_values()
    header = users[0]
    name_col = header.index("name")
    for i, row in enumerate(users[1:], start=2):
        if row[name_col] == name:
            return i, row
    return None, None

def is_admin(user_id):
    users = worksheet.get_all_values()
    header = users[0]
    user_id_col = header.index("user_id")
    admin_col = header.index("admin")
    for row in users[1:]:
        if row[user_id_col] == user_id and row[admin_col].isdigit():
            return True
    return False

def is_head_admin(user_id):
    users = worksheet.get_all_values()
    header = users[0]
    user_id_col = header.index("user_id")
    admin_col = header.index("admin")
    for row in users[1:]:
        if row[user_id_col] == user_id and row[admin_col] == "1":
            return True
    return False

def get_help_message(user_id):
    if not is_admin(user_id):
        return (
            "“login”でログインができます(記録の記入時に必須)\n"
            "“cal idt”でIDTの計算ができます(ログイン不要)\n"
            "“add idt”でIDTの記録を入力できます(ログイン必須)\n"
            "“admin request”で管理者申請\n"
        )
    else:
        return (
            "あなたは管理者（マネージャー）アカウントです。\n"
            "選手記録追加や管理用コマンドのみ利用できます。\n"
            "“admin add”で選手記録を管理者として追加\n"
            "“admin approve <名前>”で管理者昇格承認（1番管理者のみ）\n"
            "“stop responding to <ユーザ名> for <時間> time because you did <理由>”で一時停止（1番管理者のみ）"
        )

def check_suspend(user_id):
    rows = suspend_sheet.get_all_values()
    if not rows or len(rows) < 2:
        return False, None, None, None
    header = rows[0]
    if "user_id" not in header or "until" not in header or "reason" not in header:
        return False, None, None, None
    user_id_col = header.index("user_id")
    until_col = header.index("until")
    reason_col = header.index("reason")
    now = jst_now()
    for i, row in enumerate(rows[1:], start=2):
        if row[user_id_col] == user_id:
            try:
                until_time = datetime.datetime.strptime(row[until_col], "%Y/%m/%d %H:%M").replace(tzinfo=pytz.timezone('Asia/Tokyo'))
            except Exception:
                continue
            if now < until_time:
                return (True, (until_time - now), row[reason_col], i)
            else:
                # 解除（行削除）
                suspend_sheet.delete_rows(i)
                return (False, None, None, None)
    return (False, None, None, None)

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

    # スプレッドシートによる応答停止チェック
    is_sus, delta, reason, _ = check_suspend(user_id)
    if is_sus:
        mins = int(delta.total_seconds() // 60)
        hours = delta.total_seconds() / 3600
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"あなたは「{reason}」をしたので、あと{hours:.1f}時間（{mins}分）の間Botからの応答が制限されます。")
        )
        return

    # 応答停止コマンド（1番管理者のみが実行可能）
    match = re.match(r"stop responding to (.+?) for ([\d.]+) time because you did (.+)", text, re.I)
    if match and is_head_admin(user_id):
        target_name = match.group(1).strip()
        duration = float(match.group(2))
        reason = match.group(3).strip()
        users = worksheet.get_all_values()
        header = users[0]
        name_col = header.index("name")
        user_id_col = header.index("user_id")
        found = False
        for row in users[1:]:
            if row[name_col] == target_name:
                target_user_id = row[user_id_col]
                if target_user_id:
                    until = jst_now() + datetime.timedelta(hours=duration)
                    # 既存停止があれば上書きする
                    rows = suspend_sheet.get_all_values()
                    suspended = False
                    for i, srow in enumerate(rows[1:], start=2):
                        if srow[0] == target_user_id:
                            suspend_sheet.update_cell(i, 2, until.strftime("%Y/%m/%d %H:%M"))
                            suspend_sheet.update_cell(i, 3, reason)
                            suspended = True
                            break
                    if not suspended:
                        suspend_sheet.append_row([target_user_id, until.strftime("%Y/%m/%d %H:%M"), reason])
                    # Push通知は無し。次にしゃべった時のみReplyで知らせる。
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"{target_name}を{duration:.1f}時間の間一時停止しました。理由: {reason}")
                    )
                    found = True
                    break
        if not found:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="該当するユーザーが見つかりません。")
            )
        return

    if text.lower() == "end" and user_id in user_states:
        user_states.pop(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="入力モードを終了しました。")
        )
        return

    if text.lower() == "help":
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=get_help_message(user_id))
        )
        return

    # 管理者はIDT関連コマンドを利用不可
    if is_admin(user_id):
        if text.lower() in ["cal idt", "add idt"] or (
            user_id in user_states and user_states[user_id].get('mode', '').startswith('idt')
        ):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="あなたは管理者アカウントのため、IDT記録機能はご利用できません。")
            )
            return

    if text.lower() == "cal idt":
        user_states[user_id] = {'mode': 'idt'}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="タイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。\n"
                     "例: 7:32.8 56.3 m\n"
                     "性別は 男性=m、女性=w です。\n"
                     "空白やコロンの使い分けにご注意ください。\n"
                     "モード終了の場合は「end」と入力してください。"
            )
        )
        return

    if user_id in user_states and user_states[user_id].get('mode') == 'idt':
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。")
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

    if text.lower() == "login":
        user_states[user_id] = {'mode': 'login', 'step': 1, 'login_data': {}}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text="ログインするには、名前、学年、キーの順で入力してください。\n例: 太郎 2 tarou123"
            )
        )
        return

    if user_id in user_states and user_states[user_id].get('mode') == 'login':
        parts = text.split(" ")
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\n名前 学年 キー の順でスペース区切りで入力してください。\n例: 太郎 2 tarou123")
            )
            return

        name, grade, key = parts
        header = ensure_header()
        users = worksheet.get_all_values()
        data = users[1:] if len(users) > 1 else []
        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        last_auth_col = header.index("last_auth")
        admin_col = header.index("admin")

        found = False
        for i, row in enumerate(data, start=2):
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found = True
                registered_user_id = row[user_id_col] if len(row) > user_id_col else ""
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
                            text="このアカウントはすでに別の端末からログインを済ましています。\nこの操作があなたのものであれば元の端末に対して下記の確認コードを入力してください。\n確認コードを入力するまでログインは完了しません。"
                        )
                    )
                    line_bot_api.push_message(
                        registered_user_id,
                        TextSendMessage(
                            text=f"{name}があなたのアカウントに対しログインを試みています。\nこの操作があなたのものであれば以下のコードをログイン画面に入力してください。\n確認コード: {otp}"
                        )
                    )
                    user_states[user_id]["step"] = 2
                return

        if not found:
            try:
                new_row = [""] * len(header)
                new_row[name_col] = name
                new_row[grade_col] = grade
                new_row[key_col] = key
                new_row[user_id_col] = user_id
                new_row[last_auth_col] = now_str()
                new_row[admin_col] = ""
                worksheet.append_row(new_row, value_input_option="USER_ENTERED")
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="初回登録が完了しました。ログイン成功です。")
                )
                user_states.pop(user_id)
            except Exception as e:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"登録に失敗しました: {e}")
                )
            return

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
        if is_admin(user_id):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="あなたは管理者アカウントのため、IDT記録機能はご利用できません。")
            )
            return
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
                TextSendMessage(text="IDTの記録を追加します。タイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。")
            )
        else:
            user_states[user_id] = {"mode": "add_idt_direct"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="IDTの記録が直近のやり取りで行われていないようです。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。"
                )
            )
        return

    # add idt: 直近IDT計算あり
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_memory":
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。")
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
        name, grade = get_user_name_grade(user_id)
        if not name or not grade:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ユーザー情報の取得に失敗しました。再度ログインしてください。")
            )
            return
        gender_str = "m" if gend == 0.0 else "w"
        # [name, grade, gender, record_time, time, weight, idt]
        row = [name, grade, gender_str, record_time, time_str, wei, score_disp]
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

    # add idt: 直近IDT計算なし
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_direct":
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\nタイム・体重・性別を半角スペース区切りで「mm:ss.s xx.x m/w」の形式で入力してください。")
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
        name, grade = get_user_name_grade(user_id)
        if not name or not grade:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ユーザー情報の取得に失敗しました。再度ログインしてください。")
            )
            return
        gender_str = "m" if gend == 0.0 else "w"
        row = [name, grade, gender_str, record_time, f"{mi}:{se:02d}.{sed if sed else 0}", wei, score_disp]
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

    # ---------- 管理者申請・承認制度 ----------
    if text.lower() == "admin request":
        # まず再提出禁止期間のチェック
        ban_until = get_admin_request_ban(user_id)
        if ban_until:
            now = jst_now()
            if now < ban_until:
                rest_days = (ban_until - now).days + 1
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"あなたは以前admin requestを提出した際に認められなかったので残り{rest_days}日間は再度リクエストを提出することができません。")
                )
                return
        # 通常フロー：1段階目
        user_states[user_id] = {"mode": "admin_request", "step": 1}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="確認のため、現在登録しているユーザー情報（名前、学年、キー）を送ってください。")
        )
        return

    if user_id in user_states and user_states[user_id].get("mode") == "admin_request":
        step = user_states[user_id].get("step", 1)
        if step == 1:
            parts = text.split(" ")
            if len(parts) != 3:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="形式が正しくありません。名前 学年 キー の順でスペース区切りで入力してください。")
                )
                return
            name, grade, key = parts
            user_states[user_id].update({"step": 2, "name": name, "grade": grade, "key": key})
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="最終確認：選手でAdminアカウントを持つことは認められていません。\n本当にリクエストを送信しますか？（はい／いいえ）"
                )
            )
            return
        elif step == 2:
            if text not in ["はい", "はい。", "yes", "Yes", "YES"]:
                # いいえの場合は終了
                user_states.pop(user_id)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="admin requestをキャンセルしました。")
                )
                return
            # 「はい」と答えた場合のみPush
            name = user_states[user_id].get("name")
            grade = user_states[user_id].get("grade")
            key = user_states[user_id].get("key")
            # ユーザー登録チェック
            header = ensure_header()
            users = worksheet.get_all_values()
            data = users[1:] if len(users) > 1 else []
            name_col = header.index("name")
            grade_col = header.index("grade")
            key_col = header.index("key")
            user_id_col = header.index("user_id")
            admin_col = header.index("admin")
            found = False
            for i, row in enumerate(data, start=2):
                if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                    found = True
                    break
            if not found:
                user_states.pop(user_id)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="申請失敗。あなたはユーザーとして登録されていません。")
                )
                return
            # Push通知はここだけ
            admin_request_store[user_id] = {"name": name, "grade": grade, "key": key}
            number_to_userid = get_admin_number_to_userid()
            if 1 in number_to_userid:
                head_admin_id = number_to_userid[1]
                line_bot_api.push_message(
                    head_admin_id,
                    TextSendMessage(
                        text=f"{name}（学年:{grade}）が管理者申請しています。\n承認する場合は「admin approve {name}」と送信してください。"
                    )
                )
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="管理者申請を1番管理者へ送信しました。承認されるまでお待ちください。")
            )
            user_states.pop(user_id)
            return

    if text.lower().startswith("admin approve "):
        if not is_head_admin(user_id):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="この操作は1番管理者のみ可能です。")
            )
            return
        target_name = text[len("admin approve "):].strip()
        for request_user_id, req in list(admin_request_store.items()):
            if req["name"] == target_name:
                users = worksheet.get_all_values()
                header = users[0]
                data = users[1:]
                name_col = header.index("name")
                admin_col = header.index("admin")
                for i, row in enumerate(data, start=2):
                    if row[name_col] == target_name:
                        next_num = get_next_admin_number()
                        worksheet.update_cell(i, admin_col + 1, str(next_num))
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=f"{target_name}を管理者({next_num})に承認しました。")
                        )
                        # 承認時のみPush、却下時は何もしない
                        line_bot_api.push_message(
                            request_user_id,
                            TextSendMessage(text="あなたの管理者申請が承認されました。以降、IDT記録など選手向け機能はご利用いただけません。")
                        )
                        admin_request_store.pop(request_user_id)
                        return
                # 承認されなかった場合はban記録
                set_admin_request_ban(request_user_id, days=14)
                admin_request_store.pop(request_user_id)
                return
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="該当する申請が見つかりません。")
        )
        return

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

    return

if __name__ == "__main__":
    app.run()
