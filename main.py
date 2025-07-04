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
import requests
from bs4 import BeautifulSoup
import datetime
import traceback
import io
from PyPDF2 import PdfReader
import tempfile
from linebot.models import FlexSendMessage

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

# ユーザデータ用スプレッドシートURLで明示的に指定
USER_DATABASE_URL = "https://docs.google.com/spreadsheets/d/1wZR1Tdupldp0RVOm00QAbE9-muz47unt_WhxagdirFA/"
user_db_spreadsheet = gspread.authorize(creds).open_by_url(USER_DATABASE_URL)
worksheet = user_db_spreadsheet.worksheet("users")


IDT_RECORD_URL = os.environ.get("IDT_RECORD_URL", "https://docs.google.com/spreadsheets/d/11ZlpV2yl9aA3gxpS-JhBxgNniaxlDP1NO_4XmpGvg54/edit")
idt_record_sheet = gspread.authorize(creds).open_by_url(IDT_RECORD_URL).worksheet("database")

ADMIN_RECORD_URL = os.environ.get("ADMIN_RECORD_URL")
if ADMIN_RECORD_URL:
    admin_record_sheet = gspread.authorize(creds).open_by_url(ADMIN_RECORD_URL).worksheet("database")
else:
    admin_record_sheet = None

SUSPEND_SHEET_NAME = os.environ.get("SUSPEND_SHEET_NAME", "suspend_list")
try:
    suspend_sheet = user_db_spreadsheet.worksheet(SUSPEND_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    suspend_sheet = user_db_spreadsheet.add_worksheet(title=SUSPEND_SHEET_NAME, rows=100, cols=4)
    suspend_sheet.append_row(["user_id", "until", "reason"])

ADMIN_REQUEST_BAN_SHEET = "admin_request_ban"
try:
    admin_request_ban_sheet = user_db_spreadsheet.worksheet(ADMIN_REQUEST_BAN_SHEET)
except gspread.exceptions.WorksheetNotFound:
    admin_request_ban_sheet = user_db_spreadsheet.add_worksheet(title=ADMIN_REQUEST_BAN_SHEET, rows=100, cols=3)
    admin_request_ban_sheet.append_row(["user_id", "until", "last_request_date"])

def download_tide_pdf(year: int) -> str | None:
    """
    Downloads the hourly tide data PDF for Kochi for a given year.
    Returns the filepath to the temporary PDF file, or None on failure.
    KC.pdf is the code for Kochi.
    """
    url = f"https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/pdf_hourly/{year}/KC.pdf"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    temp_pdf_file = None
    try:
        res = requests.get(url, headers=headers, stream=True)
        if res.status_code == 200:
            temp_pdf_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            for chunk in res.iter_content(chunk_size=8192):
                temp_pdf_file.write(chunk)
            temp_pdf_file.close()
            return temp_pdf_file.name
        else:
            print(f"Error downloading PDF: Status {res.status_code} for URL {url}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"RequestException while downloading PDF from {url}: {e}")
        if temp_pdf_file: 
            temp_pdf_file.close()
            os.remove(temp_pdf_file.name)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while downloading or writing PDF from {url}: {e}")
        if temp_pdf_file: 
            temp_pdf_file.close()
            try:
                os.remove(temp_pdf_file.name)
            except OSError:
                pass
        return None

### 変更点 ###
# 潮位PDFから指定日時の潮位を抽出するロジックを実装
def extract_tide_from_pdf(pdf_filepath: str, target_month: int, target_day: int, target_hour: int) -> int | None:
    """
    Extracts the tide level for a specific date and hour from a JMA PDF file.
    """
    try:
        reader = PdfReader(pdf_filepath)
        
        # 月はPDFのページ番号に対応 (1月 -> 0ページ目)
        if not (0 <= target_month - 1 < len(reader.pages)):
            print(f"Error: Invalid month {target_month} for PDF with {len(reader.pages)} pages.")
            return None
            
        page = reader.pages[target_month - 1]
        text = page.extract_text()
        
        lines = text.split('\n')
        
        # データ行を走査して目的の日の潮位を探す
        for line in lines:
            # 行頭がスペースと数字で始まっている行を対象とする (例: " 1 ", "10 ")
            line_strip = line.strip()
            if not line_strip or not line_strip[0].isdigit():
                continue

            # 行をスペースで分割
            parts = re.split(r'\s+', line_strip)
            
            # 最初の部分が日付のはず
            try:
                day = int(parts[0])
            except (ValueError, IndexError):
                continue

            if day == target_day:
                # 該当日の行を見つけた
                # parts[0]は日付なので、潮位データはparts[1:]にある
                # 0時のデータは parts[1] に対応
                tide_values = parts[1:]
                
                if 0 <= target_hour < len(tide_values):
                    tide_value_str = tide_values[target_hour]
                    if tide_value_str.isdigit():
                        return int(tide_value_str)
                
                # もし見つかればその時点で終了
                return None
                
        return None # 最後まで見つからなかった場合

    except Exception as e:
        error_details = traceback.format_exc()
        print(f"Error during PDF processing with PyPDF2 for {pdf_filepath}: {e}\n{error_details}")
        return None


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
    return datetime.datetime.now(pytz.timezone('Asia/Tokyo')).strftime("%Y/%m/%d %H:%M:%S")

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

### 変更点 ###
# ヘルパー関数がシートデータ(all_users_data)を引数で受け取るように修正
def get_user_row(user_id, all_users_data):
    if not all_users_data or len(all_users_data) < 2:
        return None, None, None
    header = [h.strip() for h in all_users_data[0]]
    user_id_col = header.index("user_id")
    for row in all_users_data[1:]:
        if len(row) > user_id_col and row[user_id_col] == user_id:
            return header, row, all_users_data.index(row)
    return header, None, -1

def get_user_name_grade(user_id, all_users_data):
    header, user_row, _ = get_user_row(user_id, all_users_data)
    if user_row:
        name_col = header.index("name")
        grade_col = header.index("grade")
        return user_row[name_col], user_row[grade_col]
    return None, None

def get_last_auth(user_id, all_users_data):
    header, user_row, _ = get_user_row(user_id, all_users_data)
    if user_row:
        last_auth_col = header.index("last_auth")
        if len(user_row) > last_auth_col:
            value = user_row[last_auth_col]
            return value if value != "" else None
    return None

def set_last_auth(user_id, dt=None):
    users = worksheet.get_all_values()
    header, user_row, row_index = get_user_row(user_id, users)
    if user_row:
        last_auth_col = header.index("last_auth")
        # gspreadの行インデックスは1から始まるので+1する
        worksheet.update_cell(row_index + 1, last_auth_col + 1, dt if dt else now_str())

def ensure_header():
    header = worksheet.row_values(1)
    required = ["name", "grade", "key", "user_id", "last_auth", "admin", "gender"]
    for col in required:
        if col not in header:
            worksheet.update_cell(1, len(header) + 1, col)
            header.append(col)
    return worksheet.row_values(1)

def get_admin_number_to_userid(all_users_data):
    if not all_users_data or len(all_users_data) < 2:
        return {}
    header = all_users_data[0]
    user_id_col = header.index("user_id")
    admin_col = header.index("admin")
    number_to_userid = {}
    for row in all_users_data[1:]:
        if len(row) > admin_col and row[admin_col].isdigit():
            number_to_userid[int(row[admin_col])] = row[user_id_col]
    return number_to_userid

def get_next_admin_number(all_users_data):
    if not all_users_data or len(all_users_data) < 2:
        return 1
    header = all_users_data[0]
    admin_col = header.index("admin")
    nums = {int(row[admin_col]) for row in all_users_data[1:] if len(row) > admin_col and row[admin_col].isdigit()}
    n = 1
    while n in nums:
        n += 1
    return n

def is_admin(user_id, all_users_data):
    header, user_row, _ = get_user_row(user_id, all_users_data)
    if user_row:
        admin_col = header.index("admin")
        if len(user_row) > admin_col and user_row[admin_col].isdigit():
            return True
    return False

def is_head_admin(user_id, all_users_data):
    header, user_row, _ = get_user_row(user_id, all_users_data)
    if user_row:
        admin_col = header.index("admin")
        if len(user_row) > admin_col and user_row[admin_col] == "1":
            return True
    return False

def get_help_message(user_id, all_users_data):
    if is_head_admin(user_id, all_users_data):
        return (
            "あなたは1番管理者です。\n"
            "“add idt”で任意の選手のIDT記録を管理者として追加できます。\n"
            "入力形式: 名前 学年 タイム 性別(m/w) 体重\n"
            "例: 太郎 2 7:32.8 m 58.6\n"
            "“admin approve <名前>”で管理者昇格承認（1番管理者のみ）\n"
            "“stop responding to <ユーザ名> for <時間> time because you did <理由>”で一時停止（1番管理者のみ）"
        )
    elif is_admin(user_id, all_users_data):
        return (
            "あなたは管理者（マネージャー）アカウントです。\n"
            "“cal idt”でIDTの計算ができます\n"
            "“add idt”で任意の選手のIDT記録を管理者として追加できます。\n"
            "入力形式: 名前 学年 タイム 性別(m/w) 体重\n"
            "例: 太郎 2 7:32.8 m 56.4\n"
        )
    else:
        return (
            "“login”でログインができます(記録の記入時に必須)\n"
            "“logout”でログアウトができます\n"
            "“cal idt”でIDTの計算ができます(ログイン不要)\n"
            "“add idt”で自分のIDT記録を入力できます(ログイン必須)。例: 7:32.8 53.6\n"
            "“admin request”で管理者申請\n"
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

    ### 変更点 ###
    # handle_messageの冒頭で一度だけシートから全データを取得
    all_users_data = worksheet.get_all_values()
    header, user_row, user_row_index = get_user_row(user_id, all_users_data)


    # 1. アカウント停止中チェック
    is_sus, delta, reason, _ = check_suspend(user_id)
    if is_sus:
        mins = int(delta.total_seconds() // 60)
        hours = delta.total_seconds() / 3600
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"あなたは「{reason}」をしたので、あと{hours:.1f}時間（{mins}分）の間Botからの応答が制限されます。"
            )
        )
        return
        
# cal idtコマンド
    if text.lower() == "cal idt":
        # ログイン状態をキャッシュしたデータから判定
        last_auth = get_last_auth(user_id, all_users_data)
        
        if user_row and last_auth != "LOGGED_OUT":
            user_states[user_id] = {"mode": "cal_idt_login"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="IDT計算モードです。タイム・体重を半角スペース区切りで入力してください。\n例: 7:32.8 56.3\n終了する場合は end と入力してください。"
                )
            )
        else:
            user_states[user_id] = {"mode": "cal_idt_guest"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="IDT計算モードです。タイム・体重・性別を半角スペース区切りで入力してください。\n例: 7:32.8 56.3 m\n終了する場合は end と入力してください。"
                )
            )
        return

    # cal idt 入力モード（ログイン済み）
    if user_id in user_states and user_states[user_id].get("mode") == "cal_idt_login":
        if text.strip().lower() == "end":
            user_states.pop(user_id)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="IDT計算モードを終了しました。")
            )
            return
        parts = text.strip().split()
        if len(parts) != 2:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\nタイム 体重 の順でスペース区切りで入力してください。\n例: 7:32.8 56.3\n終了する場合は end と入力してください。")
            )
            return
        time_str, weight = parts
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
        
        gender = None
        if user_row and "gender" in header:
            gender_col = header.index("gender")
            gender = user_row[gender_col]

        if gender is None or gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ユーザー情報の性別が正しく登録されていません。管理者に連絡してください。")
            )
            return
        gend = 0.0 if gender.lower() == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"IDT計算結果: {score_disp:.2f}%"
            )
        )
        return

    # cal idt 入力モード（未ログイン）
    if user_id in user_states and user_states[user_id].get("mode") == "cal_idt_guest":
        if text.strip().lower() == "end":
            user_states.pop(user_id)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="IDT計算モードを終了しました。")
            )
            return
        parts = text.strip().split()
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。\nタイム 体重 性別(m/w) の順でスペース区切りで入力してください。\n例: 7:32.8 56.3 m\n終了する場合は end と入力してください。")
            )
            return
        time_str, weight, gender = parts
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
        if gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m か w で入力してください。")
            )
            return
        gend = 0.0 if gender.lower() == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"IDT計算結果: {score_disp:.2f}%"
            )
        )
        return

# helpコマンド
    if text.lower() == "help":
        msg = get_help_message(user_id, all_users_data)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=msg)
        )
        return         

# readme / r コマンド
    if text.lower() in ["readme", "r"]:
        flex_msg = FlexSendMessage(
            alt_text="Botの使い方はこちら",
            contents={
                "type": "bubble",
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                        {
                            "type": "text",
                            "text": "📘 Botの使い方",
                            "weight": "bold",
                            "size": "lg"
                        },
                        {
                            "type": "text",
                            "text": "以下のリンクから詳細なREADMEが見られます。(外部サイトに遷移します。)",
                            "size": "sm",
                            "wrap": True
                        }
                    ]
                },
                "footer": {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "sm",
                    "contents": [
                        {
                            "type": "button",
                            "style": "primary",
                            "action": {
                                "type": "uri",
                                "label": "READMEを見る",
                                "uri": "https://direct-preview-68679e75e78885be252c2c24.monaca.education"
                            }
                        }
                    ]
                }
            }
        )
        line_bot_api.reply_message(
            event.reply_token,
            messages=[flex_msg]
        )
        return

    # tideコマンド
    if text.lower() == "tide":
        user_states[user_id] = {"mode": "awaiting_tide_datetime"}
        reply_text = "潮位を調べる日付と時刻を「月/日 時:分」（例: 6/8 16:00）の形式で教えてください。"
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    elif user_states.get(user_id, {}).get("mode") == "awaiting_tide_datetime":
        text_input = text.strip()
        match = re.fullmatch(r"(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})", text_input)

        if not match:
            reply_text = "日付と時刻の形式が正しくありません。「月/日 時:分」（例: 6/8 16:00）の形式で入力してください。"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            return # Keep state for re-entry

        month_str, day_str, hour_str, minute_str = match.groups()

        try:
            month = int(month_str)
            day = int(day_str)
            hour = int(hour_str)

            if not (1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23):
                raise ValueError("日付または時刻の範囲が無効です。")

        except ValueError:
            reply_text = "日付または時刻の範囲が正しくありません。実在する日時を入力してください。（例: 6/8 16:00）"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            user_states.pop(user_id, None) # Clear state on invalid date logic
            return

        current_year = datetime.datetime.now().year
        
        pdf_filepath = None
        try:
            pdf_filepath = download_tide_pdf(current_year)
            if pdf_filepath:
                tide_value = extract_tide_from_pdf(pdf_filepath, month, day, hour)
                if tide_value is not None:
                    reply_text = f"高知港の{current_year}年{month}月{day}日 {hour}時の潮位は、約 {tide_value} cmです。"
                else:
                    reply_text = f"{current_year}年{month}月{day}日 {hour}時の潮位データは見つかりませんでした。日付が正しいか確認してください。"
            else:
                reply_text = f"潮位情報PDF（{current_year}年分）のダウンロードに失敗しました。時間をおいて再試行してください。"
        
        except Exception as e:
            print(f"ERROR: Unhandled error in tide processing: {e}\n{traceback.format_exc()}")
            reply_text = "潮位の取得中に予期せぬエラーが発生しました。管理者に連絡してください。"
        
        finally:
            if pdf_filepath and os.path.exists(pdf_filepath):
                os.remove(pdf_filepath)

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        user_states.pop(user_id, None) # Clear state after attempt
        return

    # 2. logout 処理
    if text.lower() == "logout":
        try:
            set_last_auth(user_id, "LOGGED_OUT")
            if user_id in user_states:
                user_states.pop(user_id)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ログアウトしました。再度利用するにはloginしてください。")
            )
        except Exception as e:
            traceback.print_exc()
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ログアウト処理中にエラーが発生しました。管理者に連絡してください。")
            )
        return


     # login処理
    if text.lower() == "login":
        if not all_users_data or len(all_users_data) < 2:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ユーザーデータベースが空です。管理者に連絡してください。")
            )
            return

        if user_row:
            user_name = user_row[header.index("name")]
            last_auth = get_last_auth(user_id, all_users_data)
            
            # シートの内容でログイン状態を判定
            if last_auth != "LOGGED_OUT":
                user_states[user_id] = {'mode': 'login_confirm', 'name': user_name}
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f'「{user_name}」としてログインしますか？（はい／いいえ）')
                )
            else:
                user_states[user_id] = {'mode': 'login_confirm', 'name': user_name}
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f'「{user_name}」としてログインしますか？（はい／いいえ）')
                )
        else:
            # サインアップ未登録
            user_states[user_id] = {'mode': 'signup'}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="初回登録です。学年 名前 性別(m/w) キー をスペース区切りで入力してください。\n例: 2 太郎 m tarou123"
                )
            )
        return

    if user_id in user_states and user_states[user_id].get('mode') == 'signup':
        parts = text.strip().split()
        if len(parts) != 4:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。学年 名前 性別(m/w) キー の順でスペース区切りで入力してください。\n例: 2 太郎 m tarou123")
            )
            return
        grade, name, gender, key = parts
        if not grade.isdigit():
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="学年は半角数字で入力してください。")
            )
            return
        if gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="性別は m か w で入力してください。")
            )
            return
        
        name_col = header.index("name")
        grade_col = header.index("grade")

        # 重複チェック (キャッシュされたデータを使用)
        for row in all_users_data[1:]:
            if row[name_col] == name and row[grade_col] == grade:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="既に同じ名前と学年のユーザーが登録されています。管理者に相談してください。")
                )
                return

        # カラム順に合わせて辞書からリストを生成
        row_dict = {
            "name": name,
            "grade": grade,
            "gender": gender,
            "key": key,
            "user_id": user_id,
            "last_auth": now_str(),
            "admin": ""
        }
        # headerをensure_headerで最新化
        current_header = ensure_header()
        new_row = [row_dict.get(col, "") for col in current_header]
        try:
            worksheet.append_row(new_row, value_input_option="USER_ENTERED")
        except Exception as e:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"スプレッドシートへの書き込みに失敗しました: {e}")
            )
            return
        
        user_states.pop(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"登録が完了しました。「{name}」としてログインしました。")
        )
        return
    
    # login_confirmフロー
    if user_id in user_states and user_states[user_id].get('mode') == 'login_confirm':
        if text.lower() in ["はい", "はい。", "yes", "yes.", "y"]:
            if not user_row:
                user_states.pop(user_id)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="ユーザー情報が見つかりません。再度“login”からやり直してください。")
                )
                return
            set_last_auth(user_id, now_str())
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"「{user_states[user_id]['name']}」としてログインしました。")
            )
            user_states.pop(user_id)
            return
        elif text.lower() in ["いいえ", "no", "n"]:
            user_states[user_id] = {'mode': 'login_switch'}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ログインしたいアカウントの 学年 名前 キー をスペース区切りで入力してください。\n例: 2 太郎 tarou123")
            )
            return
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="「はい」または「いいえ」で答えてください。")
            )
            return

    # login_switchフロー
    if user_id in user_states and user_states[user_id].get('mode') == 'login_switch':
        parts = text.strip().split()
        if len(parts) != 3:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="形式が正しくありません。学年 名前 キー の順でスペース区切りで入力してください。\n例: 2 太郎 tarou123")
            )
            return
        grade, name, key = parts
        
        name_col = header.index("name")
        grade_col = header.index("grade")
        key_col = header.index("key")
        user_id_col = header.index("user_id")
        found_target_row = None
        target_row_gspread_index = -1
        
        for i, row in enumerate(all_users_data[1:], start=2):
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found_target_row = row
                target_row_gspread_index = i
                break

        if found_target_row:
            target_user_id = found_target_row[user_id_col]
            if target_user_id == user_id:
                set_last_auth(user_id, now_str())
                user_states.pop(user_id)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"「{name}」としてログインしました。")
                )
                return
              
            user_states[user_id] = {
                'mode': 'login_switch_confirm',
                'target_row': target_row_gspread_index,
                'target_user_id': target_user_id,
                'name': name,
                'grade': grade,
                'key': key,
                'otp_start': datetime.datetime.now()
            }
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=(
                        "このアカウントは既に別の端末と紐づいています。\n"
                        "元の端末が手元にない場合は管理者に連絡できます。\n"
                        "どちらかを選んでください。\n"
                        "「コードを送信」→元の端末に確認コードを送信\n"
                        "「管理者に連絡」→1番管理者に連絡\n"
                        "「いいえ」→どちらも行わない"
                    )
                )
            )
            return
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="該当するユーザーが見つかりません。情報を確認してください。")
            )
            return

    # login_switch_confirmフロー
    if user_id in user_states and user_states[user_id].get('mode') == 'login_switch_confirm':
        choice = text.strip()
        state = user_states[user_id]
        if choice == "コードを送信":
            otp = generate_otp()
            otp_store[state['target_user_id']] = {
                "otp": otp, "requester_id": user_id, "name": state['name'],
                "timestamp": datetime.datetime.now(), "try_count": 0,
                "expire": datetime.datetime.now() + datetime.timedelta(minutes=10)
            }
            line_bot_api.push_message(
                state['target_user_id'],
                TextSendMessage(
                    text=f"{state['name']}があなたのアカウントに対しログインを試みています。\nこの操作があなたのものであれば以下のコードをログイン画面に入力してください。\n確認コード: {otp}\n（有効期限10分）"
                )
            )
            user_states[user_id]['mode'] = 'login_switch_otp'
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="確認コードを紐づいている端末に送信しました。元の端末でコードを確認して入力してください。"
                )
            )
            return
        elif choice == "管理者に連絡":
            number_to_userid = get_admin_number_to_userid(all_users_data)
            if 1 in number_to_userid:
                head_admin_id = number_to_userid[1]
                line_bot_api.push_message(
                    head_admin_id,
                    TextSendMessage(
                        text=f"{state['name']}（学年:{state['grade']}）がアカウント切り替えを希望しています。\n手元に元端末がないため管理者対応が必要です。"
                    )
                )
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="1番管理者に連絡しました。対応をお待ちください。")
                )
            else:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="1番管理者が見つかりません。管理者に直接連絡してください。")
                )
            user_states.pop(user_id)
            return
        elif choice == "いいえ":
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="ログイン切り替えをキャンセルしました。")
            )
            user_states.pop(user_id)
            return
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="「コードを送信」「管理者に連絡」「いいえ」のいずれかで答えてください。")
            )
            return

    # login_switch_otpフロー
    if user_id in user_states and user_states[user_id].get('mode') == 'login_switch_otp':
        input_otp = text.strip()
        state = user_states[user_id]
        otp_info = otp_store.get(state['target_user_id'])
        now = datetime.datetime.now()
        
        if not otp_info or now > otp_info["expire"]:
            if otp_info: otp_store.pop(state['target_user_id'])
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="このコードは10分経過したため無効になりました。最初からやり直してください。"))
            return
        
        if input_otp == otp_info["otp"]:
            if (now - state['otp_start']).total_seconds() > 1800:
                otp_store.pop(state['target_user_id'])
                user_states.pop(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="操作開始から30分経過したため、やり直してください。"))
                return
            
            user_states[user_id]['mode'] = 'login_switch_final_confirm'
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="この操作を行うと元のアカウント（旧端末側）は消失します。\n本当に切り替えてよいですか？（ok/キャンセル）"))
            return
        else:
            otp_info["try_count"] += 1
            if otp_info["try_count"] >= 2:
                until = (jst_now() + datetime.timedelta(hours=1)).strftime("%Y/%m/%d %H:%M")
                suspend_sheet.append_row([user_id, until, "OTP2回ミス"])
                
                number_to_userid = get_admin_number_to_userid(all_users_data)
                if 1 in number_to_userid:
                    head_admin_id = number_to_userid[1]
                    line_bot_api.push_message(head_admin_id, TextSendMessage(text=f"警告: user_id={user_id} が {state['target_user_id']} のアカウントに対して2回OTPミスでログインを試みました。1時間停止処置済み。"))
                
                otp_store.pop(state['target_user_id'])
                user_states.pop(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="確認コードを2回間違えたため、1時間操作を停止します。"))
                return
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="確認コードが正しくありません。もう一度入力してください。"))
                return

    # login_switch_final_confirmフロー
    if user_id in user_states and user_states[user_id].get('mode') == 'login_switch_final_confirm':
        if text.strip().lower() == "ok":
            state = user_states[user_id]
            if (datetime.datetime.now() - state['otp_start']).total_seconds() > 1800:
                user_states.pop(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="操作開始から30分経過したため、やり直してください。"))
                return

            # Note: worksheet.delete_rows() can be slow and might fail.
            # A safer approach is to clear the row and mark as deleted. For now, we stick to the original logic.
            # 元のuser_idを持つ行を見つけて削除
            users_data_for_delete = worksheet.get_all_values() # 最新のデータを取得
            user_id_col = users_data_for_delete[0].index("user_id")
            for i, row in enumerate(users_data_for_delete[1:], start=2):
                if row[user_id_col] == state['target_user_id']:
                    worksheet.delete_rows(i)
                    break
            
            worksheet.update_cell(state['target_row'], user_id_col + 1, user_id)
            set_last_auth(user_id, now_str())
            otp_store.pop(state['target_user_id'], None)
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="アカウントの切り替えが完了しました。"))
            return
        else:
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="アカウント切り替えをキャンセルしました。"))
            return

    # アカウント削除
    if text.lower() == "delete account":
        user_states[user_id] = {"mode": "delete_account_confirm"}
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="本当にアカウントを削除しますか？（はい／いいえ）\n削除すると全てのデータが失われます。"))
        return

    if user_id in user_states and user_states[user_id].get("mode") == "delete_account_confirm":
        if text.strip().lower() in ["はい", "yes", "はい。", "yes."]:
            deleted = False
            if user_row_index != -1: # user_row_index is from get_user_row, 1-based index
                worksheet.delete_rows(user_row_index + 1)
                deleted = True
            
            user_states.pop(user_id)
            if deleted:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="アカウントを削除しました。ご利用ありがとうございました。"))
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="アカウントが見つかりませんでした。"))
        elif text.strip().lower() in ["いいえ", "no", "いいえ。", "no."]:
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="アカウント削除をキャンセルしました。"))
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="「はい」または「いいえ」で答えてください。"))
        return

# add idtコマンド
    if re.match(r"^add idt($|[\s])", text, re.I):
        if not user_row:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="IDT記録の入力にはログインが必要です。“login”でログインしてください。"))
            return
        
        last_auth = get_last_auth(user_id, all_users_data)
        if last_auth == "LOGGED_OUT":
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="現在ログインしていないので記録することができません。“login”でログインしてください。"))
            return
        
        if is_admin(user_id, all_users_data):
            user_states[user_id] = {"mode": "add_idt_admin"}
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="管理者記録追加モードです。対象の選手「名前 学年 タイム 性別(m/w) 体重」を半角スペース区切りで入力してください。\n例: 太郎 2 7:32.8 m 56.3"))
        else:
            user_states[user_id] = {"mode": "add_idt_user"}
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="IDT記録追加モードです。タイム・体重を半角スペース区切りで入力してください。\n例: 7:32.8 56.3"))
        return

# 管理者によるIDT記録追加
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_admin":
        if text.strip().lower() == "end":
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="IDT記録追加モードを終了しました。"))
            return
        parts = text.split(" ")
        if len(parts) != 5:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="形式が正しくありません。\n名前 学年 タイム 性別 体重 の順でスペース区切りで入力してください。\n例: 太郎 2 7:32.8 m 56.3\n終了する場合は end と入力してください。"))
            return
        name, grade, time_str, gender, weight = parts
        if gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="性別は m か w で入力してください。"))
            return
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="タイム形式が正しくありません。例: 7:32.8"))
            return
        try:
            weight = float(weight)
        except Exception:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="体重は数値で入力してください。"))
            return
        mi, se, sed = t
        gend = 0.0 if gender.lower() == "m" else 1.0
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        record_date = today_jst_ymd()
        row = [name, grade, gender, record_date, time_str, weight, score_disp, "1"]
        try:
            idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"{name}（学年:{grade}）のIDT記録を追加しました。IDT: {score_disp:.2f}%"))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"記録に失敗しました: {e}"))
        user_states.pop(user_id)
        return

    # 一般ユーザによる記録追加
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_user":
        if text.strip().lower() == "end":
            user_states.pop(user_id)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="IDT記録追加モードを終了しました。"))
            return
        parts = text.split(" ")
        if len(parts) != 2:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="形式が正しくありません。\nタイム 体重 の順でスペース区切りで入力してください。\n例: 7:32.8 56.3\n終了する場合は end と入力してください。"))
            return
        time_str, weight = parts
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="タイム形式が正しくありません。例: 7:32.8"))
            return
        try:
            weight = float(weight)
        except Exception:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="体重は数値で入力してください。"))
            return
        mi, se, sed = t
        name = user_row[header.index("name")]
        grade = user_row[header.index("grade")]
        gender = user_row[header.index("gender")]
        
        gend = 0.0 if gender.lower() == "m" else 1.0
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        record_date = today_jst_ymd()
        row = [name, grade, gender, record_date, time_str, weight, score_disp, ""]
        idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"あなたのIDT記録を{record_date}に追加しました。IDT: {score_disp:.2f}%"))
        user_states.pop(user_id)
        return

    # ---------- 管理者申請・承認制度 ----------
    if text.lower() == "admin request":
        ban_until = get_admin_request_ban(user_id)
        if ban_until:
            now = jst_now()
            if now < ban_until:
                rest_days = (ban_until - now).days + 1
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"あなたは以前admin requestを提出した際に認められなかったので残り{rest_days}日間は再度リクエストを提出することができません。"))
                return
        user_states[user_id] = {"mode": "admin_request", "step": 1}
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="確認のため、現在登録しているユーザー情報（名前、学年、キー）を送ってください。"))
        return

    if user_id in user_states and user_states[user_id].get("mode") == "admin_request":
        step = user_states[user_id].get("step", 1)
        if step == 1:
            parts = text.split(" ")
            if len(parts) != 3:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="形式が正しくありません。名前 学年 キー の順でスペース区切りで入力してください。"))
                return
            name, grade, key = parts
            user_states[user_id].update({"step": 2, "name": name, "grade": grade, "key": key})
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="最終確認：選手でAdminアカウントを持つことは認められていません。\n本当にリクエストを送信しますか？（はい／いいえ）"))
            return
        elif step == 2:
            if text not in ["はい", "はい。", "yes", "Yes", "YES"]:
                user_states.pop(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="admin requestをキャンセルしました。"))
                return
            name = user_states[user_id].get("name")
            grade = user_states[user_id].get("grade")
            key = user_states[user_id].get("key")
            
            # Use cached data for check
            name_col = header.index("name")
            grade_col = header.index("grade")
            key_col = header.index("key")
            
            found = False
            for row in all_users_data[1:]:
                if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                    found = True
                    break
            
            if not found:
                user_states.pop(user_id)
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="申請失敗。あなたはユーザーとして登録されていません。"))
                return
            
            admin_request_store[user_id] = {"name": name, "grade": grade, "key": key}
            number_to_userid = get_admin_number_to_userid(all_users_data)
            if 1 in number_to_userid:
                head_admin_id = number_to_userid[1]
                line_bot_api.push_message(head_admin_id, TextSendMessage(text=f"{name}（学年:{grade}）が管理者申請しています。\n承認する場合は「admin approve {name}」と送信してください。"))
            
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="管理者申請を1番管理者へ送信しました。承認されるまでお待ちください。"))
            user_states.pop(user_id)
            return

    if text.lower().startswith("admin approve "):
        if not is_head_admin(user_id, all_users_data):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="この操作は1番管理者のみ可能です。"))
            return
        
        target_name = text[len("admin approve "):].strip()
        for request_user_id, req in list(admin_request_store.items()):
            if req["name"] == target_name:
                # To apply changes, we need to fetch fresh data for this specific operation
                current_users_data = worksheet.get_all_values()
                current_header = current_users_data[0]
                name_col = current_header.index("name")
                admin_col = current_header.index("admin")
                
                for i, row in enumerate(current_users_data[1:], start=2):
                    if row[name_col] == target_name:
                        next_num = get_next_admin_number(current_users_data)
                        worksheet.update_cell(i, admin_col + 1, str(next_num))
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"{target_name}を管理者({next_num})に承認しました。"))
                        line_bot_api.push_message(request_user_id, TextSendMessage(text=("あなたの管理者申請が承認されました。以降、個人のIDT記録など選手向け機能はご利用いただけません。\n")))
                        admin_request_store.pop(request_user_id)
                        
                        # Set ban for other requests from the same user if needed
                        set_admin_request_ban(request_user_id, days=14)
                        return # Exit after successful approval
        
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="該当する申請が見つかりません。"))
        return

    if text.lower() == "admin add":
        if not is_admin(user_id, all_users_data):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="管理者権限がありません。"))
            return
        
        user_states[user_id] = {'mode': 'admin_add', 'step': 1}
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="管理者記録追加モードです。選手の「名前 性別(m/w) 結果(タイム) 体重」を半角スペース区切りで入力してください。\n例: 太郎 m 7:32.8 56.3"))
        return

    if user_id in user_states and user_states[user_id].get('mode') == 'admin_add':
        if admin_record_sheet is None:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="管理者記録用スプレッドシートが設定されていません。"))
            user_states.pop(user_id)
            return
        
        parts = text.split(" ")
        if len(parts) != 4:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="形式が正しくありません。\n名前 性別(m/w) タイム 体重 の順でスペース区切りで入力してください。"))
            return
        
        name, gender, time_str, weight = parts
        if gender.lower() not in ("m", "w"):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="性別は m か w で入力してください。"))
            return
        
        t = parse_time_str(time_str)
        if not t:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="タイム形式が正しくありません。例: 7:32.8"))
            return
        
        try:
            weight = float(weight)
        except Exception:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="体重は数値で入力してください。"))
            return
        
        gend = 0.0 if gender.lower() == "m" else 1.0
        mi, se, sed = t
        score = calc_idt(mi, se, sed, weight, gend)
        score_disp = round(score + 1e-8, 2)
        record_date = today_jst_ymd()
        
        name_col = header.index("name")
        if any(row[name_col] == name for row in all_users_data[1:]):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="既に選手として追加済みのユーザー名です。管理者からの記録追加はできません。"))
            user_states.pop(user_id)
            return
        
        row = [record_date, name, gender, time_str, weight, score_disp]
        try:
            admin_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"管理者として{record_date}に記録を登録しました。\nIDT: {score_disp:.2f}%"))
            user_states.pop(user_id)
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"記録に失敗しました。{e}"))
        return

    return

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
