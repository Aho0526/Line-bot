import os
import json
import datetime
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
idt_record_sheet = gc.open_by_url(IDT_RECORD_URL).sheet1  # 1枚目タブが対象

user_states = {}
otp_store = {}
idt_memory = {}  # user_id: 最新のIDT記録（辞書形式）

def generate_otp():
    return str(random.randint(100000, 999999))

def now_str():
    return str(datetime.datetime.now())

def today_ymd():
    now = datetime.datetime.now()
    return now.strftime("%Y/%m/%d")

def parse_idt_input(text):
    """
    入力例: 太郎 2 m 7:43.6 58.3
    名前 学年 性別 記録 体重
    """
    match = re.match(
        r"^([^\s　]+)\s+(\d+)\s+([mw])\s+(\d{1,2}:[0-5]?\d(?:\.\d)?)\s+(\d{1,3}\.\d)$",
        text.strip(), re.I)
    if not match:
        return None
    name, grade, gender_code, time_str, weight_str = match.groups()
    gend = 0.0 if gender_code.lower() == "m" else 1.0
    wei = float(weight_str)
    return name, grade, gender_code.lower(), time_str, wei, gend

def parse_time_str(time_str):
    # 7:32.8 → (7, 32, 8)
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

IDT_GUIDE = (
    "名前 学年 性別 記録 体重の順で入力してください。\n"
    "例: 太郎 2 m 7:43.6 58.3"
)

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

    # 直近IDT算出用: mm:ss.s xx.x m/w
    idt_calc_match = re.match(
        r"^(\d{1,2}):([0-5]?\d)(?:\.|:)?(\d)?\s+(\d{1,3}\.\d)\s+([mw])$",
        text, re.I)
    if idt_calc_match:
        mi = int(idt_calc_match.group(1))
        se = int(idt_calc_match.group(2))
        sed = int(idt_calc_match.group(3)) if idt_calc_match.group(3) else 0
        wei = float(idt_calc_match.group(4))
        gend = 0.0 if idt_calc_match.group(5).lower() == "m" else 1.0
        gender_code = idt_calc_match.group(5).lower()
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        idt_memory[user_id] = {
            "mi": mi,
            "se": se,
            "sed": sed,
            "wei": wei,
            "gend": gend,
            "gender_code": gender_code,
            "score": score_disp,
            "time_str": f"{mi}:{se:02d}.{sed if sed else 0}"
        }
        sex_str = "男性" if gend == 0.0 else "女性"
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"あなたのIDTは{score_disp:.2f}%です。"
            )
        )
        return

    # cal idtコマンド案内
    if text.lower() == "cal idt":
        user_states[user_id] = {'mode': 'idt'}
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="数値入力の際は以下の通りに入力してください。\nまた、性別はm/w(男性=m/女性=w)として入力してください。\n\nmm:ss.s xx.x m/w\n\n記入例:タイム7:32.8、体重56.3kg、男性の場合:7:32.8 56.3 m\n空白やコロンの使い分けにご注意ください")
        )
        return

    # cal idt実値受付
    if user_id in user_states and user_states[user_id].get('mode') == 'idt':
        calc_match = re.match(
            r"^(\d{1,2}):([0-5]?\d)(?:\.|:)?(\d)?\s+(\d{1,3}\.\d)\s+([mw])$",
            text, re.I)
        if not calc_match:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。案内文の通りに入力してください。\n\n" + "数値入力の際は以下の通りに入力してください。\nまた、性別はm/w(男性=m/女性=w)として入力してください。\n\nmm:ss.s xx.x m/w\n\n記入例:タイム7:32.8、体重56.3kg、男性の場合:7:32.8 56.3 m\n空白やコロンの使い分けにご注意ください")
            )
            return
        mi = int(calc_match.group(1))
        se = int(calc_match.group(2))
        sed = int(calc_match.group(3)) if calc_match.group(3) else 0
        wei = float(calc_match.group(4))
        gend = 0.0 if calc_match.group(5).lower() == "m" else 1.0
        gender_code = calc_match.group(5).lower()
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        idt_memory[user_id] = {
            "mi": mi,
            "se": se,
            "sed": sed,
            "wei": wei,
            "gend": gend,
            "gender_code": gender_code,
            "score": score_disp,
            "time_str": f"{mi}:{se:02d}.{sed if sed else 0}"
        }
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(
                text=f"あなたのIDTは{score_disp:.2f}%です。"
            )
        )
        user_states.pop(user_id)
        return

    # add idtコマンド
    if re.match(r"^add idt($|[\s　])", text, re.I):
        # 直近IDT記録がある場合は即記録モード
        if user_id in idt_memory:
            user_states[user_id] = {"mode": "add_idt_memory"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="IDTの記録を追加します。名前 学年 性別 タイム 体重の順で入力してください。\n例: 太郎 2 m 7:43.6 58.3")
            )
        else:
            user_states[user_id] = {"mode": "add_idt_direct"}
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text="IDTの記録が直近のやり取りで行われていないようです。\n" + IDT_GUIDE
                )
            )
        return

    # add idt直後: 直近IDT計算あり
    if user_id in user_states and user_states[user_id].get("mode") == "add_idt_memory":
        result = parse_idt_input(text)
        if not result:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="入力形式が正しくありません。\n" + IDT_GUIDE)
            )
            return
        name, grade, gender_code, time_str, wei, gend = result
        # タイムが一致しなければ再計算
        time_tuple = parse_time_str(time_str)
        if not time_tuple:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。7:43.6 のように入力してください。")
            )
            return
        mi, se, sed = time_tuple
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        row = [name, grade, gender_code, time_str, wei, score_disp]
        date_disp = today_ymd()
        try:
            idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=f"IDTの記録を{date_disp}の日付で登録しました。\n今回のIDTは{score_disp:.2f}%でした。"
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
                TextSendMessage(text="入力形式が正しくありません。\n" + IDT_GUIDE)
            )
            return
        name, grade, gender_code, time_str, wei, gend = result
        time_tuple = parse_time_str(time_str)
        if not time_tuple:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="タイム形式が正しくありません。7:43.6 のように入力してください。")
            )
            return
        mi, se, sed = time_tuple
        score = calc_idt(mi, se, sed, wei, gend)
        score_disp = round(score + 1e-8, 2)
        row = [name, grade, gender_code, time_str, wei, score_disp]
        date_disp = today_ymd()
        try:
            idt_record_sheet.append_row(row, value_input_option="USER_ENTERED")
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=f"IDTの記録を{date_disp}の日付で登録しました。\n今回のIDTは{score_disp:.2f}%でした。"
                )
            )
            user_states.pop(user_id)
        except Exception as e:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"記録に失敗しました。{e}")
            )
        return

    # ログインモードに入るためのコマンド
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
        # 名前 学年 キーの入力受付
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

        # 必要な列名が存在するかチェック
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

        found = False
        for i, row in enumerate(data, start=2):  # 2行目から
            if row[name_col] == name and row[grade_col] == grade and row[key_col] == key:
                found = True
                registered_user_id = row[user_id_col]
                if registered_user_id == "":
                    # 新規認証
                    worksheet.update_cell(i, user_id_col + 1, user_id)
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="認証成功！ユーザー情報を登録しました。")
                    )
                    user_states.pop(user_id)
                elif registered_user_id == user_id:
                    # すでに自分の端末で認証済み
                    worksheet.update_cell(i, last_auth_col + 1, now_str())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="ログイン成功！ようこそ。")
                    )
                    user_states.pop(user_id)
                else:
                    # 別端末からログイン要求
                    otp = generate_otp()
                    otp_store[registered_user_id] = {
                        "otp": otp,
                        "requester_id": user_id,
                        "name": name,
                        "timestamp": datetime.datetime.now()
                    }
                    # 要求元への案内
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(
                            text="このアカウントはすでに別の端末からログインを済ましています。\nこの操作があなたのものであれば元の端末に対して確認コードを送信しているのでコードを確認しログインを完了してください。"
                        )
                    )
                    # 元のアカウント所持者へOTP送信
                    line_bot_api.push_message(
                        registered_user_id,
                        TextSendMessage(
                            text=f"{name}があなたのアカウントに対しログインを試みています。\nこの操作があなたのものであれば以下のコードをログインを試みている端末に入力し、ログインを完了してください。\nもしもあなたの操作でない場合はキーが漏れている可能性があるので直ちに変更してください。\n\n確認コード: {otp}"
                        )
                    )
                    user_states[user_id]["step"] = 2  # OTP待ち状態に
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
        # OTP入力を期待
        input_otp = text.strip()
        # どのアカウントのOTPか判定
        for owner_id, otp_info in otp_store.items():
            if otp_info["requester_id"] == user_id:
                if otp_info["otp"] == input_otp:
                    # 認証成功→user_idをシートに登録
                    users = worksheet.get_all_values()
                    header = users[0]
                    data = users[1:]
                    name_col = header.index("name")
                    grade_col = header.index("grade")
                    key_col = header.index("key")
                    user_id_col = header.index("user_id")
                    last_auth_col = header.index("last_auth")
                    # 入力時のname, grade, keyを取得
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

    # 通常のメッセージ
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="「login」と送信するとログインモードになります。\n"
                             "IDTスコア計算は 例: 7:43.6 58.3 m\n"
                             "「cal idt」と送信するとIDTスコア計算ガイドが表示されます。\n"
                             "「add idt」と送信するとIDT記録ができます。")
    )

if __name__ == "__main__":
    app.run()
