# auth_state.py

# 一時的な状態保存（ユーザーIDごとに）
auth_states = {}

def start_auth(user_id):
    auth_states[user_id] = {
        "status": "awaiting_credentials",  # 名前+キー待ち
        "attempts": 0
    }

def reset_auth(user_id):
    if user_id in auth_states:
        del auth_states[user_id]

def increment_attempts(user_id):
    if user_id in auth_states:
        auth_states[user_id]["attempts"] += 1

def get_state(user_id):
    return auth_states.get(user_id, None)
