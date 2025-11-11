import requests
import json
import os
from dotenv import load_dotenv

# .env 로드
load_dotenv(dotenv_path=".env")
KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")

# KIS 토큰 로드 및 저장
def get_access_token(KIS_APP_KEY, KIS_APP_SECRET):
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
    }
    res = requests.post(url, headers=headers, data=json.dumps(data))
    
    if res.status_code != 200:
        print(f"Failed to get token: {res.status_code}, {res.text}")
        return None

    token = res.json().get("access_token")
    
    if token:
        save_path = "./kis_access_token.dat"
        with open(save_path, "w") as f:
            print('Save the access token.')
            f.write(token)
        return token
    else:
        print("No access_token in response.")
        return None

# 실제 실행
if __name__ == "__main__":
    token = get_access_token(KIS_APP_KEY, KIS_APP_SECRET)
    print(f"Access token: {token}")