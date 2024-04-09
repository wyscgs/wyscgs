import requests
from ruamel.yaml import YAML
import traceback
from datetime import datetime, timedelta


def make_api_request(url, headers, payload):
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error making API request:", str(e))
        traceback.print_exc()
        return None


def login_auth():
    url = "http://36.137.36.34:9015/v1/Login/UserLogin"
    payload = {
        "LoginName": "南宁威耀集采集配供应链管理有限公司",
        "Password": "Dnw*6919"
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': ''
    }
    now = datetime.now()
    try:
        response_json = make_api_request(url, headers, payload)
        if response_json and 'Data' in response_json and 'Token' in response_json['Data']:
            yamlpath = r'C:\Users\Administrator\PycharmProjects\mclz\Token.yaml'
            yaml = YAML(typ='unsafe', pure=True)
            with open(yamlpath, "w", encoding="utf-8") as f:
                yaml.dump(
                    {"token": response_json['Data']['Token'], "TokenTimeOut": response_json['Data']['TokenTimeOut'],
                     "TokenIssueTime": now}, f)
        else:
            print("Failed to retrieve Token")
    except Exception as e:
        print("An error occurred:", str(e))
        traceback.print_exc()


def get_token():
    # login_auth()

    yaml_path = r'C:\Users\Administrator\PycharmProjects\mclz\Token.yaml'

    try:
        with open(yaml_path, 'r', encoding='utf-8') as file:
            yaml = YAML(typ='safe', pure=True)
            token_data = yaml.load(file)

            if 'token' in token_data:
                return token_data
            else:
                print("Error: 'token' key not found in the YAML file.")
                return None
    except FileNotFoundError:
        print(f"Error: File not found at {yaml_path}")
        return None


def check_token_expiration(token_data):
    if token_data and 'TokenTimeOut' in token_data:
        expiration_time_minutes = int(token_data['TokenTimeOut'])
        expiration_time_delta = timedelta(minutes=expiration_time_minutes)

        # 检查'TokenIssueTime'是字符串还是日期时间对象
        if isinstance(token_data['TokenIssueTime'], str):
            token_issue_time = datetime.strptime(token_data['TokenIssueTime'], '%Y-%m-%dT%H:%M:%S')
        elif isinstance(token_data['TokenIssueTime'], datetime):
            token_issue_time = token_data['TokenIssueTime']
        else:
            print("错误：无效的令牌时间格式。")
            return False

        expiration_time = token_issue_time + expiration_time_delta
        current_time = datetime.now()

        if current_time < expiration_time:
            print("令牌仍然有效。")
            return True
        else:
            print("令牌已过期。")
            return False
    else:
        print("错误：无效的令牌数据。")
        return False


if __name__ == "__main__":
    token = get_token()
    check_token_expiration(token)
    if token:
        print("Token:", token)
