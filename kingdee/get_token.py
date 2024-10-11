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


def savetoken():
    getAppToken_url = "http://180.141.91.139:8023/ierp/api/getAppToken.do"
    getAppToken_payload = {
        "appId": "openapi_app",
        "appSecuret": "NNWNtzjt20240321!",
        "tenantid": "wnjtsit",
        "accountId": "1938351379391184896",
        "language": "zh_CN"
    }
    headers = {
        'Content-Type': 'application/json'
    }

    now = datetime.now()
    try:
        response_apptoken = (requests.post(getAppToken_url, json=getAppToken_payload, headers=headers)).json()
        if response_apptoken and 'data' in response_apptoken and 'app_token' in response_apptoken['data']:
            apptoken = response_apptoken['data']['app_token']
            getaccess_token_url = "http://180.141.91.139:8023/ierp/api/login.do"
            getaccess_token_payload = {
                "user": "43007523",
                "apptoken": apptoken,
                "tenantid": "wnjtsit",
                "accountId": "1938351379391184896",
                "usertype": "UserName"
            }
            try:
                response_access_token = (requests.post(getaccess_token_url, json=getaccess_token_payload, headers=headers)).json()
                if response_access_token and 'data' in response_access_token and 'access_token' in \
                        response_access_token['data']:
                    yamlpath = r'C:\Users\Administrator\PycharmProjects\kingdee\Token.yaml'
                    yaml = YAML(typ='unsafe', pure=True)
                    with open(yamlpath, "w", encoding="utf-8") as f:
                        yaml.dump(
                            {"token": response_access_token['data']['access_token'],
                             "TokenTimeOut": '60',
                             "TokenIssueTime": now}, f)
                else:
                    print("Failed to retrieve Token")
            except Exception as e:
                print("An error occurred:", str(e))
                traceback.print_exc()
        else:
            print("Failed to retrieve Token")
    except Exception as e:
        print("An error occurred:", str(e))
        traceback.print_exc()


def get_token():
    # login_auth()

    yaml_path = r'C:\Users\Administrator\PycharmProjects\kingdee\Token.yaml'

    try:
        with open(yaml_path, 'r', encoding='utf-8') as file:
            yaml = YAML(typ='safe', pure=True)
            token_data = yaml.load(file)
            return token_data
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
    savetoken()
    token = get_token()
    check_token_expiration(token)
    if token:
        print("Token:", token)
