import requests

from ruamel import yaml


def login_auth():
    url = "http://www.XXX.com.cn/XXX/HTTP//auth"
    headers = {"Content-Type": "application/json"}
    data = {
        "LoginName": "123",
        "Password": "123"
    }
    try:
        response = requests.post(url=url, headers=headers, json=data)  # 向接口发送POST请求获取Token
        if response.status_code == 200:  # 如果返回状态码为200表示成功
            token = response.json()["token"]  # 从JSON响应中提取Token值
            # 把token值写入配置文件中
            yamlpath = r'C:\Users\Administrator\PycharmProjects\mclz\Token.yaml'
            tokenval = {
                "token": token
            }
            with open(yamlpath, "w", encoding="utf-8") as f:
                yaml.dump(tokenval, f, Dumper=yaml.RoundTripDumper)
        else:
            print("Failed to retrieve Token")  # 打印失败信息
            return None
    except Exception as e:
        print("An error occurred while retrieving the Token:", str(e))  # 打印错误信息
        return None


