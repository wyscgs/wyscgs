import os
from datetime import datetime


# 写入日志函数，顺便打印出信息
# by ly 202311-5


def write_log(message):
    log_dir = "C:/wy_ApiServiceLog"

    # 如果log文件夹不存在，则创建文件夹
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # 获取当前日期
    date_str = datetime.now().strftime("%Y-%m-%d")

    # 创建文件名为yyyy-mm-dd.log的日志文件
    log_file = os.path.join(log_dir, f"{date_str}.log")
    # 打印信息
    print(f"[{datetime.now()}] {message}\n")
    # 写入日志信息
    with open(log_file, "a") as file:
        file.write(f"[{datetime.now()}] {message}\n")

# 调用write_log函数写入日志
# write_log("这是一条日志信息")