import dbconnection as dbcon
import write_log

import concurrent.futures


# 假设这是处理单个订单号的函数
def process_data(bill_number):
    # 在这里执行处理逻辑
    print(f"处理订单号: {bill_number}")
    # 例如，发送消息到MCLZ或执行其他操作


# 假设这是从数据库中获取订单号的函数
def select_distno():
    # 这里是模拟的订单号列表
    return [i for i in range(1, 241)]  # 假设有240个订单号


# 主函数，使用线程池来并发处理订单号
def send_message_to_mclz():
    write_log.write_log('发送定时任务开始执行...')

    # 获取订单号列表
    dist_billnumber = select_distno()

    # 创建一个线程池，最大工作线程数可以根据需要进行调整
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # 提交任务到线程池，每个任务处理一个订单号
        futures = [executor.submit(process_data, bill_number) for bill_number in dist_billnumber]

        # 等待所有任务完成
        concurrent.futures.wait(futures)

    write_log.write_log('本次发送定时任务结束')


# 如果这是脚本的入口点，调用send_message_to_mclz函数
if __name__ == "__main__":
    send_message_to_mclz()

