import schedule
import time
import signal
import threading
import send_mesage
import write_log

# 定义一个全局变量，用于存储上一次任务的调度器
last_job = None


# 程序结束提示信息打印输出
def signal_handler(signum, frame):
    write_log.write_log("明厨亮灶发送数据程序关闭")
    exit(0)


def task_print(prt_str):
    write_log.write_log(prt_str)


# 定义一个运行线程的函数
def run_threaded(job_func, *args):
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()


# 以下是定义线程任务---------------------------------------------------------------------------------------
# 定义定时任务A的执行函数
def task_a(prt_str):
    write_log.write_log('定时任务A开始执行:' + prt_str)
    write_log.write_log('定时任务A执行完成')


# 定义定时线程任每天收货情况
# 将未上传的前100条数据状态置为2
def task_update2_mclz():
    exec_sql = """
    UPDATE  wy_dist_to_mclz
    set state ='2'
    where  state=0
    """
    key_name = 'update_mclz'
    send_mesage.task_update2_mclz(key_name, exec_sql)


# 定时任务删除上传成功记录（state=1）
def task_datele_mclz():
    exec_sql = """
    delete  from wy_dist_to_mclz
    where  state='1' 
    """
    key_name = 'delete_mclz'
    send_mesage.task_datele_mclz(key_name, exec_sql)


# 定义定时线程任将状态为2的订单发送
def task_send_mclz():
    send_mesage.send_message_to_mclz()


# 定义定时线程任  每天凌晨5：00将上传表状态为1（上传成功）的订单备份到wy_dist_to_mclz_log中
# def task_wy_dist_to_mclz():
#     send_mesage.task_wy_dist_to_mclz()


# schedule.every(5).minutes.do(run_threaded, task_print, '定义定时心跳，每5分钟执行一次')
schedule.every(5).minutes.do(run_threaded, task_update2_mclz)
schedule.every(20).minutes.do(run_threaded, task_send_mclz)

# 每天凌晨5：00删除wy_dist_to_mclz上传成功记录
schedule.every().day.at("05:00").do(run_threaded, task_datele_mclz)

if __name__ == '__main__':
    write_log.write_log(f'明厨亮灶发送数据开始程序执行')
    # 注册信号处理函数(有其他信号执行signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    # 立即执行一次任务
    task_send_mclz()
    while True:
        # 检查是否有定时任务需要执行
        schedule.run_pending()
        # 其他代码...
        time.sleep(1)
