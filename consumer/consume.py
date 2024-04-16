from kafka import KafkaConsumer
import numpy as np
import time
import os
import signal
import requests
import json
import argparse

running = True  # 将 running 变量定义为全局变量

def signal_handler(sig, frame):
    global running
    print("stop!!")
    running = False

def main():
    global running  # 声明使用全局变量

    # 设置 Ctrl-C 信号处理程序
    signal.signal(signal.SIGINT, signal_handler)

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Kafka Consumer Configuration')
    parser.add_argument('--max_records', type=int, default=1000, help='Maximum records to poll')
    parser.add_argument('--timeout', type=int, default=0, help='Timeout in ms for poll')
    args = parser.parse_args()

    # 从环境变量中获取 Kafka 服务器、消费者组ID、主题和服务端URL信息
    bootstrap = os.getenv('BOOTSTRAP_SERVER')
    group_id = os.getenv('GROUP_ID')
    topic = os.getenv('TOPIC')
    serving_server = os.getenv('SERVING_URL')

    # 创建 Kafka 消费者
    consumer = KafkaConsumer(topic,
                             group_id=group_id,
                             bootstrap_servers=[bootstrap])

    count = 0

    while running:
        msg = consumer.poll(args.timeout, max_records=args.max_records)
        if msg:
            batch_data = np.empty((0, 20))
            for part in list(msg.values()):
                for record in part:
                    data = np.frombuffer(record.value, dtype=np.float64).reshape((1, 20))
                    batch_data = np.vstack((batch_data, data))

            # 构建请求数据
            input_data = {"signature_name": 'serving_default',
                          'instances': batch_data.tolist()
                         }
            # 发送请求
            resp = requests.post(serving_server, json=input_data)
            pred = resp.json()['predictions']
            result = np.argmax(pred, axis=1)
            #print(len(result))

    consumer.close()

if __name__ == "__main__":
    main()