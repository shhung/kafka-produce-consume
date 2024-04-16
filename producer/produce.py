from kafka import KafkaProducer
import numpy as np
import time
import os
import signal
import argparse

def signal_handler(sig, frame):
    global running
    print("stop producing!!")
    running = False

def main():
    global running

    # 设置 Ctrl-C 信号处理程序
    signal.signal(signal.SIGINT, signal_handler)

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Kafka Producer Configuration')
    parser.add_argument('--message_per_second', type=int, default=10, help='Number of messages per second')
    parser.add_argument('--burst', action='store_true', help='Enable burst mode')
    args = parser.parse_args()

    # 从环境变量中获取 Kafka 服务器和主题信息
    bootstrap = os.getenv('BOOTSTRAP_SERVER')
    topic = os.getenv('TOPIC')

    # 创建 Kafka 生产者
    producer = KafkaProducer(bootstrap_servers=[bootstrap])

    # 主循环
    running = True
    while running:
        if not args.burst:
            time.sleep(1 / args.message_per_second)
        data = np.random.rand(1, 20)
        producer.send(topic, data.tobytes())

if __name__ == "__main__":
    main()