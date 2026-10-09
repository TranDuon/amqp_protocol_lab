"""Bai 1 - Consumer: lang nghe queue iot_lab_queue va in message nhan duoc."""
import sys
from datetime import datetime
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config


def on_message(channel, method, properties, body):
    """Duoc goi moi khi broker giao mot message tu queue (chay tren main thread)."""
    print(f"Da nhan message: {body.decode('utf-8', errors='replace')}")
    print(f"Thoi gian nhan: {datetime.now():%H:%M:%S}\n")
    # Ack sau khi xu ly xong: neu consumer chet truoc dong nay, RabbitMQ se giao lai message.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection, channel = config.connect_or_exit()
    config.declare_queue(channel, config.QUEUE_BAI1)  # tao queue neu producer chua tao
    channel.basic_qos(prefetch_count=1)  # moi lan chi giu 1 message chua ack
    channel.basic_consume(queue=config.QUEUE_BAI1, on_message_callback=on_message)

    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")
    print(f"Dang lang nghe queue {config.QUEUE_BAI1} ... (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()  # chan main thread, goi on_message moi khi co message
    except KeyboardInterrupt:
        print("Da dung consumer.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
