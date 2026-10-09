"""Bai 1 - Producer: gui message chao mung vao queue iot_lab_queue."""
import argparse
import sys
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config


def main():
    parser = argparse.ArgumentParser(description="Bai 1 - AMQP producer")
    parser.add_argument("-n", "--count", type=int, default=1, help="so message can gui (mac dinh 1)")
    args = parser.parse_args()

    message = f"Xin chao tu ung dung Python AMQP - {config.STUDENT_ID} - {config.STUDENT_NAME}"

    connection, channel = config.connect_or_exit()
    channel.confirm_delivery()  # bat publisher confirm: moi lan publish cho broker xac nhan
    config.declare_queue(channel, config.QUEUE_BAI1)  # tao queue neu consumer chua tao
    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")

    try:
        for i in range(1, args.count + 1):
            # exchange="" la default exchange: routing_key = ten queue -> message vao thang queue do.
            config.publish(channel, exchange="", routing_key=config.QUEUE_BAI1, body=message)
            print(f"Da gui message [{i}/{args.count}] vao queue {config.QUEUE_BAI1}: {message}")
    except KeyboardInterrupt:
        print("Da dung producer.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
