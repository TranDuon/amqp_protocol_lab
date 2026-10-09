"""Bai 2 - Sensor Producer: mo phong cam bien gui nhiet do/do am dang JSON vao queue moi 3 giay."""
import argparse
import json
import random
import sys
from datetime import datetime
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config


def read_sensor(device_id):
    """Sinh mot ban ghi gia lap. Khoang gia tri vuot qua nguong de thay duoc canh bao."""
    return {
        "device_id": device_id,
        "temperature": round(random.uniform(25.0, 40.0), 1),
        "humidity": round(random.uniform(30.0, 80.0), 1),
        "timestamp": datetime.now().isoformat(timespec="seconds"),  # mo rong: thoi gian gui
    }


def main():
    parser = argparse.ArgumentParser(description="Bai 2 - Sensor producer")
    parser.add_argument("devices", nargs="*", default=["sensor01"],
                        help="danh sach device_id can mo phong (mac dinh: sensor01)")
    parser.add_argument("-i", "--interval", type=float, default=3.0, help="chu ky gui, giay (mac dinh 3)")
    args = parser.parse_args()

    connection, channel = config.connect_or_exit()
    channel.confirm_delivery()
    config.declare_queue(channel, config.QUEUE_SENSOR)
    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")
    print(f"Gui du lieu cua {', '.join(args.devices)} vao queue {config.QUEUE_SENSOR} "
          f"moi {args.interval:g} giay (Ctrl+C de dung)\n")

    try:
        while True:
            for device_id in args.devices:
                payload = json.dumps(read_sensor(device_id))  # dict -> chuoi JSON
                config.publish(channel, exchange="", routing_key=config.QUEUE_SENSOR, body=payload)
                print(f"[{datetime.now():%H:%M:%S}] {config.QUEUE_SENSOR} <- {payload}")
            # connection.sleep (khong dung time.sleep): vua cho vua xu ly heartbeat voi broker.
            connection.sleep(args.interval)
    except KeyboardInterrupt:
        print("Da dung sensor producer.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
