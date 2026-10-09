"""Bai 3 - Alert Producer: gui canh bao vao direct exchange iot_alert_exchange, routing key = muc canh bao."""
import argparse
import sys
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config

SAMPLE_ALERTS = [
    ("info", "Nhiet do phong may on dinh info"),
    ("warning", "Nhiet do phong may vuot nguong warning"),
    ("critical", "Cam bien kho lanh mat ket noi critical"),
]


def main():
    parser = argparse.ArgumentParser(description="Bai 3 - Alert producer")
    parser.add_argument("level", nargs="?", choices=config.ALERT_LEVELS,
                        help="muc canh bao; bo trong de gui 3 canh bao mau (info, warning, critical)")
    parser.add_argument("message", nargs="?", help="noi dung canh bao (di kem level)")
    args = parser.parse_args()

    if args.level:
        alerts = [(args.level, args.message or f"Canh bao thu nghiem muc {args.level}")]
    else:
        alerts = SAMPLE_ALERTS

    connection, channel = config.connect_or_exit()
    channel.confirm_delivery()
    config.declare_alert_exchanges(channel)  # producer chi tao exchange; queue do consumer tao va bind
    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}\n")

    try:
        for level, text in alerts:
            # Direct exchange: chi queue nao bind dung routing key = level moi nhan duoc.
            routed = config.publish(channel, exchange=config.ALERT_EXCHANGE, routing_key=level, body=text)
            result = "da vao queue" if routed else "khong co queue nao nhan (khong co binding cho routing key nay)"
            print(f"[{level}] Da gui toi {config.ALERT_EXCHANGE}: {text}")
            print(f"        -> {result}")
            # Mo rong: gui them vao topic exchange voi routing key alert.<level> cho consumer nhan tat ca.
            config.publish(channel, exchange=config.ALERT_TOPIC_EXCHANGE, routing_key=f"alert.{level}",
                           body=text, mandatory=False)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
