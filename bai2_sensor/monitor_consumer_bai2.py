"""Bai 2 - Monitoring Consumer: nhan du lieu cam bien tu queue, in ra va canh bao khi vuot nguong."""
import json
import sys
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config

TEMP_HIGH = 35.0      # nhiet do > 35  -> canh bao
HUMIDITY_LOW = 40.0   # do am   < 40  -> canh bao


def check_alerts(temperature, humidity):
    """Tra ve danh sach canh bao; hai dieu kien kiem tra doc lap nen co the co ca hai."""
    alerts = []
    if temperature > TEMP_HIGH:
        alerts.append("CANH BAO: Nhiet do cao")
    if humidity < HUMIDITY_LOW:
        alerts.append("CANH BAO: Do am thap")
    return alerts


def on_message(channel, method, properties, body):
    # Bat loi o day: exception lot ra ngoai callback se lam start_consuming() dung va consumer bi tat.
    try:
        data = json.loads(body)  # chuoi JSON -> dict
        device_id = data["device_id"]
        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError):
        print(f"LOI: Payload khong hop le, bo qua: {body!r}\n")
        # requeue=False: bo message hong. Neu dua lai queue, consumer se nhan lai va loi mai mai.
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    print(f"Device: {device_id}")
    print(f"Temperature: {temperature}")
    print(f"Humidity: {humidity}")
    if "timestamp" in data:
        print(f"Thoi gian gui: {data['timestamp']}")
    for alert in check_alerts(temperature, humidity):
        print(alert)
    print()
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection, channel = config.connect_or_exit()
    config.declare_queue(channel, config.QUEUE_SENSOR)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=config.QUEUE_SENSOR, on_message_callback=on_message)

    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")
    print(f"Dang giam sat queue {config.QUEUE_SENSOR} ... (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Da dung monitor.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
