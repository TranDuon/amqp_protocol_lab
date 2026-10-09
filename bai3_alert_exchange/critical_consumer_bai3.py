"""Bai 3 - Critical Consumer: tao critical_queue, bind voi iot_alert_exchange bang routing key "critical"."""
import sys
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config

QUEUE = "critical_queue"
BINDING_KEY = "critical"


def on_message(channel, method, properties, body):
    text = body.decode("utf-8", errors="replace")
    print(f"[{QUEUE}] Da nhan: {text}")
    config.log_alert(QUEUE, method.routing_key, text)  # mo rong: ghi log ra file
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection, channel = config.connect_or_exit()
    config.declare_alert_exchanges(channel)
    config.declare_queue(channel, QUEUE)
    # Binding: exchange chi chuyen vao queue nay cac message co routing key dung bang "critical".
    channel.queue_bind(queue=QUEUE, exchange=config.ALERT_EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")
    print(f"{QUEUE} <- {config.ALERT_EXCHANGE} (routing key '{BINDING_KEY}') ... (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Da dung critical consumer.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
