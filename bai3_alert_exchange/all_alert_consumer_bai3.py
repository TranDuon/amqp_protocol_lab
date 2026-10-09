"""Bai 3 (mo rong) - All Alert Consumer: nhan TAT CA canh bao qua topic exchange voi binding "alert.#"."""
import sys
from pathlib import Path

import pika.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # thu muc goc lab, de import common/
from common import config

QUEUE = "all_alerts_queue"
BINDING_KEY = "alert.#"  # topic: '#' khop 0 hoac nhieu tu -> alert.info, alert.warning, alert.critical


def on_message(channel, method, properties, body):
    text = body.decode("utf-8", errors="replace")
    print(f"[{QUEUE}] ({method.routing_key}) Da nhan: {text}")
    config.log_alert(QUEUE, method.routing_key, text)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection, channel = config.connect_or_exit()
    config.declare_alert_exchanges(channel)
    config.declare_queue(channel, QUEUE)
    channel.queue_bind(queue=QUEUE, exchange=config.ALERT_TOPIC_EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

    print(f"Da ket noi RabbitMQ {config.BROKER_HOST}:{config.BROKER_PORT}")
    print(f"{QUEUE} <- {config.ALERT_TOPIC_EXCHANGE} (binding '{BINDING_KEY}') ... (Ctrl+C de thoat)\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Da dung all alert consumer.")
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Mat ket noi RabbitMQ ({type(err).__name__}).")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
