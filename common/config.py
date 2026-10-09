"""Cau hinh va ham tien ich AMQP (RabbitMQ + pika) dung chung cho ca 3 bai."""
import os
import sys
from datetime import datetime
from pathlib import Path

import pika
import pika.exceptions

STUDENT_NAME = "Tran Dang Duong"
STUDENT_ID = "B23DCCN227"

BROKER_HOST = os.environ.get("AMQP_HOST", "127.0.0.1")
BROKER_PORT = int(os.environ.get("AMQP_PORT", "5672"))
BROKER_USER = os.environ.get("AMQP_USER", "guest")
BROKER_PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")

QUEUE_BAI1 = "iot_lab_queue"
QUEUE_SENSOR = "sensor_data_queue"

ALERT_EXCHANGE = "iot_alert_exchange"              # direct exchange (de bai)
ALERT_TOPIC_EXCHANGE = "iot_alert_topic_exchange"  # topic exchange (mo rong: nhan tat ca canh bao)
ALERT_LEVELS = ("info", "warning", "critical")
ALERT_LOG_FILE = Path(__file__).resolve().parent.parent / "bai3_alert_exchange" / "alerts.log"

# delivery_mode=Persistent: broker ghi message xuong dia, khong mat khi RabbitMQ khoi dong lai.
PERSISTENT = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")


def connect_or_exit():
    """Mo ket noi + channel toi RabbitMQ; neu khong ket noi duoc thi bao loi ro rang va thoat."""
    params = pika.ConnectionParameters(
        host=BROKER_HOST,
        port=BROKER_PORT,
        credentials=pika.PlainCredentials(BROKER_USER, BROKER_PASSWORD),
    )
    try:
        connection = pika.BlockingConnection(params)
    except pika.exceptions.ProbableAuthenticationError:
        print(f"LOI: RabbitMQ tai {BROKER_HOST}:{BROKER_PORT} tu choi tai khoan '{BROKER_USER}' (sai user/password).")
        sys.exit(1)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"LOI: Khong ket noi duoc RabbitMQ tai {BROKER_HOST}:{BROKER_PORT} ({type(err).__name__}).")
        print("Hay kiem tra RabbitMQ da chay chua: docker compose up -d")
        sys.exit(1)
    return connection, connection.channel()


def declare_queue(channel, name):
    """Khai bao (tao neu chua co) mot queue durable.

    RabbitMQ 4.x khong cho tao queue khong ben (durable=False, mac dinh cua pika), va moi noi
    khai bao cung mot queue phai dung cung tham so, nen tat ca deu goi qua ham nay.
    """
    channel.queue_declare(queue=name, durable=True)


def declare_alert_exchanges(channel):
    """Khai bao 2 exchange cua Bai 3 voi cung tham so o moi chuong trinh."""
    channel.exchange_declare(exchange=ALERT_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=ALERT_TOPIC_EXCHANGE, exchange_type="topic", durable=True)


def publish(channel, exchange, routing_key, body, mandatory=True):
    """Gui message persistent. Tra ve False neu broker bao khong co queue nao nhan message.

    Can goi channel.confirm_delivery() truoc: khi do basic_publish cho broker xac nhan,
    va voi mandatory=True broker tra lai message khong dinh tuyen duoc (UnroutableError).
    """
    try:
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                              properties=PERSISTENT, mandatory=mandatory)
    except pika.exceptions.UnroutableError:
        return False
    return True


def log_alert(queue_name, routing_key, text):
    """Ghi mot canh bao vao file log cua Bai 3 (mo rong)."""
    with open(ALERT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{queue_name}] [{routing_key}] {text}\n")
