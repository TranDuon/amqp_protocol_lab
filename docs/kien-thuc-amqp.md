# Kiến thức AMQP dùng trong lab

Tài liệu giải thích các khái niệm AMQP, RabbitMQ và pika thực sự được dùng trong 3 bài, kèm so sánh với MQTT (lab trước).

## 1. AMQP và RabbitMQ

**AMQP 0-9-1** (Advanced Message Queuing Protocol) là giao thức hàng đợi message chạy trên TCP, cổng mặc định **5672**. Nó được thiết kế cho trao đổi message tin cậy giữa các hệ thống: message được lưu trong hàng đợi, có xác nhận, có định tuyến linh hoạt.

**RabbitMQ** là broker AMQP phổ biến nhất (viết bằng Erlang). Plugin *management* cung cấp giao diện web ở cổng **15672** để xem queue, exchange, binding và số message.

**pika** là thư viện Python nói giao thức AMQP 0-9-1 với RabbitMQ.

## 2. Mô hình: producer → exchange → queue → consumer

```
Producer ──publish(exchange, routing_key, body)──▶ EXCHANGE ──binding──▶ QUEUE ──▶ Consumer
```

Điểm quan trọng nhất: **producer không gửi thẳng vào queue**. Nó gửi vào một exchange; exchange dựa vào routing key của message và các binding để quyết định message vào queue nào (có thể nhiều queue, có thể không queue nào).

| Khái niệm | Ý nghĩa | Trong lab |
|---|---|---|
| **Producer** | Chương trình gửi message | `producer_bai1`, `sensor_producer_bai2`, `alert_producer_bai3` |
| **Consumer** | Chương trình nhận message từ một queue | `consumer_bai1`, `monitor_consumer_bai2`, các consumer Bài 3 |
| **Queue** | Hàng đợi **lưu message** trên broker cho tới khi consumer lấy và ack | `iot_lab_queue`, `sensor_data_queue`, `warning_queue`, `critical_queue` |
| **Exchange** | Bộ định tuyến: nhận message từ producer, chuyển vào queue theo luật | default exchange, `iot_alert_exchange`, `iot_alert_topic_exchange` |
| **Binding** | Luật nối exchange → queue, kèm *binding key* | `iot_alert_exchange --[warning]--> warning_queue` |
| **Routing key** | Nhãn producer gắn lên message; exchange so với binding key | `info`, `warning`, `critical`, `alert.warning` |
| **Connection** | Kết nối TCP tới broker | `pika.BlockingConnection` |
| **Channel** | Kênh logic bên trong connection; mọi lệnh AMQP đi qua channel | `connection.channel()` |
| **Virtual host** | Không gian tách biệt trên broker (queue/exchange riêng) | mặc định `/` |

## 3. Các loại exchange

| Loại | Luật định tuyến | Trong lab |
|---|---|---|
| **Default** (tên `""`) | Là direct exchange đặc biệt, mọi queue tự động được bind với binding key = tên queue. Gửi với `routing_key="ten_queue"` thì message vào queue đó | Bài 1, 2 ("gửi vào queue") |
| **Direct** | Routing key phải **trùng chính xác** binding key | Bài 3 `iot_alert_exchange` |
| **Topic** | Routing key gồm các từ ngăn cách bằng `.`; binding key dùng `*` (đúng một từ) và `#` (0 hoặc nhiều từ) | Mở rộng Bài 3: `alert.#` |
| **Fanout** | Gửi cho mọi queue được bind, bỏ qua routing key | Không dùng |

Ví dụ topic: binding `alert.*` khớp `alert.warning` nhưng không khớp `alert.room1.warning`; binding `alert.#` khớp cả hai.

Một message khớp nhiều binding thì **mỗi queue nhận một bản sao**. Không khớp binding nào thì message **bị bỏ** (trừ khi gửi với `mandatory=True`, xem mục 5).

## 4. Queue lưu message, consumer chia nhau message

- Producer gửi khi chưa có consumer: message **nằm chờ trong queue**. Consumer kết nối sau vẫn nhận đủ. (MQTT không làm được điều này nếu không dùng retain/session.)
- Nhiều consumer cùng lấy **một queue**: message được **chia lần lượt** (round-robin), mỗi message chỉ tới một consumer. Đây là mô hình *work queue*, dùng để chia tải xử lý.
- Muốn nhiều bên cùng nhận đủ mọi message: mỗi bên một queue riêng, cùng bind vào một exchange (như `warning_queue` và `all_alerts_queue` cùng nhận cảnh báo warning qua hai exchange).

## 5. Tin cậy: durable, persistent, ack, confirm

| Cơ chế | Ý nghĩa | Trong lab |
|---|---|---|
| **Durable queue/exchange** | Định nghĩa queue/exchange còn sau khi broker khởi động lại | Mọi queue và exchange (`durable=True`) |
| **Persistent message** | Message được ghi xuống đĩa, còn sau khi broker khởi động lại | `delivery_mode=Persistent` trong `config.PERSISTENT` |
| **Consumer ack** | Consumer báo đã xử lý xong; chưa ack mà consumer chết thì broker giao lại message | `basic_ack` sau khi in |
| **Nack** | Consumer từ chối message; `requeue=False` thì bỏ message | Payload hỏng ở Bài 2 |
| **Prefetch** (`basic_qos`) | Số message tối đa consumer giữ mà chưa ack | `prefetch_count=1` |
| **Publisher confirm** | Broker xác nhận đã nhận từng message producer gửi (giống PUBACK của MQTT) | `channel.confirm_delivery()` |
| **mandatory** | Broker trả message về cho producer nếu không vào được queue nào | Cảnh báo `info` ở Bài 3 |

### Lưu ý với RabbitMQ 4.x

- **Không tạo được queue không bền.** pika mặc định `queue_declare(durable=False)`; RabbitMQ 4.x từ chối kiểu queue này (*transient non-exclusive queue*) và **đóng cả kết nối** với lỗi `541 INTERNAL_ERROR - Feature transient_nonexcl_queues is deprecated`. Code mẫu cũ trên mạng (`queue_declare(queue='hello')`) sẽ lỗi; phải dùng `durable=True` (hoặc `exclusive=True` cho queue tạm của riêng một kết nối).
- **Khai báo phải nhất quán.** `queue_declare`/`exchange_declare` là "tạo nếu chưa có", nhưng nếu đã có với tham số khác thì broker báo `406 PRECONDITION_FAILED - inequivalent arg 'durable'`. Vì vậy lab gom việc khai báo vào `config.declare_queue()` và `config.declare_alert_exchanges()`.
- `basic_qos(global_qos=True)` cũng không còn được phép; chỉ dùng prefetch theo từng consumer.

## 6. pika trong Python

### 6.1. Kết nối

```python
params = pika.ConnectionParameters(host="127.0.0.1", port=5672,
                                   credentials=pika.PlainCredentials("guest", "guest"))
connection = pika.BlockingConnection(params)   # lỗi -> pika.exceptions.AMQPConnectionError
channel = connection.channel()
```

`BlockingConnection` là kiểu kết nối đồng bộ, đơn giản nhất. Các lỗi kết nối đều kế thừa `AMQPConnectionError`: `ProbableAuthenticationError` (sai tài khoản), `StreamLostError` (mất kết nối), `ConnectionClosedByBroker` (broker đóng kết nối).

### 6.2. Gửi

```python
channel.confirm_delivery()                              # bật publisher confirm
channel.queue_declare(queue="iot_lab_queue", durable=True)
channel.basic_publish(exchange="", routing_key="iot_lab_queue", body="...",
                      properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
                      mandatory=True)                   # không queue nào nhận -> UnroutableError
```

### 6.3. Nhận

```python
def on_message(ch, method, properties, body):          # body là bytes
    print(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="iot_lab_queue", on_message_callback=on_message)
channel.start_consuming()                               # chặn main thread
```

`method.routing_key` cho biết routing key của message (Bài 3 dùng để biết mức cảnh báo), `method.delivery_tag` dùng để ack.

### 6.4. Thread và heartbeat

- pika gọi callback **trên chính thread đang chạy `start_consuming()`** (main thread), không có thread nền như `loop_start()` của paho-mqtt. Chương trình vì thế đơn giản hơn, nhưng callback chạy lâu sẽ chặn việc nhận message khác.
- pika **không thread-safe**: không dùng chung một connection/channel giữa nhiều thread.
- Broker và client gửi **heartbeat** định kỳ (giống keep alive của MQTT). `BlockingConnection` chỉ xử lý heartbeat khi được gọi, nên trong vòng lặp gửi định kỳ dùng `connection.sleep(giây)` thay vì `time.sleep()`.
- Exception lọt ra khỏi callback sẽ làm `start_consuming()` dừng: phần xử lý dễ lỗi (như `json.loads`) phải nằm trong `try/except`.
- `BlockingConnection` **không tự kết nối lại** khi mất kết nối; lab bắt lỗi và thoát với thông báo rõ ràng.

## 7. So sánh AMQP và MQTT

| | MQTT (lab trước) | AMQP (lab này) |
|---|---|---|
| Đích gửi | Topic | Exchange + routing key |
| Broker lưu message | Không (trừ retain / persistent session) | Có, trong queue, cho tới khi được ack |
| Nhiều bên nhận | Mọi subscriber khớp topic đều nhận | Mỗi queue một bản; các consumer cùng queue chia nhau |
| Định tuyến | Wildcard `+`, `#` trong topic filter | Loại exchange + binding (`*`, `#` ở topic exchange) |
| Xác nhận | QoS 0/1/2 | Consumer ack/nack, publisher confirm |
| Thư viện Python | paho-mqtt: callback trên thread mạng | pika: callback trên main thread |
| Tự kết nối lại | Có (paho) | Không (pika BlockingConnection) |
| Phù hợp | Thiết bị nhẹ, mạng yếu, telemetry | Hệ thống backend, xử lý tin cậy, phân phối công việc |

Trong thực tế hai giao thức hay đi cùng nhau: thiết bị gửi bằng MQTT, RabbitMQ (có plugin MQTT) nhận và chuyển vào queue AMQP cho backend xử lý.

## 8. Áp dụng trong 3 bài

| Bài | Kiến thức chính |
|---|---|
| 1 | Connection/channel, `queue_declare` durable, default exchange, `basic_publish`, `basic_consume` + ack, queue lưu message, round-robin |
| 2 | Payload JSON, gửi định kỳ với `connection.sleep`, nhiều producer một queue, nack message hỏng |
| 3 | Direct exchange, binding theo routing key, message không định tuyến được (`mandatory`), topic exchange với `#` |
