# Nếu tự làm lại từ đầu

Hướng dẫn tóm tắt cách tiếp cận một bài AMQP/RabbitMQ để có thể tự làm bài tương tự mà không cần code mẫu.

## 1. Đọc đề như thế nào

Ghi ra giấy trước khi code:

1. **Có những chương trình nào?** Producer nào, consumer nào.
2. **Message đi qua đâu?** Đề nói "gửi vào queue X" → default exchange. Đề nói "exchange", "routing key", "bind" → phải tạo exchange và binding.
3. **Tên chính xác:** tên queue, tên exchange, loại exchange (direct/topic/fanout), routing key, tên trường JSON, chuỗi in ra màn hình.
4. **Ai tạo cái gì?** Đề Bài 3 nói rõ: producer tạo exchange, consumer tạo queue và bind.
5. **Tiêu chí đánh giá** – dùng làm checklist nghiệm thu.

## 2. Xác định các thành phần

Lập bảng:

| Chương trình | Gửi vào exchange | Routing key | Queue lấy ra | Binding tạo ra |
|---|---|---|---|---|
| alert_producer | `iot_alert_exchange` | `info`/`warning`/`critical` | – | – |
| warning_consumer | – | – | `warning_queue` | `iot_alert_exchange` --[warning]--> `warning_queue` |

## 3. Thiết kế kiến trúc

- Vẽ sơ đồ: producer → exchange → (binding) → queue → consumer.
- Hỏi: **mỗi message cần tới bao nhiêu nơi?**
  - Một nơi xử lý, có thể nhiều consumer chia tải → một queue, nhiều consumer.
  - Nhiều nơi cùng nhận đủ → mỗi nơi một queue, cùng bind một exchange.
- Hỏi: **chọn đích theo tiêu chí gì?**
  - Theo tên queue cố định → default exchange.
  - Theo một giá trị chính xác (mức cảnh báo) → direct exchange.
  - Theo mẫu (`alert.#`, `*.critical`) → topic exchange.
  - Gửi cho tất cả → fanout exchange.

## 4. Xác định producer / consumer / broker

- **Broker** là RabbitMQ (chạy riêng, thường bằng Docker), không phải code của mình.
- Chương trình tạo dữ liệu / phát sự kiện → producer.
- Chương trình xử lý dữ liệu → consumer.
- Một chương trình có thể vừa consume vừa publish (ví dụ nhận dữ liệu thô, xử lý, rồi gửi kết quả sang exchange khác).

## 5. Đặt tên queue, exchange, routing key

- Queue đặt theo **người xử lý**: `warning_queue`, `email_queue`.
- Exchange đặt theo **loại sự kiện**: `iot_alert_exchange`.
- Routing key topic dùng các từ ngăn cách bằng dấu chấm, từ chung đến riêng: `alert.room1.critical`, để sau này bind bằng `alert.#` hay `*.*.critical`.

## 6. Các hàm và class cần tạo

- **Module cấu hình chung:** thông tin broker, tên queue/exchange, hàm kết nối có báo lỗi, hàm khai báo queue/exchange (đảm bảo cùng tham số ở mọi nơi).
- **Mỗi consumer:** `on_message(ch, method, properties, body)` (xử lý + ack), `main()` (kết nối → declare → bind → qos → consume → start_consuming).
- **Mỗi producer:** `main()` (kết nối → confirm_delivery → declare → publish → close).
- **Hàm thuần cho nghiệp vụ**, tách khỏi AMQP: `check_alerts()`, `read_sensor()`.

## 7. Bắt đầu code từ đâu

1. Chạy broker và **kiểm tra bằng giao diện web** (http://localhost:15672): tạo thử queue, *Publish message*, *Get messages*.
2. Cài pika, viết module cấu hình, thử kết nối.
3. Viết consumer, kiểm tra bằng cách publish tay trên giao diện web.
4. Viết producer, kiểm tra bằng consumer vừa viết và cột *Ready* trên giao diện web.
5. Thêm nghiệp vụ (JSON, ngưỡng, routing).
6. Thêm phần mở rộng.
7. Thêm xử lý lỗi: broker tắt, payload hỏng, Ctrl+C.

Khung tối thiểu một consumer:

```python
import pika

def on_message(ch, method, properties, body):
    print(body.decode())
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
channel = connection.channel()
channel.queue_declare(queue="iot_lab_queue", durable=True)
channel.basic_consume(queue="iot_lab_queue", on_message_callback=on_message)
channel.start_consuming()
```

Khung tối thiểu một producer:

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
channel = connection.channel()
channel.queue_declare(queue="iot_lab_queue", durable=True)
channel.basic_publish(exchange="", routing_key="iot_lab_queue", body="hello")
connection.close()
```

Khung tối thiểu cho exchange + binding:

```python
channel.exchange_declare(exchange="iot_alert_exchange", exchange_type="direct", durable=True)
channel.queue_declare(queue="warning_queue", durable=True)
channel.queue_bind(queue="warning_queue", exchange="iot_alert_exchange", routing_key="warning")
# producer:
channel.basic_publish(exchange="iot_alert_exchange", routing_key="warning", body="...")
```

## 8. Test từng phần

- Test consumer một mình bằng *Publish message* trên giao diện web; test producer một mình bằng cột *Ready* / *Get messages*.
- Test thứ tự chạy: producer trước rồi consumer, và ngược lại.
- Test định tuyến: gửi đủ mọi routing key, kiểm tra từng queue chỉ nhận đúng phần của mình; xem tab *Exchanges* → *Bindings*.
- Test dữ liệu sai, giá trị biên, broker tắt, Ctrl+C.

## 9. Debug AMQP

Đi theo đường đi của message:

1. **Broker có chạy không?** `docker ps`, `docker exec iot-rabbitmq rabbitmq-diagnostics ping`.
2. **Có kết nối không?** Tab *Connections* trên giao diện web hiện các client đang kết nối.
3. **Exchange có tồn tại, đúng loại không?** Tab *Exchanges*.
4. **Binding có đúng không?** Mở exchange → mục *Bindings*: queue nào, binding key gì. Sai một ký tự là không khớp (routing key phân biệt hoa thường).
5. **Message có vào queue không?** Cột *Ready* tăng là message đã vào queue. Không tăng → vấn đề ở exchange/binding/routing key. Gửi với `mandatory=True` để producer biết khi message không vào queue nào.
6. **Consumer có lấy không?** *Ready* không giảm → consumer không chạy hoặc lấy nhầm queue. *Unacked* tăng mãi → consumer quên `basic_ack`.
7. **Callback có ném exception không?** Exception làm `start_consuming()` dừng.

## 10. Lỗi người mới thường gặp

| Lỗi | Hậu quả | Cách tránh |
|---|---|---|
| `queue_declare(queue=...)` không có `durable=True` trên RabbitMQ 4.x | Broker đóng kết nối (`transient_nonexcl_queues is deprecated`) | Luôn `durable=True` |
| Khai báo cùng queue/exchange với tham số khác nhau ở hai chương trình | `PRECONDITION_FAILED - inequivalent arg` | Gom khai báo vào một hàm dùng chung |
| Gửi vào exchange khi chưa có queue nào bind | Message biến mất không báo lỗi | Chạy consumer (tạo binding) trước; dùng `mandatory=True` + `confirm_delivery()` |
| Quên `basic_ack` (khi `auto_ack=False`) | Message kẹt ở *Unacked*, giao lại khi consumer khởi động lại | Ack sau khi xử lý xong |
| `auto_ack=True` rồi xử lý lỗi | Message mất khi consumer chết giữa chừng | Ack thủ công |
| `basic_nack(requeue=True)` cho message hỏng | Vòng lặp vô hạn nhận lại – lỗi lại | `requeue=False` (hoặc chuyển sang dead-letter queue) |
| Không bắt exception trong callback | Consumer dừng khi gặp một message hỏng | `try/except` quanh phần xử lý |
| `time.sleep()` dài trong chương trình dùng `BlockingConnection` | Broker cắt kết nối vì mất heartbeat | `connection.sleep()` |
| Dùng chung connection/channel giữa nhiều thread | Lỗi khó đoán | Mỗi thread một connection |
| Chờ đợi mọi consumer đều nhận đủ message khi cùng một queue | Mỗi consumer chỉ nhận một phần (round-robin) | Mỗi consumer một queue riêng nếu cần nhận đủ |
| Nhầm routing key với tên queue khi dùng exchange khác default | Message không tới đâu | Chỉ default exchange mới dùng tên queue làm routing key |
| Dùng `guest` để kết nối từ máy khác tới RabbitMQ cài trực tiếp | Bị từ chối đăng nhập | Tạo user riêng, hoặc kết nối từ chính máy chạy broker |
