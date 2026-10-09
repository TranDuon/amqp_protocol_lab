# Bài 1 – Gửi và nhận message cơ bản qua queue

Làm quen với mô hình **producer → queue → consumer**.

| | |
|---|---|
| Queue | `iot_lab_queue` (durable) |
| Exchange | default exchange `""`, routing key = `iot_lab_queue` |
| Message | `Xin chao tu ung dung Python AMQP - <MSSV> - <Ho ten>` (persistent) |

## Chương trình

| File | Vai trò | Làm gì |
|---|---|---|
| `producer_bai1.py` | Producer | Kết nối RabbitMQ, khai báo queue `iot_lab_queue`, gửi message chào mừng (mặc định 1, `-n` để gửi nhiều), chờ broker xác nhận từng message rồi thoát. |
| `consumer_bai1.py` | Consumer | Kết nối RabbitMQ, khai báo queue, nhận message, in nội dung và thời điểm nhận, **ack** sau khi in. Chạy đến khi Ctrl+C. |

Họ tên và MSSV lấy từ `common/config.py`.

## Luồng chạy

```
producer:  connect ─▶ confirm_delivery ─▶ queue_declare(iot_lab_queue, durable)
           basic_publish(exchange="", routing_key="iot_lab_queue", body) ─▶ broker xác nhận ─▶ close

consumer:  connect ─▶ queue_declare(iot_lab_queue, durable) ─▶ basic_qos(prefetch=1)
           basic_consume(on_message) ─▶ start_consuming()   (chặn main thread)
           broker giao message ─▶ on_message: in 2 dòng ─▶ basic_ack
```

- Cả hai bên đều `queue_declare`: thao tác này là "tạo nếu chưa có", nên chạy bên nào trước cũng được.
- **Default exchange** (tên rỗng) tự nối mọi queue với routing key bằng chính tên queue; vì vậy gửi với `routing_key="iot_lab_queue"` là message vào thẳng queue này.
- Queue lưu message cho tới khi có consumer lấy và ack: producer chạy khi consumer đang tắt thì message vẫn chờ trong queue.
- `basic_ack` gửi sau khi xử lý xong: nếu consumer chết trước khi ack, RabbitMQ giao lại message cho lần sau.
- pika gọi `on_message` trên chính main thread (không có thread nền như paho-mqtt).

## Cách chạy

```powershell
python bai1_queue/consumer_bai1.py           # terminal 1
python bai1_queue/producer_bai1.py           # terminal 2 (thêm -n 5 để gửi 5 message)
```

Thử thứ tự ngược: chạy producer trước (`-n 2`), mở http://localhost:15672 → **Queues and Streams** thấy `iot_lab_queue` có 2 message *Ready*; sau đó chạy consumer sẽ nhận đủ 2 message.

## Output thực tế

Producer (`-n 3`):

```
Da ket noi RabbitMQ 127.0.0.1:5672
Da gui message [1/3] vao queue iot_lab_queue: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
Da gui message [2/3] vao queue iot_lab_queue: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
Da gui message [3/3] vao queue iot_lab_queue: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
```

Consumer:

```
Da ket noi RabbitMQ 127.0.0.1:5672
Dang lang nghe queue iot_lab_queue ... (Ctrl+C de thoat)

Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
Thoi gian nhan: 10:53:17

Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
Thoi gian nhan: 10:53:17

Da nhan message: Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong
Thoi gian nhan: 10:53:17

Da dung consumer.
```

## Đối chiếu tiêu chí đánh giá

- [x] Kết nối được broker
- [x] Gửi được message vào queue `iot_lab_queue`
- [x] Consumer nhận và hiển thị đúng nội dung và thời điểm nhận
- [x] Mở rộng: producer gửi nhiều message liên tiếp (`-n`); consumer chạy liên tục đến khi Ctrl+C
