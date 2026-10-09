# Kết quả kiểm thử và đối chiếu đề bài

Môi trường kiểm thử: Windows 11, Python 3.13.6, pika 1.4.4, RabbitMQ 4.3.6 (Erlang/OTP 27) chạy bằng Docker (`rabbitmq:4-management`).

## 1. Đối chiếu yêu cầu đề bài

### Yêu cầu chung

- [x] Python 3.x + thư viện pika
- [x] RabbitMQ broker (Docker, cấu hình trong `docker-compose.yml`, hướng dẫn trong `README.md` mục 4)
- [x] Đủ 7 file đúng tên: `producer_bai1.py`, `consumer_bai1.py`, `sensor_producer_bai2.py`, `monitor_consumer_bai2.py`, `alert_producer_bai3.py`, `warning_consumer_bai3.py`, `critical_consumer_bai3.py`
- [x] `README.md` mô tả broker sử dụng, cách chạy từng chương trình, kết quả đạt được

### Bài 1

- [x] Producer kết nối RabbitMQ, tạo/sử dụng queue `iot_lab_queue`
- [x] Message gồm họ tên, MSSV, lời chào: `Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong`
- [x] Consumer kết nối cùng broker, lắng nghe `iot_lab_queue`
- [x] In nội dung (`Da nhan message: ...`) và thời điểm nhận (`Thoi gian nhan: HH:MM:SS`)
- [x] Mở rộng: producer gửi nhiều message (`-n`)
- [x] Mở rộng: consumer chạy liên tục đến khi Ctrl+C

### Bài 2

- [x] Sensor gửi dữ liệu mỗi 3 giây vào `sensor_data_queue`
- [x] Payload JSON gồm `device_id`, `temperature`, `humidity`
- [x] Giá trị sinh ngẫu nhiên
- [x] Consumer nhận từ `sensor_data_queue`, in mã thiết bị, nhiệt độ, độ ẩm
- [x] Nhiệt độ > 35 → `CANH BAO: Nhiet do cao`
- [x] Độ ẩm < 40 → `CANH BAO: Do am thap`
- [x] Mở rộng: bổ sung trường thời gian gửi (`timestamp`)
- [x] Mở rộng: mô phỏng 2–3 cảm biến

### Bài 3

- [x] Producer tạo direct exchange `iot_alert_exchange`
- [x] Gửi cảnh báo các mức info, warning, critical với routing key tương ứng
- [x] Warning consumer tạo `warning_queue`, bind với routing key `warning`, chỉ nhận mức warning
- [x] Critical consumer tạo `critical_queue`, bind với routing key `critical`, chỉ nhận mức critical
- [x] info: không consumer nào nhận; warning: chỉ `warning_queue`; critical: chỉ `critical_queue`
- [x] Output dạng `[warning_queue] Da nhan: ...`, `[critical_queue] Da nhan: ...`
- [x] Mở rộng: consumer nhận tất cả cảnh báo bằng topic exchange (`all_alert_consumer_bai3.py`)
- [x] Mở rộng: ghi log cảnh báo ra file (`bai3_alert_exchange/alerts.log`)

## 2. Các ca kiểm thử đã chạy

| # | Ca kiểm thử | Cách thực hiện | Kết quả |
|---|---|---|---|
| 1 | Broker hoạt động | `rabbitmq-diagnostics ping`; HTTP API: tạo queue, publish qua default exchange, lấy message | Nhận lại đúng `hello amqp`, exchange `''` |
| 2 | Kết nối từ Python | `config.connect_or_exit()` | Kết nối RabbitMQ 4.3.6 |
| 3 | Bài 1 đầu cuối | Consumer trước, producer `-n 3` | Consumer nhận đủ 3, đúng format |
| 4 | Queue giữ message | Producer `-n 2` khi chưa có consumer, sau đó bật consumer | Queue có 2 message *Ready*; consumer nhận đủ 2; queue về 0 |
| 5 | Nhiều consumer một queue | 2 consumer, producer `-n 4` | Mỗi consumer nhận 2 (round-robin) |
| 6 | Chu kỳ Bài 2 | Sensor mặc định ~7,5 giây | Gửi lúc :35, :38, :41 – đúng 3 giây |
| 7 | Cảnh báo | Gửi tay 36.4 / 38.9 | Cả 2 cảnh báo |
| 8 | Giá trị biên | Gửi tay 35.0 / 40.0 | Không cảnh báo |
| 9 | Một cảnh báo | Gửi tay 20.5 / 39.9; sensor 35.2 | Chỉ cảnh báo tương ứng |
| 10 | Payload không phải JSON / thiếu trường | Gửi tay `du lieu loi`, `{"device_id":"sensor01","temperature":30}` | In lỗi, monitor tiếp tục; queue không còn message (đã nack loại bỏ) |
| 11 | Nhiều cảm biến | `sensor01 sensor02 sensor03 -i 1` | Monitor nhận dữ liệu cả 3 cảm biến |
| 12 | Định tuyến Bài 3 | 3 consumer + producer gửi 3 cảnh báo mẫu | info: producer báo không queue nào nhận; warning chỉ tới `warning_queue`; critical chỉ tới `critical_queue`; `all_alerts_queue` nhận cả 3 |
| 13 | Cảnh báo tự nhập | `alert_producer_bai3.py critical "Mat dien phong server"` | Chỉ `critical_queue` và `all_alerts_queue` nhận |
| 14 | Exchange và binding trên broker | HTTP API `/api/exchanges`, `/api/bindings` | `iot_alert_exchange` type direct; binding `warning`→`warning_queue`, `critical`→`critical_queue`, không có binding `info` |
| 15 | Consumer đang tắt | Gửi warning khi không có consumer, sau đó bật warning consumer | Producer báo "da vao queue"; queue có 1 message; consumer bật lên nhận được |
| 16 | Ghi log | Xem `alerts.log` sau ca 12–13 | Có đủ các dòng của từng queue |
| 17 | Broker không chạy | `AMQP_PORT=5999` với consumer, sensor, alert producer | Một dòng lỗi rõ ràng, exit code 1, không traceback |
| 18 | Sai tài khoản | `AMQP_PASSWORD=sai` | Báo bị từ chối tài khoản, exit code 1 |
| 19 | Broker tắt giữa chừng | `docker stop iot-rabbitmq` khi sensor và monitor đang chạy | Cả hai báo `Mat ket noi RabbitMQ (ConnectionClosedByBroker)` và thoát gọn |
| 20 | Ctrl+C | Gửi Ctrl+C thật (`GenerateConsoleCtrlEvent`) tới 6 chương trình chạy liên tục | Tất cả in thông báo dừng, exit code 0 |

## 3. Vấn đề phát hiện trong quá trình làm

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| Tạo queue `durable=false` bị từ chối (`transient_nonexcl_queues is deprecated`) | RabbitMQ 4.x không còn cho phép queue không bền, không độc quyền; pika mặc định `durable=False` | Mọi queue khai báo qua `config.declare_queue()` với `durable=True` |
| Khai báo lại queue với tham số khác gây `PRECONDITION_FAILED` | Queue đã tồn tại với `durable=True` | Gom khai báo queue/exchange vào hàm dùng chung để luôn cùng tham số |
| Không cài được RabbitMQ bằng winget | winget không có gói RabbitMQ server; Erlang trên winget (29.x) khác phiên bản image chính thức dùng (27.x) | Chạy RabbitMQ bằng Docker image chính thức |
