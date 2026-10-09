# Lab AMQP – Lập trình Python với giao thức AMQP

Sinh viên: **Trần Đăng Dương** – MSSV: **B23DCCN227**

Bài thực hành gồm 3 bài, các chương trình Python trao đổi message với nhau qua broker **RabbitMQ** bằng thư viện **pika**:

| Bài | Folder | Chương trình | Thành phần AMQP | Nội dung |
|---|---|---|---|---|
| 1 | [bai1_queue](bai1_queue/) | `producer_bai1.py`, `consumer_bai1.py` | queue `iot_lab_queue` | Gửi/nhận message cơ bản qua queue |
| 2 | [bai2_sensor](bai2_sensor/) | `sensor_producer_bai2.py`, `monitor_consumer_bai2.py` | queue `sensor_data_queue` | Cảm biến gửi JSON mỗi 3 giây, giám sát và cảnh báo ngưỡng |
| 3 | [bai3_alert_exchange](bai3_alert_exchange/) | `alert_producer_bai3.py`, `warning_consumer_bai3.py`, `critical_consumer_bai3.py` (+ `all_alert_consumer_bai3.py` mở rộng) | direct exchange `iot_alert_exchange`, routing key `info`/`warning`/`critical` | Điều phối cảnh báo theo mức |

Mỗi folder bài có `README.md` riêng mô tả yêu cầu, luồng dữ liệu, cách chạy và output thực tế.

---

## 1. Công nghệ sử dụng

| Thành phần | Phiên bản | Vai trò |
|---|---|---|
| Python | 3.x (đã chạy thử trên 3.13.6) | Ngôn ngữ lập trình |
| [pika](https://pypi.org/project/pika/) | 1.4.4 | Thư viện AMQP 0-9-1 client (đề bài yêu cầu) |
| [RabbitMQ](https://www.rabbitmq.com/) | 4.3.x (image Docker `rabbitmq:4-management`) | AMQP broker, kèm giao diện web quản lý |
| Docker Desktop | – | Chạy RabbitMQ không cần cài Erlang |
| AMQP | 0-9-1 | Giao thức message queue |
| JSON | – | Định dạng dữ liệu cảm biến Bài 2 |

## 2. Kiến trúc

Producer **không gửi thẳng vào queue**: nó gửi vào một **exchange**, exchange dựa vào **routing key** và các **binding** để chuyển message vào queue; consumer lấy message từ queue.

```
                    ┌──────────────────────────────────────────────┐
                    │  RabbitMQ (Docker)  127.0.0.1:5672  · web :15672 │
                    └──────────────────────────────────────────────┘
Bài 1  producer ─▶ default exchange ─(rk = iot_lab_queue)─▶ [iot_lab_queue] ─▶ consumer

Bài 2  sensor ─▶ default exchange ─(rk = sensor_data_queue)─▶ [sensor_data_queue] ─▶ monitor (+ cảnh báo)

Bài 3  alert_producer ─▶ iot_alert_exchange (direct)
                          ├─ rk = warning  ─▶ [warning_queue]  ─▶ warning_consumer
                          ├─ rk = critical ─▶ [critical_queue] ─▶ critical_consumer
                          └─ rk = info     ─▶ (không có binding → không queue nào nhận)
       (mở rộng)       ─▶ iot_alert_topic_exchange (topic) ─ rk = alert.<mức> ─▶ [all_alerts_queue] ─▶ all_alert_consumer
```

- Bài 1, 2 dùng **default exchange** (tên rỗng `""`): gửi với routing key = tên queue thì message vào đúng queue đó. Đây là cách "gửi vào queue" mà đề bài mô tả.
- Bài 3 dùng **direct exchange**: message chỉ vào queue có binding key trùng chính xác routing key.
- Mọi queue đều **durable**, message **persistent**, consumer **ack** sau khi xử lý: message không mất khi consumer chưa chạy hoặc broker khởi động lại.

## 3. Cấu trúc project

```
amqp_protocol/
├── README.md                 # file này: tổng quan, cấu hình broker, cách chạy
├── docker-compose.yml        # cấu hình broker RabbitMQ
├── requirements.txt          # pika==1.4.4
├── common/
│   └── config.py             # dùng chung: địa chỉ broker, họ tên + MSSV, tên queue/exchange,
│                             # connect_or_exit(), declare_queue(), declare_alert_exchanges(), publish()
├── bai1_queue/               # Bài 1: producer_bai1.py, consumer_bai1.py, README.md
├── bai2_sensor/              # Bài 2: sensor_producer_bai2.py, monitor_consumer_bai2.py, README.md
├── bai3_alert_exchange/      # Bài 3: alert_producer_bai3.py, warning_consumer_bai3.py,
│                             # critical_consumer_bai3.py, all_alert_consumer_bai3.py (mở rộng), README.md
└── docs/
    ├── de-bai.md / de-bai.docx      # đề bài
    ├── kien-thuc-amqp.md            # kiến thức AMQP, RabbitMQ, pika dùng trong lab
    ├── ket-qua-kiem-thu.md          # checklist đối chiếu đề bài và kết quả test
    └── huong-dan-tu-lam-lai.md      # hướng dẫn tự làm lại một bài AMQP tương tự
```

Mỗi script tự thêm thư mục gốc lab vào `sys.path` để import `common/config.py`, nên chạy được từ bất kỳ thư mục nào.

---

## 4. Cài đặt và cấu hình broker RabbitMQ

### 4.1. Cách khuyến nghị: Docker

1. Cài [Docker Desktop](https://www.docker.com/products/docker-desktop/) và mở nó (biểu tượng cá voi ở khay hệ thống chuyển sang trạng thái *running*).
2. Tại thư mục `amqp_protocol`:
   ```powershell
   docker compose up -d
   ```
   Lần đầu Docker tải image `rabbitmq:4-management` (khoảng 250 MB).
3. Kiểm tra:
   ```powershell
   docker ps                                              # thấy container iot-rabbitmq trạng thái Up
   docker exec iot-rabbitmq rabbitmq-diagnostics ping     # Ping succeeded
   ```
4. Mở giao diện quản lý http://localhost:15672, đăng nhập `guest` / `guest`. Tab **Queues and Streams** và **Exchanges** cho thấy queue, exchange, binding và số message đang chờ.

Dừng broker: `docker compose down`. Xóa sạch dữ liệu (queue, message) để làm lại từ đầu: `docker compose down -v`.

### 4.2. Nội dung `docker-compose.yml`

| Mục | Ý nghĩa |
|---|---|
| `image: rabbitmq:4-management` | RabbitMQ 4.x kèm plugin giao diện web quản lý (Erlang đã có sẵn trong image) |
| `hostname: rabbitmq` | Cố định tên node; RabbitMQ lưu dữ liệu theo tên node nên cần cố định để dữ liệu durable dùng lại được |
| `127.0.0.1:5672:5672` | Cổng AMQP mà pika kết nối, chỉ mở trên máy này |
| `127.0.0.1:15672:15672` | Cổng giao diện web quản lý |
| `rabbitmq_data:/var/lib/rabbitmq` | Volume lưu queue và message durable qua các lần khởi động lại |

Tài khoản mặc định `guest` / `guest`. Muốn máy khác trong mạng LAN kết nối, bỏ `127.0.0.1:` trong phần `ports` (khi đó nên tạo tài khoản riêng thay cho `guest`).

### 4.3. Cách khác: cài RabbitMQ trực tiếp trên Windows

Cài Erlang/OTP **đúng phiên bản RabbitMQ hỗ trợ** (xem bảng tương thích tại https://www.rabbitmq.com/docs/which-erlang), rồi cài RabbitMQ từ https://www.rabbitmq.com/docs/install-windows và bật giao diện web:

```powershell
rabbitmq-plugins enable rabbitmq_management
```

Cài trực tiếp thì tài khoản `guest` chỉ đăng nhập được từ chính máy cài broker.

### 4.4. Dùng broker khác (ví dụ broker chung của lớp)

Thông tin broker đọc từ biến môi trường, không cần sửa code:

```powershell
$env:AMQP_HOST = "192.168.1.10"      # cmd: set AMQP_HOST=192.168.1.10
$env:AMQP_PORT = "5672"
$env:AMQP_USER = "student"
$env:AMQP_PASSWORD = "matkhau"
```

---

## 5. Cài môi trường Python

Tại thư mục `amqp_protocol`:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

**Mỗi terminal mới đều phải chạy `.venv\Scripts\activate`** (đầu dòng hiện `(.venv)`), nếu không sẽ gặp lỗi `No module named 'pika'`. Hoặc gọi thẳng `.venv\Scripts\python` thay cho `python`.

Họ tên và MSSV dùng trong Bài 1 nằm ở `common/config.py` (`STUDENT_NAME`, `STUDENT_ID`).

## 6. Cách chạy

Mỗi chương trình chạy trong một terminal riêng (đã activate venv, đứng ở thư mục `amqp_protocol`). Khác MQTT, **thứ tự chạy Bài 1 và 2 không quan trọng**: queue giữ message cho tới khi consumer lấy. Ở Bài 3, consumer tạo queue và binding nên **chạy consumer trước** (ít nhất một lần).

```powershell
# Bài 1
python bai1_queue/consumer_bai1.py                     # Ctrl+C để dừng
python bai1_queue/producer_bai1.py                     # gửi 1 message; -n 5 để gửi 5 message

# Bài 2
python bai2_sensor/monitor_consumer_bai2.py
python bai2_sensor/sensor_producer_bai2.py             # gửi mỗi 3 giây; thêm "sensor01 sensor02 sensor03" cho nhiều cảm biến

# Bài 3
python bai3_alert_exchange/warning_consumer_bai3.py
python bai3_alert_exchange/critical_consumer_bai3.py
python bai3_alert_exchange/all_alert_consumer_bai3.py  # (mở rộng) nhận tất cả cảnh báo
python bai3_alert_exchange/alert_producer_bai3.py      # gửi 3 cảnh báo mẫu info/warning/critical
python bai3_alert_exchange/alert_producer_bai3.py critical "Mat dien phong server"   # gửi 1 cảnh báo tự nhập
```

## 7. Cách kiểm tra

- Giao diện web http://localhost:15672: tab **Queues and Streams** xem số message đang chờ (*Ready*) và chưa ack (*Unacked*) của từng queue; tab **Exchanges** → `iot_alert_exchange` → mục *Bindings* xem queue nào bind bằng routing key nào.
- Dừng consumer rồi chạy producer: số *Ready* của queue tăng; bật lại consumer thì message được giao và số này về 0.
- Gửi tay một message: trong giao diện web, mở queue (ví dụ `sensor_data_queue`) → mục **Publish message** → nhập payload JSON → *Publish message*.
- Các ca test chi tiết và kết quả: [docs/ket-qua-kiem-thu.md](docs/ket-qua-kiem-thu.md).

## 8. Kết quả đạt được

- **Bài 1:** producer gửi `Xin chao tu ung dung Python AMQP - B23DCCN227 - Tran Dang Duong` vào `iot_lab_queue`; consumer in `Da nhan message: ...` và `Thoi gian nhan: HH:MM:SS`. Message gửi khi consumer chưa chạy vẫn được giữ trong queue và giao khi consumer bật lên. Hỗ trợ gửi nhiều message, consumer chạy đến khi Ctrl+C.
- **Bài 2:** sensor gửi JSON `{device_id, temperature, humidity, timestamp}` vào `sensor_data_queue` mỗi 3 giây; monitor in dữ liệu và cảnh báo `CANH BAO: Nhiet do cao` (> 35), `CANH BAO: Do am thap` (< 40); payload hỏng bị loại bỏ mà không làm dừng monitor; hỗ trợ nhiều cảm biến.
- **Bài 3:** tạo được direct exchange `iot_alert_exchange`; `warning_queue` chỉ nhận cảnh báo `warning`, `critical_queue` chỉ nhận `critical`, cảnh báo `info` không queue nào nhận (producer báo rõ). Mở rộng: consumer nhận tất cả cảnh báo qua topic exchange (binding `alert.#`) và ghi log cảnh báo ra file `bai3_alert_exchange/alerts.log`.
- Tất cả chương trình báo lỗi rõ ràng khi không kết nối được broker hoặc mất kết nối giữa chừng, và thoát gọn khi nhấn Ctrl+C.

## 9. Xử lý sự cố

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `LOI: Khong ket noi duoc RabbitMQ tai 127.0.0.1:5672` | Docker Desktop hoặc container chưa chạy | Mở Docker Desktop, `docker compose up -d`, chờ khoảng 10 giây |
| `LOI: ... tu choi tai khoan 'guest'` | Sai user/password | Kiểm tra biến `AMQP_USER` / `AMQP_PASSWORD` |
| `No module named 'pika'` | Chưa activate venv hoặc chưa cài thư viện | `.venv\Scripts\activate` rồi `pip install -r requirements.txt` |
| `PRECONDITION_FAILED - inequivalent arg 'durable'` | Queue/exchange đã tồn tại với tham số khác (thường do chạy code mẫu khác cùng tên queue) | Xóa queue/exchange đó trong giao diện web, hoặc `docker compose down -v` rồi `up -d` |
| `Feature transient_nonexcl_queues is deprecated` | Code tạo queue không có `durable=True` (mặc định của pika), RabbitMQ 4.x không cho phép | Khai báo queue với `durable=True` (dùng `config.declare_queue`) |
| Bài 3: producer báo `warning`/`critical` không có queue nào nhận | Consumer chưa từng chạy nên queue và binding chưa được tạo | Chạy `warning_consumer_bai3.py` / `critical_consumer_bai3.py` trước |
| Consumer nhận message cũ ngay khi bật | Message gửi lúc consumer tắt được queue giữ lại (đúng thiết kế AMQP) | Xóa message cũ: giao diện web → queue → *Purge Messages* |
| Hai consumer cùng queue, mỗi bên chỉ nhận một phần | Nhiều consumer trên một queue sẽ chia nhau message (round-robin) | Đúng thiết kế; muốn mỗi bên nhận đủ thì mỗi bên một queue riêng bind cùng exchange |

## 10. Tài liệu kèm theo

- [docs/de-bai.md](docs/de-bai.md) – đề bài.
- [docs/kien-thuc-amqp.md](docs/kien-thuc-amqp.md) – kiến thức AMQP, RabbitMQ, pika áp dụng trong lab (có so sánh với MQTT).
- [docs/ket-qua-kiem-thu.md](docs/ket-qua-kiem-thu.md) – checklist đối chiếu đề và kết quả kiểm thử.
- [docs/huong-dan-tu-lam-lai.md](docs/huong-dan-tu-lam-lai.md) – các bước tự làm lại một bài AMQP tương tự.
