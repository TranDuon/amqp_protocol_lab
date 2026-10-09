# Bài 2 – Mô phỏng cảm biến IoT gửi dữ liệu môi trường

Thiết bị IoT giả lập gửi **telemetry** định kỳ dạng **JSON** vào queue; chương trình giám sát nhận, phân tích và **cảnh báo** khi vượt ngưỡng.

| | |
|---|---|
| Queue | `sensor_data_queue` (durable), gửi qua default exchange |
| Payload | `{"device_id": "sensor01", "temperature": 29.5, "humidity": 62.1, "timestamp": "2026-10-09T10:54:35"}` |
| Chu kỳ | 3 giây |
| Ngưỡng | Nhiệt độ **> 35** → `CANH BAO: Nhiet do cao`; độ ẩm **< 40** → `CANH BAO: Do am thap` |

## Chương trình

| File | Vai trò | Làm gì |
|---|---|---|
| `sensor_producer_bai2.py` | Producer định kỳ | Mỗi 3 giây sinh nhiệt độ 25–40, độ ẩm 30–80 (khoảng vượt qua ngưỡng để thấy cảnh báo), thêm `timestamp` (thời gian gửi), gửi JSON vào queue. Nhận nhiều `device_id` để mô phỏng nhiều cảm biến. |
| `monitor_consumer_bai2.py` | Consumer + xử lý | Lấy message từ queue, `json.loads`, in Device/Temperature/Humidity (và thời gian gửi nếu có), kiểm tra 2 ngưỡng **độc lập**, ack. Payload hỏng thì in lỗi và loại bỏ. |

## Luồng chạy

```
sensor:   dict ─json.dumps─▶ chuỗi JSON ─basic_publish(exchange="", rk="sensor_data_queue")─▶ [sensor_data_queue]
          connection.sleep(3) rồi lặp lại
                                                                      │
monitor:  on_message ◀───────────────────── broker giao ──────────────┘
          json.loads ─▶ in dữ liệu ─▶ check_alerts() ─▶ basic_ack
          (payload hỏng ─▶ basic_nack(requeue=False): loại bỏ)
```

- **Nhiều cảm biến dùng chung một queue**; consumer phân biệt bằng trường `device_id` trong payload (khác MQTT, nơi mỗi cảm biến có topic riêng).
- Producer chờ bằng `connection.sleep()` thay vì `time.sleep()`: pika `BlockingConnection` chỉ xử lý mạng (heartbeat) khi được gọi, `connection.sleep()` vừa chờ vừa xử lý.
- Phân tích JSON nằm trong `try/except`: exception lọt ra khỏi callback sẽ làm `start_consuming()` dừng.
- Payload hỏng dùng `basic_nack(requeue=False)`: nếu `requeue=True`, message hỏng quay lại queue, consumer nhận lại và lỗi mãi (*poison message*).

## Cách chạy

```powershell
python bai2_sensor/monitor_consumer_bai2.py                        # terminal 1
python bai2_sensor/sensor_producer_bai2.py                         # terminal 2 – Ctrl+C để dừng
python bai2_sensor/sensor_producer_bai2.py sensor01 sensor02 sensor03   # mở rộng: 3 cảm biến
```

Tùy chọn producer: `-i` chu kỳ gửi (giây).

## Output thực tế

Sensor producer:

```
Da ket noi RabbitMQ 127.0.0.1:5672
Gui du lieu cua sensor01 vao queue sensor_data_queue moi 3 giay (Ctrl+C de dung)

[10:54:35] sensor_data_queue <- {"device_id": "sensor01", "temperature": 37.0, "humidity": 62.2, "timestamp": "2026-10-09T10:54:35"}
[10:54:38] sensor_data_queue <- {"device_id": "sensor01", "temperature": 35.2, "humidity": 55.1, "timestamp": "2026-10-09T10:54:38"}
[10:54:41] sensor_data_queue <- {"device_id": "sensor01", "temperature": 39.2, "humidity": 45.7, "timestamp": "2026-10-09T10:54:41"}
Da dung sensor producer.
```

Monitor consumer (các bản ghi sau dòng `10:54:41` được gửi tay để kiểm tra ngưỡng và payload lỗi):

```
Da ket noi RabbitMQ 127.0.0.1:5672
Dang giam sat queue sensor_data_queue ... (Ctrl+C de thoat)

Device: sensor01
Temperature: 37.0
Humidity: 62.2
Thoi gian gui: 2026-10-09T10:54:35
CANH BAO: Nhiet do cao

Device: sensor01
Temperature: 35.2
Humidity: 55.1
Thoi gian gui: 2026-10-09T10:54:38
CANH BAO: Nhiet do cao

Device: sensor01
Temperature: 39.2
Humidity: 45.7
Thoi gian gui: 2026-10-09T10:54:41
CANH BAO: Nhiet do cao

Device: sensor01
Temperature: 36.4
Humidity: 38.9
CANH BAO: Nhiet do cao
CANH BAO: Do am thap

Device: sensor01
Temperature: 35.0
Humidity: 40.0

LOI: Payload khong hop le, bo qua: b'du lieu loi'

LOI: Payload khong hop le, bo qua: b'{"device_id":"sensor01","temperature":30}'

Device: sensor01
Temperature: 20.5
Humidity: 39.9
CANH BAO: Do am thap
```

## Đối chiếu tiêu chí đánh giá

- [x] Payload đúng định dạng JSON với `device_id`, `temperature`, `humidity`
- [x] Gửi dữ liệu tuần hoàn mỗi 3 giây
- [x] Consumer xử lý đúng và phát hiện cảnh báo (đúng ngưỡng 35.0 / 40.0 thì không cảnh báo)
- [x] Mở rộng: bổ sung trường thời gian gửi `timestamp`
- [x] Mở rộng: mô phỏng 2–3 cảm biến (`sensor01 sensor02 sensor03`)
