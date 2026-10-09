# Bài 3 – Điều phối cảnh báo IoT với exchange

Làm quen với **exchange** và **routing key**: producer gửi cảnh báo kèm mức độ, exchange tự chuyển mỗi cảnh báo tới đúng nơi xử lý.

| Thành phần | Tên | Ghi chú |
|---|---|---|
| Exchange | `iot_alert_exchange` | **direct**, durable |
| Routing key | `info`, `warning`, `critical` | = mức cảnh báo |
| Queue + binding | `warning_queue` ← `warning` | tạo bởi `warning_consumer_bai3.py` |
| Queue + binding | `critical_queue` ← `critical` | tạo bởi `critical_consumer_bai3.py` |
| Mở rộng | `iot_alert_topic_exchange` (**topic**), `all_alerts_queue` ← `alert.#` | tạo bởi `all_alert_consumer_bai3.py` |

## Chương trình

| File | Vai trò | Làm gì |
|---|---|---|
| `alert_producer_bai3.py` | Producer | Tạo exchange, gửi cảnh báo với routing key = mức. Không tham số: gửi 3 cảnh báo mẫu info/warning/critical. Có tham số: gửi 1 cảnh báo tự nhập. In cho biết mỗi cảnh báo có vào queue nào không. |
| `warning_consumer_bai3.py` | Consumer | Tạo `warning_queue`, bind với routing key `warning`, in `[warning_queue] Da nhan: ...` |
| `critical_consumer_bai3.py` | Consumer | Tạo `critical_queue`, bind với routing key `critical`, in `[critical_queue] Da nhan: ...` |
| `all_alert_consumer_bai3.py` | Consumer (mở rộng) | Tạo `all_alerts_queue`, bind với topic exchange bằng `alert.#`, nhận **tất cả** cảnh báo |

Mọi consumer của bài này ghi thêm cảnh báo vào file `alerts.log` trong folder này (mở rộng "ghi log cảnh báo ra file").

## Luồng chạy

```
alert_producer ── basic_publish(iot_alert_exchange, rk=level) ──▶ iot_alert_exchange (direct)
                                                                    ├─ binding "warning"  ─▶ warning_queue  ─▶ warning_consumer
                                                                    ├─ binding "critical" ─▶ critical_queue ─▶ critical_consumer
                                                                    └─ "info": không binding nào khớp → message bị bỏ
               ── basic_publish(iot_alert_topic_exchange, rk=alert.<level>) ──▶ topic exchange
                                                                    └─ binding "alert.#" ─▶ all_alerts_queue ─▶ all_alert_consumer
```

- Producer **không biết** có những queue nào; nó chỉ gửi vào exchange kèm routing key. Mỗi consumer tự tạo queue và **bind** queue vào exchange bằng binding key.
- **Direct exchange**: message vào queue có binding key **trùng chính xác** routing key. Không ai bind `info` nên cảnh báo `info` bị bỏ.
- Producer gửi với `mandatory=True` và bật publisher confirm: khi không có queue nào khớp, broker trả message về (pika báo `UnroutableError`), producer in "khong co queue nao nhan". Nhờ vậy thấy được hành vi định tuyến ngay trên màn hình.
- **Topic exchange** (mở rộng): routing key gồm các từ ngăn cách bằng dấu chấm; trong binding, `*` thay đúng một từ, `#` thay 0 hoặc nhiều từ. `alert.#` khớp `alert.info`, `alert.warning`, `alert.critical`. (Exchange chính bắt buộc là direct, mà direct không hỗ trợ ký tự đại diện, nên phần mở rộng dùng một topic exchange riêng.)
- Queue durable nên khi consumer tắt, cảnh báo vẫn được giữ trong queue và giao khi consumer chạy lại.

## Cách chạy

**Chạy các consumer trước** (chúng tạo queue và binding):

```powershell
python bai3_alert_exchange/warning_consumer_bai3.py       # terminal 1
python bai3_alert_exchange/critical_consumer_bai3.py      # terminal 2
python bai3_alert_exchange/all_alert_consumer_bai3.py     # terminal 3 (mở rộng, tùy chọn)
python bai3_alert_exchange/alert_producer_bai3.py         # terminal 4: gửi 3 cảnh báo mẫu
```

Gửi một cảnh báo tự nhập:

```powershell
python bai3_alert_exchange/alert_producer_bai3.py critical "Mat dien phong server"
python bai3_alert_exchange/alert_producer_bai3.py warning "Do am kho lanh giam"
```

## Output thực tế

Producer (không tham số, rồi một cảnh báo tự nhập):

```
Da ket noi RabbitMQ 127.0.0.1:5672

[info] Da gui toi iot_alert_exchange: Nhiet do phong may on dinh info
        -> khong co queue nao nhan (khong co binding cho routing key nay)
[warning] Da gui toi iot_alert_exchange: Nhiet do phong may vuot nguong warning
        -> da vao queue
[critical] Da gui toi iot_alert_exchange: Cam bien kho lanh mat ket noi critical
        -> da vao queue

Da ket noi RabbitMQ 127.0.0.1:5672

[critical] Da gui toi iot_alert_exchange: Mat dien phong server
        -> da vao queue
```

Warning consumer – chỉ nhận mức warning:

```
Da ket noi RabbitMQ 127.0.0.1:5672
warning_queue <- iot_alert_exchange (routing key 'warning') ... (Ctrl+C de thoat)

[warning_queue] Da nhan: Nhiet do phong may vuot nguong warning
```

Critical consumer – chỉ nhận mức critical:

```
Da ket noi RabbitMQ 127.0.0.1:5672
critical_queue <- iot_alert_exchange (routing key 'critical') ... (Ctrl+C de thoat)

[critical_queue] Da nhan: Cam bien kho lanh mat ket noi critical
[critical_queue] Da nhan: Mat dien phong server
```

All alert consumer (mở rộng) – nhận tất cả, kể cả info:

```
Da ket noi RabbitMQ 127.0.0.1:5672
all_alerts_queue <- iot_alert_topic_exchange (binding 'alert.#') ... (Ctrl+C de thoat)

[all_alerts_queue] (alert.info) Da nhan: Nhiet do phong may on dinh info
[all_alerts_queue] (alert.warning) Da nhan: Nhiet do phong may vuot nguong warning
[all_alerts_queue] (alert.critical) Da nhan: Cam bien kho lanh mat ket noi critical
[all_alerts_queue] (alert.critical) Da nhan: Mat dien phong server
```

File `alerts.log`:

```
2026-10-09 10:56:08 [all_alerts_queue] [alert.info] Nhiet do phong may on dinh info
2026-10-09 10:56:08 [warning_queue] [warning] Nhiet do phong may vuot nguong warning
2026-10-09 10:56:08 [all_alerts_queue] [alert.warning] Nhiet do phong may vuot nguong warning
2026-10-09 10:56:08 [critical_queue] [critical] Cam bien kho lanh mat ket noi critical
...
```

Exchange và binding trên broker (giao diện web hoặc HTTP API):

```
iot_alert_exchange         type=direct
iot_alert_topic_exchange   type=topic
iot_alert_exchange         --[critical]--> critical_queue
iot_alert_exchange         --[warning]--> warning_queue
iot_alert_topic_exchange   --[alert.#]--> all_alerts_queue
```

## Đối chiếu tiêu chí đánh giá

- [x] Tạo được direct exchange `iot_alert_exchange`
- [x] Bind queue đúng routing key (`warning_queue` ← `warning`, `critical_queue` ← `critical`)
- [x] Message được phân phối đúng nơi nhận: info → không queue nào, warning → chỉ `warning_queue`, critical → chỉ `critical_queue`
- [x] Mở rộng: consumer nhận tất cả cảnh báo bằng topic exchange
- [x] Mở rộng: ghi log cảnh báo ra file `alerts.log`
