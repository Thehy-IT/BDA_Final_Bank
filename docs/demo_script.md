# 🎓 Kịch Bản Demo & Hướng Dẫn Bảo Vệ Đồ Án
**Đồ án:** Hệ Thống Phát Hiện Gian Lận Giao Dịch Ngân Hàng Thời Gian Thực (Real-time Banking Transaction Anomaly Pipeline)

Tài liệu này được thiết kế để hướng dẫn bạn từng bước chuẩn bị hệ thống, trình bày kịch bản demo trực quan và chuẩn bị các câu hỏi phản biện thường gặp của Giảng viên khi bảo vệ đồ án/bài tập lớn.

---

## 📋 1. Chuẩn Bị Trước Demo (Setup Checklist)

Trước khi bắt đầu buổi bảo vệ, hãy đảm bảo hệ thống đã được cài đặt và ở trạng thái "sạch" để tránh lỗi xung đột dữ liệu cũ.

### Các bước thực hiện:

1. **Khởi động Docker Desktop**: Đảm bảo Docker đang chạy trên máy của bạn.
2. **Dọn dẹp tài nguyên cũ (Reset)**: Mở Terminal tại thư mục gốc của dự án và chạy lệnh sau để xóa các container, network và dữ liệu cũ trong volume (tránh nghẽn hàng đợi Kafka hoặc tràn Redis từ các lần chạy trước):
   ```bash
   make clean
   ```
3. **Khởi động hệ thống mới**: Chạy lệnh dưới đây để tự động build và chạy toàn bộ dịch vụ:
   ```bash
   make up
   ```
4. **Chuẩn bị sẵn các Tab trên trình duyệt và Terminal**:
   - **Tab Trình duyệt 1**: Mở Dashboard giám sát tại địa chỉ 👉 **[http://localhost:8080](http://localhost:8080)**.
   - **Tab Terminal 1**: Dùng để theo dõi luồng logs thời gian thực từ các container:
     ```bash
     make logs
     ```
   - **Tab Terminal 2**: Chuẩn bị sẵn để kết nối trực tiếp vào luồng tin nhắn cảnh báo trong Kafka khi giảng viên yêu cầu xem dữ liệu thô:
     ```bash
     make tail-alerts
     ```

---

## ⏱️ 2. Kịch Bản Trình Bày Từng Bước (Step-by-Step Demo Flow)

Hãy tuân theo kịch bản dưới đây để dẫn dắt giảng viên đi từ tổng quan kiến trúc đến các tính năng kỹ thuật chi tiết.

### 🎬 Bước 1: Giới thiệu Kiến trúc & Luồng dữ liệu (2 phút)
> **Hành động**: Chiếu slide kiến trúc hoặc mở phần `README.md` có hình vẽ sơ đồ Mermaid.
* **Lời thoại đề xuất**:
  > *"Kính thưa thầy/cô, hệ thống phát hiện gian lận giao dịch của em được thiết kế theo kiến trúc hướng sự kiện (Event-driven Architecture) bao gồm 5 thành phần cốt lõi:*
  > 1. ***Producer**: Giả lập luồng giao dịch ngân hàng thực tế gửi lên liên tục.*
  > 2. ***Apache Kafka**: Đóng vai trò Broker trung chuyển tin nhắn có khả năng chịu tải và kháng lỗi cao.*
  > 3. ***Processor**: Bộ xử lý luồng (micro-batching) quét dữ liệu sau mỗi 1 giây để đối chiếu các quy tắc gian lận.*
  > 4. ***Redis**: State Store tốc độ cao giúp lưu trữ trạng thái giao dịch thời gian thực phục vụ cho các luật có trạng thái (stateful).*
  > 5. ***Dashboard**: Backend FastAPI kết nối Kafka đẩy dữ liệu thời gian thực lên giao diện HTML thông qua giao thức Server-Sent Events (SSE).*
  > *Bây giờ, em xin phép demo trực tiếp luồng hoạt động của hệ thống."*

### 📊 Bước 2: Demo trực quan Dashboard & Luồng logs (3 phút)
> **Hành động**: Mở màn hình Dashboard (`localhost:8080`) đang chạy luồng giao dịch và Terminal đang chạy `make logs`.
* **Lời thoại đề xuất**:
  > *"Như thầy/cô thấy trên màn hình, phía bên trái là cột **Live Transactions** hiển thị các giao dịch thông thường đang đổ về liên tục (với tốc độ mặc định 5 giao dịch/giây). Phía bên phải là cột **Fraud Alerts** hiển thị các cảnh báo gian lận màu đỏ/cam ngay khi hệ thống phát hiện được.*
  > *Đồng thời, trên màn hình Terminal logs, dịch vụ `processor` đang in ra các dòng thông tin chi tiết về điểm số rủi ro (Risk Score) và các quy tắc (rules) bị vi phạm như `HIGH_AMOUNT`, `ODD_HOURS`, hay `GEO_VELOCITY`."*

### 🔒 Bước 3: Giải thích các Quy tắc Phát hiện Gian lận (3 phút)
> **Hành động**: Cuộn xuống bảng phân tích biểu đồ trên Dashboard để giảng viên thấy biểu đồ phân bổ mức độ nghiêm trọng (Low, Medium, High, Critical) và các loại luật bị kích hoạt.
* **Lời thoại đề xuất**:
  > *"Hệ thống của em chia làm hai loại quy tắc phát hiện:*
  > 1. ***Quy tắc phi trạng thái (Stateless Rules)**: Đánh giá trực tiếp trên từng giao dịch riêng lẻ, ví dụ:*
  >    - *`HIGH_AMOUNT`: Giao dịch $\ge \$5,000$ (Cảnh báo HIGH) và $\ge \$20,000$ (Cảnh báo CRITICAL).*
  >    - *`ODD_HOURS`: Giao dịch vào giờ nhạy cảm đêm khuya từ 2h đến 5h sáng (giờ UTC).*
  >    - *`ROUND_NUMBER`: Số tiền chẵn tròn trăm hoặc tròn nghìn đô-la (ví dụ: $100, $500, $1,000, $5,000, $10,000) thường thấy trong các hành vi rửa tiền hoặc giao dịch bất thường.*
  >    - *`HIGH_RISK_MERCHANT`: Giao dịch qua ví điện tử lạ, sàn giao dịch crypto hoặc cờ bạc.*
  > 2. ***Quy tắc có trạng thái (Stateful Rules)**: Phải dựa vào lịch sử được lưu trữ tại bộ đệm Redis, ví dụ:*
  >    - *`VELOCITY`: Phát hiện một tài khoản phát sinh nhiều hơn 6 giao dịch trong vòng 10 phút. Em sử dụng cấu trúc dữ liệu **Redis Sorted Sets (ZSET)** để thực hiện việc này."*

### 🚀 Bước 4: Demo Case đặc biệt - Di chuyển phi vật lý (2 phút)
> **Hành động**: Chỉ trực tiếp vào một dòng cảnh báo có quy tắc `GEO_VELOCITY` màu đỏ trên Dashboard (hoặc chỉ vào log Terminal).
* **Lời thoại đề xuất**:
  > *"Em xin nhấn mạnh quy tắc **GEO_VELOCITY (Impossible Travel)**. Đây là một cảnh báo mức **CRITICAL** (điểm rủi ro 90) khi phát hiện một tài khoản có hai giao dịch liên tiếp cách nhau dưới 30 phút nhưng khoảng cách địa lý giữa 2 thành phố lại lớn hơn 400km (vượt quá giới hạn di chuyển vật lý của con người).*
  > *Thuật toán sử dụng công thức **Haversine** để tính khoảng cách bề mặt Trái Đất dựa vào kinh/vĩ độ (`latitude`/`longitude`) và lưu lại trạng thái giao dịch liền trước vào Redis với khóa `geo:<account_id>` cùng thời gian sống (TTL) 60 phút."*

### 📈 Bước 5: Giải thích khả năng mở rộng với PySpark (1 phút)
> **Hành động**: Mở tệp `processor/spark_detector.py` trên IDE để trình bày mã nguồn.
* **Lời thoại đề xuất**:
  > *"Để chuẩn bị cho môi trường sản xuất thực tế với dữ liệu cực lớn, dự án của em có đi kèm phiên bản viết bằng **PySpark Structured Streaming** trong tệp `spark_detector.py`. Tệp này cho phép triển khai ứng dụng trên các cụm máy chủ dữ liệu lớn (Big Data Cluster) để thực hiện tính toán song song, phân tán với độ trễ cực thấp."*

---

## 💡 3. Giải Thích Công Nghệ Cốt Lõi (Core Technical Pillars)

Hãy nắm chắc các giải pháp công nghệ dưới đây để trả lời trôi chảy khi giảng viên đi sâu vào chi tiết kỹ thuật:

```
+------------------+      (Transactions)      +-------------------+
|  Tx Producer     | -----------------------> | Apache Kafka      |
|  (Mô phỏng giao  |                          | (Broker chịu tải) |
|   dịch liên tục) |                          +-------------------+
+------------------+                                    |
                                                        v
+------------------+      (Fraud Alerts)      +-------------------+
| FastAPI Backend  | <----------------------- | Anomaly Processor |
| (Phục vụ SSE)    |                          | (Micro-batching   |
+------------------+                          |  & Risk Engine)   |
         |                                    +-------------------+
         | (Server-Sent Events)                         ^
         v                                              | (Đọc/Ghi trạng thái)
+------------------+                                    v
| Web Dashboard    |                          +-------------------+
| (Chart.js UI)    |                          | Redis 7 State     |
+------------------+                          | (Cửa sổ trượt)    |
                                              +-------------------+
```

1. **Apache Kafka (KRaft mode)**:
   - Tại sao không dùng ZooKeeper? KRaft (Kafka Raft Metadata mode) loại bỏ sự phụ thuộc vào cụm ZooKeeper bên ngoài, giúp quản lý metadata trực tiếp ngay trong Kafka, giảm tài nguyên hệ thống và cải thiện tốc độ khởi động, thu gọn cấu trúc vận hành.
   - Kafka topics được phân vùng như thế nào? Topic `transactions` có **4 partitions** để đảm bảo khả năng mở rộng song song (parallel processing). Topic `fraud-alerts` có **2 partitions**.
2. **Cửa sổ trượt Redis Sorted Sets (ZSET) cho luật `VELOCITY`**:
   - Khi có giao dịch mới, ta thêm một phần tử vào Sorted Set bằng lệnh `ZADD key timestamp transaction_id`.
   - Ta dọn dẹp các giao dịch cũ nằm ngoài cửa sổ 10 phút bằng lệnh `ZREMRANGEBYSCORE key 0 <timestamp_10m_ago>` (trong đó `<timestamp_10m_ago>` bằng `now_timestamp - 600`).
   - Ta lấy ra tổng số lượng giao dịch trong cửa sổ bằng lệnh `ZCARD key`.
   - Cơ chế này chạy cực nhanh nhờ vào độ phức tạp thuật toán thấp của cấu trúc dữ liệu SkipList trong Redis.
3. **Server-Sent Events (SSE)**:
   - Đây là công nghệ đẩy dữ liệu một chiều từ Server xuống Client (Unidirectional) qua kết nối HTTP liên tục. Nó tiết kiệm tài nguyên hệ thống hơn nhiều so với WebSockets (yêu cầu bắt tay 2 chiều phức tạp) và tối ưu hơn so với cơ chế Short Polling (gửi request liên tục lên server gây lãng phí băng thông).

---

## 🧠 4. Bộ Câu Hỏi Phản Biện Q&A Thường Gặp (Q&A Cheat Sheet)

Dưới đây là các câu hỏi "hóc búa" nhất mà các giảng viên thường đặt ra để kiểm tra mức độ tự làm và hiểu biết sâu của sinh viên.

### ❓ Câu 1: Tại sao em lại dùng Apache Kafka thay vì các hàng đợi tin nhắn nhẹ hơn như RabbitMQ hay ActiveMQ?
* **Cách trả lời**:
  > *"Thưa thầy/cô, RabbitMQ được tối ưu hóa cho việc phân phối tin nhắn phức tạp và định tuyến thông minh (Smart Broker - Dumb Consumer), phù hợp với kiến trúc microservices thông thường. Tuy nhiên, dự án này là hệ thống xử lý luồng dữ liệu lớn (Stream Processing) thời gian thực.*
  > *Apache Kafka được chọn vì những lý do sau:*
  > 1. *Kafka là một phân tán commit log có tính năng lưu trữ tin nhắn bền vững trên đĩa cứng (message persistence), cho phép xem lại dữ liệu cũ khi hệ thống bị lỗi (replayability).*
  > 2. *Kafka quản lý theo cơ chế partition, giúp phân chia tải cực tốt cho nhiều consumer xử lý cùng lúc (Dumb Broker - Smart Consumer), rất thích hợp để scale khi thông lượng giao dịch tăng lên hàng triệu giao dịch mỗi giây.*
  > 3. *Kafka có tích hợp tự nhiên cực tốt với hệ sinh thái Big Data như Spark, Flink.*

### ❓ Câu 2: Tại sao em lại dùng Redis để lưu trữ trạng thái mà không dùng cơ sở dữ liệu quan hệ (SQL) như PostgreSQL hay MySQL?
* **Cách trả lời**:
  > *"Lý do cốt lõi là **yêu cầu về độ trễ cực thấp (Ultra-low Latency)** của bài toán phát hiện gian lận ngân hàng. Mỗi giao dịch cần được phân tích và đưa ra cảnh báo trong vòng vài mili-giây trước khi giao dịch đó được phê duyệt.*
  > *Nếu sử dụng PostgreSQL hay MySQL để truy vấn và đếm số lượng giao dịch trong 10 phút hoặc tính toán khoảng cách tọa độ, hệ thống sẽ phải thực hiện các phép đọc/ghi trực tiếp xuống đĩa cứng (Disk I/O) và chạy các câu lệnh `COUNT` hoặc `JOIN` phức tạp. Khi có hàng nghìn giao dịch đổ về mỗi giây, cơ sở dữ liệu SQL sẽ lập tức bị nghẽn cổ chai.*
  > *Redis hoạt động hoàn toàn trên bộ nhớ RAM (In-Memory), hỗ trợ các cấu trúc dữ liệu tối ưu như Sorted Sets giúp thực hiện phép thêm và dọn dẹp cửa sổ trượt chỉ mất thời gian độ phức tạp $O(\log(N) + M)$ (khoảng dưới $1$ mili-giây), đảm bảo hệ thống không bao giờ bị trễ luồng xử lý."*

### ❓ Câu 3: Làm thế nào để mở rộng (Scale) hệ thống này khi lưu lượng giao dịch tăng lên gấp 100 lần trong thực tế?
* **Cách trả lời**:
  > *"Để mở rộng hệ thống chịu tải lớn, em sẽ áp dụng các giải pháp sau:*
  > 1. ***Về mặt Kafka**: Tăng số lượng partitions của topic `transactions` lên nhiều hơn (ví dụ 16 hoặc 32 partitions) để tăng khả năng song song hóa dữ liệu đầu vào.*
  > 2. ***Về mặt Processor**: Chạy nhiều container của dịch vụ `processor` cùng chung một Consumer Group. Kafka sẽ tự động cân bằng tải, chia các partitions cho từng consumer xử lý độc lập.*
  > 3. ***Về mặt State Store**: Chuyển đổi Redis đơn lẻ (Standalone) sang mô hình **Redis Cluster** phân tán để chia nhỏ dữ liệu lưu khóa (`vel` và `geo`) sang nhiều node vật lý khác nhau.*
  > 4. ***Về mặt Processing Engine**: Thay thế bộ xử lý Python Core mặc định bằng bộ xử lý **PySpark Structured Streaming** (mã nguồn có sẵn tại `spark_detector.py`) chạy trên cụm Spark để tận dụng tối đa sức mạnh của hàng trăm máy chủ trong cụm."*

### ❓ Câu 4: Em nhận thấy hệ thống này có những điểm yếu gì về mặt bảo mật không? Nếu có thì khắc phục thế nào?
* **Cách trả lời**:
  > *"Dạ có, qua tự đánh giá chất lượng (Security Audit), em phát hiện hệ thống hiện tại đang được cấu hình ở chế độ phát triển nhanh nên còn các điểm yếu bảo mật sau:*
  > 1. *Redis đang chạy không có mật khẩu xác thực và mở cổng 6379 ra ngoài.*
  > 2. *Kafka đang giao tiếp qua kênh không mã hóa PLAINTEXT.*
  > *Để khắc phục khi đưa lên môi trường thật (Production), em sẽ:*
  > - *Cài đặt cấu hình `--requirepass` cho Redis và loại bỏ phần khai báo cổng `ports: ["6379:6379"]` trong `docker-compose.yml` để các dịch vụ chỉ kết nối nội bộ trong mạng ảo Docker Network.*
  > - *Thay thế cổng Kafka `9092/9094` bằng cơ chế mã hóa **SASL_SSL** để đảm bảo dữ liệu trên đường truyền không bị nghe trộm, đồng thời cài đặt mật khẩu xác thực người dùng kết nối tới Kafka."*

### ❓ Câu 5: Sự khác biệt lớn nhất giữa tệp xử lý Python (`anomaly_detector.py`) và tệp Spark (`spark_detector.py`) trong dự án của em là gì?
* **Cách trả lời**:
  > *"Sự khác biệt nằm ở **Mô hình tính toán (Computational Model)**:*
  > - *Tệp `anomaly_detector.py` là một trình xử lý luồng đơn tiến trình (Single-process micro-batch). Nó phù hợp với các hệ thống vừa và nhỏ, rất dễ cấu hình, chạy nhanh không cần cấu trúc nền tảng cồng kềnh.*
  > - *Tệp `spark_detector.py` sử dụng công nghệ PySpark Structured Streaming. Nó chuyển đổi luồng dữ liệu thành một chuỗi các Spark DataFrame phân tán. Khi chạy trên cụm máy chủ, Spark sẽ tự động tối ưu hóa kế hoạch thực thi (Execution Plan), phân chia các phép toán phi trạng thái ra hàng chục Worker để tính toán song song, giúp xử lý lượng dữ liệu khổng lồ vượt quá giới hạn của một máy tính đơn lẻ."*

---
*Chúc bạn có một buổi trình bày xuất sắc và đạt điểm số tối đa!*
