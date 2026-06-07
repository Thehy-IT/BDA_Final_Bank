# Báo Cáo Chuyên Sâu: Hệ Thống Phát Hiện Gian Lận Giao Dịch Ngân Hàng Thời Gian Thực (Real-time Banking Transaction Fraud Detection System)

## 1. Tổng Quan Dự Án
Dự án là một hệ thống đường ống dữ liệu (data pipeline) hoàn chỉnh, được thiết kế để phân tích và phát hiện các hành vi gian lận trong giao dịch ngân hàng theo thời gian thực. Bằng việc kết hợp kiến trúc hướng sự kiện (Event-driven Architecture) với các hệ thống phân tích luồng dữ liệu (Stream Processing), dự án mô phỏng quá trình tiếp nhận lượng lớn giao dịch, áp dụng các luật nghiệp vụ khắt khe, và đưa ra cảnh báo ngay tức thời đối với các giao dịch có độ rủi ro cao.

Dự án mang tính ứng dụng thực tiễn lớn trong lĩnh vực công nghệ tài chính (Fintech), giúp giảm thiểu rủi ro tài chính cho ngân hàng và tăng cường bảo mật cho khách hàng.

## 2. Kiến Trúc Hệ Thống (System Architecture)
Hệ thống được xây dựng theo mô hình Microservices phân tán, với luồng dữ liệu (Data Flow) đi qua các thành phần cốt lõi:

*   **Producer (Trình sinh dữ liệu)**: Khởi tạo các giao dịch ngân hàng giả lập liên tục (Transactions). Có khả năng sinh ra các mẫu dữ liệu bình thường cũng như cố ý tạo ra các mẫu dữ liệu dị thường (anomalies) để kiểm thử hệ thống.
*   **Message Broker (Apache Kafka)**: Xương sống của hệ thống, tiếp nhận và luân chuyển hàng nghìn giao dịch mỗi giây. Dữ liệu từ Producer sẽ được đẩy vào topic `transactions`, và các cảnh báo sau xử lý được đưa vào topic `fraud-alerts`.
*   **Stream Processor (Engine xử lý dữ liệu)**: Lõi phân tích của hệ thống được viết bằng Python (hỗ trợ micro-batch và có phiên bản mở rộng PySpark). Component này lắng nghe topic `transactions`, trích xuất thông tin, và chạy đối chiếu với hệ thống điểm rủi ro.
*   **Fraud State Store (Redis)**: Đóng vai trò làm bộ nhớ đệm tốc độ cao (In-memory Data Store). Redis được sử dụng chuyên biệt để xử lý các luật phát hiện gian lận dựa trên trạng thái (Stateful Rules) như đếm số lượng giao dịch của một tài khoản trong thời gian ngắn hoặc theo dõi vị trí địa lý cuối cùng.
*   **Dashboard (Hệ thống giám sát)**: Cung cấp giao diện trực quan cho quản trị viên. Dashboard kết nối với Kafka qua Backend FastAPI và luân chuyển dữ liệu hiển thị lên Frontend thông qua giao thức Server-Sent Events (SSE).

## 3. Hệ Thống Quy Tắc Phát Hiện Gian Lận (Fraud Detection Rules)
Hệ thống tính toán "Điểm rủi ro" (Risk Score) thông qua việc kết hợp nhiều quy tắc nghiệp vụ khác nhau. Điểm số được cộng dồn nếu giao dịch vi phạm nhiều luật, và dựa vào mức điểm cuối cùng, hệ thống sẽ phân loại theo các mức độ: **LOW**, **MEDIUM**, **HIGH**, hoặc **CRITICAL**.

Các quy tắc được chia làm 2 nhóm chính:

### 3.1. Các Quy Tắc Phi Trạng Thái (Stateless Rules)
Đây là các quy tắc có thể đánh giá ngay trên một bản ghi giao dịch đơn lẻ mà không cần thông tin lịch sử:
*   `HIGH_AMOUNT`: Cảnh báo giao dịch có số tiền quá lớn (Lớn hơn $5,000 cảnh báo mức HIGH; lớn hơn $20,000 cảnh báo mức CRITICAL).
*   `CARD_NOT_PRESENT_HIGH`: Các giao dịch trực tuyến (không sử dụng thẻ vật lý) nhưng có số tiền lớn (≥ $2,000).
*   `ODD_HOURS`: Giao dịch diễn ra vào các khung giờ nhạy cảm (từ 02:00 sáng đến 05:00 sáng theo giờ UTC).
*   `ROUND_NUMBER`: Giao dịch có con số làm tròn tuyệt đối và lớn (ví dụ: $1,000, $5,000, $10,000) thường liên quan tới các giao dịch rửa tiền hoặc rút vốn.
*   `HIGH_RISK_MERCHANT`: Giao dịch liên quan tới các nhóm danh mục đối tác rủi ro cao như: chuyển tiền quốc tế (wire_transfer), tiền ảo (crypto), cờ bạc (gambling), hoặc đối tác không xác định.

### 3.2. Các Quy Tắc Có Trạng Thái (Stateful Rules)
Đòi hỏi việc truy vấn dữ liệu lịch sử từ bộ nhớ Redis:
*   `VELOCITY` (Tần suất giao dịch): Phát hiện khi một tài khoản phát sinh nhiều hơn 6 giao dịch trong vòng 10 phút.
*   `GEO_VELOCITY` (Di chuyển phi thực tế - Impossible Travel): Đo lường sự thay đổi toạ độ địa lý qua các giao dịch của cùng một thẻ. Cảnh báo mức **CRITICAL** nếu khoảng cách chênh lệch trên 400km nhưng thời gian thực hiện lại nhỏ hơn 30 phút (tốc độ vượt quá khả năng di chuyển vật lý thông thường).

## 4. Công Nghệ Sử Dụng (Technology Stack)
*   **Cơ sở hạ tầng & Truyền tải sự kiện:** Apache Kafka 3.6 (chạy ở chế độ KRaft, không phụ thuộc vào ZooKeeper).
*   **Xử lý dữ liệu luồng (Stream Processing):** Python Micro-batching và PySpark Structured Streaming (dành cho mô hình cụm phân tán).
*   **Lưu trữ trạng thái (State Store):** Redis 7 Alpine.
*   **Web Backend & Streaming UI:** FastAPI, Server-Sent Events (SSE) để truyền luồng thời gian thực, Vanilla JS và Chart.js cho hiển thị biểu đồ.
*   **Triển khai (Deployment):** Docker & Docker Compose giúp khởi chạy và đóng gói toàn bộ hệ thống độc lập.

## 5. Đánh Giá Khả Năng Mở Rộng & Định Hướng Tương Lai
Hệ thống sở hữu tính decoupling (giảm sự phụ thuộc lẫn nhau) rất tốt giữa các thành phần. Nhờ vào việc sử dụng Kafka, hệ thống dễ dàng đáp ứng khả năng thu phóng tĩnh (Horizontal Scaling). Các Worker xử lý dữ liệu (Processor) có thể được tăng cường số lượng dựa trên cấu hình nhóm tiêu dùng (Consumer Group) của Kafka để chịu tải hàng triệu giao dịch mỗi ngày.

Ngoài ra, với việc hỗ trợ phiên bản chạy qua PySpark, dự án hoàn toàn đủ nền tảng để triển khai thực tế trên các môi trường dữ liệu lớn (Big Data Clusters) như Hadoop/YARN hoặc Databricks. Việc kết hợp thêm Machine Learning Models thay thế dần các luật tĩnh (Rule-based) cũng là hướng đi tối ưu để nâng cấp mức độ tinh vi của hệ thống phát hiện gian lận trong tương lai.