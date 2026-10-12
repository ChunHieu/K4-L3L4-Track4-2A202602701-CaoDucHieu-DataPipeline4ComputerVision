# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Cao Đức Hiếu |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602701 |
| GitHub | ChunHieu |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-07304 |

## Kết quả cuối

- `dataset_version` của bạn: `51bc9b5836c0`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 726 / 159 / 134 (Tổng cộng 1019 khung hình)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt chính giữa telemetry nguồn A và nguồn B được ghi nhận ở tầng bronze:
1. Định dạng thời gian: Nguồn A sử dụng epoch milliseconds dạng số nguyên (`epoch_ms`), trong khi nguồn B sử dụng chuỗi ISO 8601 (`iso_time`).
2. Đơn vị đo vận tốc: Nguồn A ghi nhận bằng mét trên giây (`speed_mps`), còn nguồn B ghi nhận bằng dặm trên giờ (`speed_mph`).
3. Cấu trúc tọa độ địa lý: Nguồn A tách riêng hai trường số thực `lat` và `lon`, trong khi nguồn B dùng tên trường và định dạng chuỗi khác biệt.
4. Lệch đồng hồ (Clock drift): Nguồn B bị lệch đồng hồ nghiêm trọng so với thời gian quay video thực tế, trong khi nguồn A đồng bộ trực tiếp.
Pipeline giữ mỗi nguồn một bảng riêng ở tầng Bronze và lưu toàn bộ dưới dạng chuỗi (text/raw) để tuân thủ nguyên tắc schema-on-read của Medallion Lakehouse. Thiết kế này bảo vệ pipeline không bị gãy vỡ (crash) khi dữ liệu có dị thường hoặc schema drift, lưu vết trọn vẹn dữ liệu gốc để phục vụ kiểm toán (audit trail), và cho phép chuyển toàn bộ trách nhiệm chuẩn hóa, kiểm định chất lượng xuống tầng Silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn về chung một schema chuẩn bằng các bước: chuẩn hóa thời gian về `TIMESTAMPTZ` (UTC); đổi vận tốc về m/s (nguồn B nhân hệ số `0.44704`); và loại bỏ độ lệch đồng hồ của nguồn B bằng cách tính delta giữa timestamp telemetry đầu tiên với thời điểm bắt đầu video rồi trừ đi độ lệch đó. Bằng chứng kiểm tra bước 2 đạt tuyệt đối: test `drive B clock offset removed (first sample within 0.5 s of video start)` PASS với mốc `2025-10-08 14:30:00.009000+00:00`, kiểm tra tốc độ `0..70 m/s` PASS và không còn trùng lặp `(drive, ts)`.

Bảng quarantine thực tế:
| drive_id | duplicate | speed_missing | speed_out_of_range | Tổng rejects |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Tổng cộng 106 bản ghi bị cách ly. Các dòng lỗi được đưa vào bảng `rejects` thay vì xóa bỏ nhằm đảm bảo tính quan sát (observability), giúp đội ngũ kỹ sư phần cứng phân tích tỷ lệ hỏng cảm biến (sensor malfunction) và phục vụ việc truy vết nguyên nhân gốc rễ mà không làm thất thoát dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` chỉ ở mức `warn` vì việc mất tín hiệu ngắt quãng tạm thời (do xe đi qua hầm hoặc che khuất vệ tinh GPS) là hiện tượng vật lý bình thường, dữ liệu các đoạn còn lại vẫn có giá trị sử dụng. Ngược lại, `in_range` (0..70 m/s) bắt buộc là `error` vì vận tốc âm hoặc vượt ngưỡng vật lý xe hơi là dấu hiệu hỏng hóc dữ liệu nghiêm trọng, nếu đưa vào mô hình tự lái sẽ gây hậu quả khôn lường.
Trong thí nghiệm `break-gate` (`break_gate.json`), khi nới lỏng giới hạn trên lên 1000 m/s, test dbt `in_range_silver_telemetry_speed_mps__70__0` đã phát hiện chính xác 6 dòng dữ liệu dị thường (tốc độ rác 999 m/s), làm kết quả chuyển thành `PASS=5 WARN=1 ERROR=1`. Sau khi khôi phục, hệ thống trở về `PASS=6 WARN=1 ERROR=0`. Điều này chứng minh "cổng chất lượng" (quality gate) đóng vai trò lá chắn sống còn ngăn ngừa rác dữ liệu lây lan xuống downstream consumers.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo kết quả từ `idempotency.json` trên partition `trip-00003`:
- Số dòng ban đầu: 60 dòng (toàn bảng 1246 dòng, checksum `6c7480dfde7e`).
- Sau lần chạy lại 1: 60 dòng (toàn bảng 1246 dòng, checksum `6c7480dfde7e`, không đổi).
- Sau lần chạy lại 2: 60 dòng (toàn bảng 1246 dòng, checksum `6c7480dfde7e`, không đổi).
- Khi chạy thử nghiệm kiểu ghi `append`: Số dòng của trip tăng gấp đôi lên 120 dòng (toàn bảng 1306 dòng), gây trùng lặp dữ liệu nghiêm trọng.
Ghi đè theo partition trong Delta Lake an toàn vượt trội so với thao tác "xóa rồi chép" vì Delta Lake đảm bảo tính nguyên tử ACID qua transaction log: dữ liệu mới được ghi độc lập, sau đó metadata trỏ hoán đổi nguyên tử trong một commit duy nhất. Nếu tiến trình bị sập giữa chừng, transaction bị hủy bỏ và dữ liệu cũ vẫn nguyên vẹn; trong khi thao tác xóa thủ công nếu sập giữa chừng sẽ làm mất trắng dữ liệu của trip đó.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen (`black`) là khung hình mất tín hiệu hoàn toàn (độ sáng trung bình cực thấp < 5 do nắp che ống kính hoặc hỏng sensor), không chứa thông tin hữu ích và bị pipeline loại bỏ thẳng tay. Khung hình tối (`low-light`) là khung hình thiếu sáng thực tế (xe đi ban đêm, độ sáng 5..30) nhưng vẫn bảo toàn chi tiết đường sá, được pipeline đánh dấu và giữ lại cho mô hình học lái ban đêm; tôi hoàn toàn đồng ý với cách phân loại này.

Số liệu từ `threshold.json`:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng dHash = 2. Ngưỡng này loại bỏ triệt để các khung hình bất động khi xe dừng đèn đỏ hoặc kẹt xe mà không làm mất các biến đổi tinh tế của cảnh vật. Ngưỡng dHash phụ thuộc chặt chẽ vào vận tốc: xe chạy tốc độ cao thì sự khác biệt giữa hai khung hình liên tiếp rất lớn (dHash cao), còn khi xe chạy chậm trong phố thì khung cảnh biến đổi chậm (dHash thấp). Nếu đặt ngưỡng quá cao như 6, ta sẽ vô tình xóa nhầm rất nhiều khung hình giá trị khi xe di chuyển tốc độ thấp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Theo `leakage.json`, khi chia ngẫu nhiên theo từng khung hình (frame split), toàn bộ 26/26 trip (100%) đều bị chia cắt và xuất hiện đồng thời ở cả tập huấn luyện (train) lẫn tập kiểm thử (test). Ngược lại, khi phân chia theo đơn vị trip (trip split), có đúng 0/26 trip bị chia cắt, đảm bảo ranh giới độc lập hoàn toàn giữa train (18 trips), val (4 trips) và test (4 trips).
Chia theo khung hình tạo ra điểm kiểm thử đẹp giả tạo (data leakage) vì các khung hình kề cận trong cùng một trip có tương quan không thời gian cực cao (cùng một cung đường, cùng điều kiện ánh sáng, góc camera và quang cảnh xung quanh). Mô hình thị giác máy tính sẽ ghi nhớ đặc trưng cụ thể của trip đó ở tập train và dễ dàng đoán đúng ở tập test, cho ra độ chính xác cao ảo nhưng sẽ thất bại hoàn toàn khi triển khai thực tế trên cung đường mới lạ chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` sử dụng hàm băm nội dung (content-based hashing) nhằm bảo đảm tính xác định (determinism) và tính tái lập (reproducibility) của bộ dữ liệu phục vụ Machine Learning: cùng một tập đầu vào và cùng logic xử lý sẽ luôn tạo ra một chuỗi băm đồng nhất tuyệt đối, bất kể thời điểm chạy. Hai lần chạy liên tiếp của tôi đều tạo ra đúng mã phiên bản `51bc9b5836c0`.
Mã `dataset_version` của tôi khác biệt hoàn toàn với các bạn khác dù xử lý chung tập dữ liệu nguồn là vì pipeline đã đưa mã số sinh viên `LAB_STUDENT_ID` (`2A202602701`) vào làm salt trong thuật toán phân chia trip. Muối băm này làm cho danh sách trip được gán ngẫu nhiên vào các tập train/val/test của mỗi sinh viên là duy nhất, dẫn đến manifest và nội dung bộ dữ liệu vàng sinh ra là duy nhất.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dựa trên trường `lineage` trong `manifest.json`, dataset này được tạo từ:
- Bảng `bronze_raw_telemetry` phiên bản gốc và video raw trong bucket `lake`.
- Bảng `silver_telemetry` phiên bản Delta tương ứng với các ràng buộc schema sạch.
- Bảng `silver_frames` phiên bản Delta sau khi trích xuất và lọc chất lượng hình ảnh.
- Bảng `gold_dataset_frames` với hạt giống phân chia `student_salt: 2A202602701`.
- Danh sách 26 trip cùng mã băm SHA-256 xác thực của từng file ảnh JPEG được xuất.
Nếu 3 tháng sau cần tái lập chính xác bộ dữ liệu này, kỹ sư cần 4 thông tin: (1) Mã commit Git của kho mã nguồn pipeline, (2) Checksum SHA-256 của tập dữ liệu raw gốc (`day09-data.zip`), (3) Giá trị `LAB_STUDENT_ID=2A202602701` được cấu hình trong `.env`, và (4) Phiên bản commit của các bảng Delta được ghi nhận trong `lineage`.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi thiết lập `LAB_FAIL_ONCE=trip-07304`, task instance `silver_frames` tại `map_index=19` (ứng với trip `trip-07304`) gặp ngoại lệ mô phỏng `RuntimeError: simulated crash while extracting trip-07304; run it again` và chuyển sang màu ĐỎ (`failed`). Toàn bộ 7 task phụ thuộc downstream (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) bị chặn và chuyển sang trạng thái CAM/HỒNG (`upstream_failed`).
Để khắc phục, tôi thực hiện clear task `silver_frames`. Nhờ cơ chế cắm cờ `flag.touch()` và partition overwrite của Delta Lake, lần chạy thứ hai thành công trọn vẹn: dữ liệu hoàn toàn không bị nhân đôi (vẫn đúng 60 frames cho trip và 1246 frames toàn bảng silver), `dataset_version` giữ nguyên giá trị `51bc9b5836c0`. Bằng chứng là ảnh chụp `report/screenshots/airflow_task_crash.png` thể hiện rõ log crash tại `map_index=19` và ảnh `report/screenshots/airflow_graph.png` thể hiện toàn bộ pipeline chuyển sang màu xanh (`success`).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi quy mô tăng lên 500 xe mỗi ngày:
1. Xử lý video đơn máy bằng FFmpeg cục bộ (Local single-node extraction): Bộ xử lý FFmpeg và đĩa cục bộ trên container Airflow sẽ quá tải và cạn kiệt dung lượng đĩa trước hàng chục ngàn giờ video. Giải pháp (Slide 12): Thay bằng kiến trúc trích xuất phân tán phi tập trung (Serverless Video Workers hoặc Ray/Celery clusters chạy trên Kubernetes), trích xuất và stream trực tiếp frame ảnh lên Object Storage (MinIO/S3).
2. Tầng chuyển đổi DuckDB/Postgres cục bộ: Cơ sở dữ liệu nhúng và database đơn node sẽ nghẽn RAM và IOPS khi xử lý hàng trăm triệu dòng telemetry. Giải pháp (Slide 22): Thay thế bằng động cơ tính toán phân tán Apache Spark hoặc Trino truy vấn trực tiếp trên Delta Lake/Iceberg.
3. Bộ điều phối Airflow LocalExecutor: Sẽ cạn kiệt slot worker và gây nghẽn kết nối database. Giải pháp (Slide 23): Nâng cấp lên Airflow KubernetesExecutor hoặc CeleryExecutor với cơ chế tự động mở rộng (Autoscaling), kết hợp hàng đợi sự kiện Kafka để đệm dòng telemetry từ xe gửi về.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    t.trip_id,
    t.drive_id,
    t.lighting,
    t.conditions,
    COUNT(f.frame_id) as total_extracted_frames,
    SUM(CASE WHEN f.is_black OR f.is_blurry OR f.is_near_duplicate OR f.is_out_of_sync THEN 1 ELSE 0 END) as dropped_frames,
    ROUND(100.0 * SUM(CASE WHEN f.is_black OR f.is_blurry OR f.is_near_duplicate OR f.is_out_of_sync THEN 1 ELSE 0 END) / COUNT(f.frame_id), 1) as drop_rate_pct
FROM silver_frames f
JOIN trips t ON f.trip_id = t.trip_id
GROUP BY t.trip_id, t.drive_id, t.lighting, t.conditions
ORDER BY drop_rate_pct DESC;
```

Truy vấn trên (đã thêm vào Dashboard 2 với Card ID 46) thống kê chi tiết tỷ lệ khung hình bị loại bỏ (`drop_rate_pct`) trên từng chuyến đi dựa trên các điều kiện môi trường thực tế (ánh sáng `lighting`, thời tiết `conditions`). Kết quả trực quan hóa giúp đội ngũ kỹ sư nhận diện các chuyến đi trong điều kiện ban đêm hoặc thời tiết xấu có tỷ lệ frame bị loại bỏ do rung nhòe (`blurry`) cao vượt trội so với ban ngày, từ đó lên kế hoạch điều chỉnh phần cứng chống rung camera hoặc bố trí lộ trình thu thập dữ liệu bổ sung để bù đắp cho tập huấn luyện AI.

## Nhìn lại

Điều tôi ấn tượng nhất là cơ chế băm nội dung kết hợp tính năng partition overwrite của Delta Lake, giúp việc tái lập và khôi phục khi sập hệ thống diễn ra vô cùng tin cậy. Điểm tôi thấy cần đào sâu thêm là cách thức đồng bộ và bù trễ đồng hồ trong kịch bản xe di chuyển vào hầm sâu mất tín hiệu GPS kéo dài nhiều phút. Một lỗi dữ liệu mà pipeline hiện tại chưa bắt được là hiện tượng lóa sáng quang học (lens flare do ánh sáng mặt trời chiếu trực diện) hoặc giọt nước đọng trên bề mặt ống kính; dù khung hình không bị nhận diện là mờ theo chuẩn Laplace nhưng vẫn gây biến dạng nghiêm trọng thị giác máy tính.
