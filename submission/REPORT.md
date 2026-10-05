# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Ngô Minh Thu / 2A202602679
**Repo:** https://github.com/ngominhthu09/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:**dade02a37f389cccbf308b8b9720d98db9d947bf
**AI đã dùng và phạm vi hỗ trợ:** Codex hỗ trợ đọc code/test, đề xuất và áp dụng ba sửa lỗi, ; người nộp chạy kiểm chứng và viết REPORT.
**Nguồn tham khảo khác:** Không.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Silver có 24 hàng nhưng chỉ 12 `ticket_id`; T-91 xuất hiện cả `low/open` và `high/closed`. | Feature u05 ngày 12/08 là `(2,1,0)`, không khớp full recompute `(5,3,1)`. | T-97 đã bị xoá ở nguồn nhưng còn trong Silver, snapshot mới nhất và RAG chunks. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ `INSERT`, không ghi theo khoá giữa các batch. | `LOOKBACK_DAYS=0` nên lần ingest 15/08 không tính lại event date 12/08. | Staging chỉ lấy khoá từ `after`, nhưng CDC delete có `after=null`, nên bản ghi bị loại. |
| **Cách sửa** | `pipeline/silver.py`: `MERGE` theo `ticket_id`, chỉ update khi `_lsn` lớn hơn. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3=ceil(P99)` để overwrite các partition bị ảnh hưởng. | `pipeline/staging.py`: `coalesce(after.ticket_id,before.ticket_id,key.ticket_id)` và giữ tombstone. |
| **Khái niệm** | Unique key, CDC ordering bằng LSN, idempotency/replay safety. | Event time khác ingest time; đo lateness để chọn lookback microbatch. | CDC delete mang thay đổi nghiệp vụ; Kafka `value=null` phục vụ log compaction. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- Parity: `PARITY` — `silver_tickets` và `gold_feature_daily` cùng checksum với dbt.

## 3. Lựa chọn công cụ / kỹ thuật

- `MERGE` theo khoá giữ một trạng thái hiện tại/ticket; overwrite-partition đưa late event về đúng event date.
- Tombstone giữ lịch sử CDC và phát tín hiệu xoá xuống downstream mà không mất lineage.
- Snapshot “as of” bất biến giúp tái lập đúng tập huấn luyện đã dùng tại từng thời điểm.
- DuckDB/dbt đủ cho dữ liệu nhỏ chạy local nhưng vẫn hỗ trợ merge, microbatch, test và parity; Spark là dư thừa.

## 4. Hai câu hỏi suy ngẫm

1. Khi có yêu cầu xoá hợp lệ, quyền xoá phải ưu tiên: tìm mọi snapshot/index liên quan bằng lineage rồi xoá, tái tạo hoặc crypto-shred theo retention/legal-hold; audit chỉ lưu mã định danh và bằng chứng xử lý, không giữ PII. “Bất biến” chỉ áp dụng khi chưa có yêu cầu erasure.
2. Đặt NER/PII detector tiếng Việt ở cổng Bronze→Silver và chốt lại trước Gold/RAG; tự động mask trường hợp chắc chắn, quarantine trường hợp confidence thấp. Đo precision, recall, F1 theo từng loại PII và tỷ lệ PII lọt xuống Gold trên tập gán nhãn/canary định kỳ.

## 5. Output (dán nguyên văn)

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.96s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> Push-Location dbt_project
PS> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
PS> Pop-Location

PS> .\.venv\Scripts\python.exe -m scripts.parity
RESULT: PARITY — both implementations agree
  silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
```
