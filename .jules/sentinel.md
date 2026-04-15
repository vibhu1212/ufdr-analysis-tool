## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).

## 2024-05-25 - [SQL Injection via Untrusted Uploaded DB Tables]
**Vulnerability:** Found `PRAGMA table_info({table})` and `INSERT INTO {table}` using unsanitized table names sourced from an uploaded, untrusted SQLite database in `frontend/components/ufdr_upload_component.py`. This is a SQL injection risk as malicious databases could use crafted table names to execute arbitrary schema commands or queries.
**Learning:** Table names cannot be parameterized in SQL queries (unlike values). When dealing with dynamic table names, especially from external sources like uploaded `.ufdr` or `.db` files, they must be validated against a strict allowlist.
**Prevention:** Use a predefined set of allowed tables (like `SQLValidator.ALLOWED_TABLES`) to validate all dynamically extracted table names before inserting them into any query string.
