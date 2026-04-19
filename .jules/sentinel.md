## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).

## 2024-05-24 - [SQL Injection in dynamic table creation from UFDR]
**Vulnerability:** The application was copying tables dynamically from an uploaded UFDR SQLite database to the main case database. It passed table names directly into `PRAGMA table_info({table})`, `SELECT * FROM {table}`, and `INSERT INTO {table}` using f-strings without quotes or validation. An attacker could craft a malicious UFDR database with a table name like `devices; DROP TABLE cases;--` which would execute arbitrary SQL.
**Learning:** SQLite cannot parameterize table names or PRAGMA statements. When interacting with untrusted dynamic schemas, directly interpolating table names is a critical vulnerability.
**Prevention:** Always validate dynamic table names with `.isidentifier()` to ensure they only contain alphanumeric characters and underscores, and cannot contain spaces or SQL statements. Always wrap the table name in double quotes (e.g., `\"{table}\"`) when inserting into SQL queries.
