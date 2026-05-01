## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).
## 2026-05-01 - SQL Injection in Dynamic Table Selection
**Vulnerability:** Found SQL injection vulnerabilities in `frontend/components/ufdr_upload_component.py` and `rag/report_generator.py` where untrusted table names from uploaded databases were directly interpolated into SQL queries without validation.
**Learning:** SQLite cannot parameterize table names in PRAGMA or SELECT statements. Using f-strings directly with user-provided database structures creates a critical RCE/Injection risk.
**Prevention:** Use `table.isidentifier()` to validate that the string is a safe identifier before interpolation, and wrap the table name in quotes (`"{table}"`) as a defense-in-depth measure.
