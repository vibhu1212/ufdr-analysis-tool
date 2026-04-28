## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).

## 2024-05-24 - [Fix SQL Injection in External Database Processing]
**Vulnerability:** Found `PRAGMA table_info({table})` and `SELECT * FROM {table}` directly interpolating untrusted table names from uploaded UFDR databases in `frontend/components/ufdr_upload_component.py`. An attacker could exploit this by providing a maliciously named table, leading to SQL injection.
**Learning:** Table names read from an external, user-uploaded database (e.g., via `sqlite_master`) must be treated as untrusted input. Directly interpolating them into SQL queries without validation or escaping creates critical SQL injection vulnerabilities.
**Prevention:** Always validate dynamically extracted identifiers (like table names) using `.isidentifier()` and properly enclose them in double quotes (`"{table}"`) in all dynamically constructed SQL queries.
