## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).

## 2024-05-24 - [Unsanitized Dynamic Table Names in Database Queries]
**Vulnerability:** Found unparameterized, unquoted, and unvalidated string formatting for dynamic table names when querying user-uploaded UFDR SQLite databases in `frontend/components/ufdr_upload_component.py` (e.g., `f"PRAGMA table_info({table})"` and `f"SELECT * FROM {table}"`). This allows a malicious UFDR upload to inject arbitrary SQL statements into the host system.
**Learning:** Even internal tool tables extracted from untrusted files must be treated as hostile input. Standard sanitization via an allowlist doesn't work for dynamically discovered tables, so validation combined with structural safety is required.
**Prevention:** For untrusted dynamic table names, always validate against `.isidentifier()` to prevent injection sequences and explicitly enclose the validated variable in double quotes (`f'SELECT * FROM "{table}"'`).
