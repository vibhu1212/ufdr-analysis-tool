## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).

## 2025-02-20 - [Fix SQL injection in dynamic table handling]
**Vulnerability:** Found SQL injection vulnerabilities in `frontend/components/ufdr_upload_component.py` where table names and column names read from untrusted SQLite databases (`ufdr` archives) were directly interpolated into SQL queries like `PRAGMA table_info({table})`, `SELECT * FROM {table}`, and `INSERT`.
**Learning:** When interacting with untrusted databases and dynamically constructing queries involving table names that cannot be parameterized by standard means (like `?`), it is critical to validate identifiers and correctly quote them.
**Prevention:** Use `.isidentifier()` to validate dynamically-sourced table and column names. Always use double-quotes (e.g. `"{table}"`) when interpolating these identifiers into query strings to prevent SQL injection.
