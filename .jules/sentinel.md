## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).
## 2026-05-02 - Hardcoded deployment credentials
**Vulnerability:** Hardcoded fallback password `password123` for Neo4j database found in `install/install_linux.sh` and `install/install_windows.ps1` scripts.
**Learning:** Deployment templates frequently encode 'example' passwords that are pushed to production if unchanged. The fix was applied by leveraging native random string generators (`/dev/urandom` for Linux, `Get-Random` for PowerShell).
**Prevention:** Configuration templates must dynamically generate secure random values during installation.
