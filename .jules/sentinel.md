## 2024-05-24 - [Avoid `eval()` on external metadata]
**Vulnerability:** Found `eval()` being used in `media/video_processor.py` to parse video frame rates from FFmpeg probe output (`video_stream.get('r_frame_rate', '0/1')`). This is a critical Remote Code Execution (RCE) vulnerability because an attacker could craft a malicious video file with embedded Python code in its metadata, which would be executed when the system runs `ffmpeg.probe()`.
**Learning:** External data from tools like FFmpeg should be treated as untrusted input. The assumption that video metadata will always be safely formatted as `"num/den"` is insecure.
**Prevention:** Never use `eval()` to parse strings. Use safe parsing methods such as string splitting (`str.split()`), regex, and explicit type conversion (`float()`, `int()`).
## 2025-02-14 - Fix Hardcoded Neo4j Password in Installation Scripts
**Vulnerability:** The deployment scripts (`install_linux.sh` and `install_windows.ps1`) hardcoded the Neo4j database password (`password123`) when generating the `config.env` file.
**Learning:** Hardcoding credentials in installation scripts is a common but dangerous pattern that exposes default database access to anyone with the script or access to the deployed environment. Default passwords are a primary vector for automated attacks.
**Prevention:** Always use cryptographically secure methods (e.g., `/dev/urandom` in Linux, `Get-Random` in PowerShell) to dynamically generate unique, strong passwords during the installation phase and write them securely to configuration files.
