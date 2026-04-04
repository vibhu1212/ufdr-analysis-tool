## 2024-04-04 - [CRITICAL] Remote Code Execution via eval() on Video Metadata
**Vulnerability:** A critical Remote Code Execution (RCE) vulnerability was found in `media/video_processor.py`, where `eval()` was used to parse `r_frame_rate` strings obtained from external video metadata (e.g., `'30000/1001'`).
**Learning:** External data, such as media metadata (FFmpeg probe output), can be arbitrarily manipulated by an attacker to contain malicious Python code. Using `eval()` on this unsanitized data poses a critical RCE risk.
**Prevention:** Avoid `eval()` entirely for parsing data. Always use safe, deterministic string parsing, such as `split('/')` and float casting, to safely handle and validate metadata.
