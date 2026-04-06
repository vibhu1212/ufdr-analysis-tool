## 2024-04-06 - [CRITICAL] Remote Code Execution via eval() on FFmpeg probe output
**Vulnerability:** A critical Remote Code Execution (RCE) vulnerability was present in `media/video_processor.py`. The `get_video_info` function used the Python built-in `eval()` to calculate the video frame rate (`fps`) from FFmpeg's `ffprobe` output (`video_stream.get('r_frame_rate', '0/1')`). While usually a string like "30000/1001" or "30/1", an attacker could craft a malicious video file where the metadata for frame rate contains arbitrary Python code. When parsed, this code would be executed by `eval()` within the application's context.
**Learning:** Never trust metadata or outputs from external tools, even established ones like FFmpeg. While `ffprobe` extracts information, the data itself is originally supplied by the file creator and is untrusted user input. Using `eval()` on any untrusted data is a severe security risk.
**Prevention:**
- Use safe string parsing (e.g., `.split('/')`) and explicit type conversions (`float()`) instead of evaluating strings as code.
- Always treat metadata extracted from user-supplied files (images, videos, documents) as malicious user input.
- Adopt a "trust nothing, verify everything" mindset, particularly when bridging external processes and the Python runtime.
