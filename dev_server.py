#!/usr/bin/env python3
import http.server
import os
import threading
import time
import subprocess
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, 'out')
NDJSON = os.path.join(OUT_DIR, 'constellation.ndjson')

def file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

def needs_refresh(max_age_seconds: int = 3600) -> bool:
    mtime = file_mtime(NDJSON)
    if mtime <= 0:
        return True
    age = time.time() - mtime
    return age >= max_age_seconds

def fetch_latest(hours: int = 24):
    cmd = ['python', os.path.join(ROOT, 'fetch_constellation.py'), '--out-dir', OUT_DIR, '--hours', str(hours)]
    print('[dev_server] Running:', ' '.join(cmd), flush=True)
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print('[dev_server] fetch failed:', e, flush=True)

def refresher_loop():
    while True:
        try:
            if needs_refresh(3600):
                fetch_latest(24)
        except Exception as e:
            print('[dev_server] refresher error:', e, flush=True)
        time.sleep(120)  # check every 2 minutes

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

def main():
    os.chdir(ROOT)
    # Start background refresher
    t = threading.Thread(target=refresher_loop, daemon=True)
    t.start()
    # Serve files
    port = int(os.environ.get('PORT', '8080'))
    with http.server.ThreadingHTTPServer(('', port), CORSRequestHandler) as httpd:
        print(f'[dev_server] Serving on http://localhost:{port}/viewer/', flush=True)
        httpd.serve_forever()

if __name__ == '__main__':
    main()



