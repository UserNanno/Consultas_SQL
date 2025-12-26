start "" msedge.exe ^
  --remote-debugging-port=9222 ^
  --user-data-dir="%~dp0edge_profile"







http://127.0.0.1:9222/json/version



netstat -ano | findstr :9222

