@echo off
chcp 65001 >nul
cd /d "%~dp0"
title 실시간 채팅 서버

where node >nul 2>nul || (echo [오류] Node.js가 필요합니다: https://nodejs.org & pause & exit /b 1)

set CF=cloudflared
where cloudflared >nul 2>nul || set CF="C:\Program Files (x86)\cloudflared\cloudflared.exe"

echo 채팅 서버를 켭니다 (http://localhost:3100)
start "chat-server" /min node server.js

echo 공개 링크를 만드는 중... 아래에 나오는 https://xxxx.trycloudflare.com 주소를 공유하세요.
echo (이 창을 닫으면 공개 링크가 끊깁니다)
echo.
%CF% tunnel --no-autoupdate --url http://localhost:3100
pause
