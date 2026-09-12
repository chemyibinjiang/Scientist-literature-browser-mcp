#!/bin/sh
set -eu

display="${DISPLAY:-:99}"
profile="${LITERATURE_BROWSER_PROFILE:-/browser-profile/chromium}"
screen="${LITERATURE_BROWSER_SCREEN:-1440x1000x24}"
renderer_process_limit="${LITERATURE_BROWSER_RENDERER_PROCESS_LIMIT:-8}"

mkdir -p "$HOME" "$profile" /tmp/runtime
chmod 0700 /tmp/runtime
rm -f \
    "$profile/SingletonLock" \
    "$profile/SingletonCookie" \
    "$profile/SingletonSocket"

export DISPLAY="$display"
export XDG_RUNTIME_DIR=/tmp/runtime
export LITERATURE_BROWSER_HEADLESS=false
export LITERATURE_BROWSER_CDP_URL=http://127.0.0.1:9222

Xvfb "$display" -screen 0 "$screen" -nolisten tcp &
xvfb_pid=$!
api_pid=""
chromium_pid=""

cleanup() {
    [ -z "$api_pid" ] || kill "$api_pid" 2>/dev/null || true
    [ -z "$chromium_pid" ] || kill "$chromium_pid" 2>/dev/null || true
    kill "$xvfb_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 50); do
    if xdpyinfo -display "$display" >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

openbox-session >/tmp/openbox.log 2>&1 &

chromium \
    --no-sandbox \
    --disable-dev-shm-usage \
    --disable-features=AsyncDns,DnsOverHttps,OptimizationHints,Translate,UseDnsHttpsSvcbAlpn \
    --disable-session-crashed-bubble \
    --renderer-process-limit="$renderer_process_limit" \
    --no-first-run \
    --no-default-browser-check \
    --remote-debugging-address=127.0.0.1 \
    --remote-debugging-port=9222 \
    --user-data-dir="$profile" \
    about:blank >/tmp/chromium.log 2>&1 &
chromium_pid=$!

for _ in $(seq 1 100); do
    if python3 -c \
        "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9222/json/version', timeout=1).read(1)" \
        >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

python3 /app/literature_browser.py &
api_pid=$!
wait "$api_pid"
