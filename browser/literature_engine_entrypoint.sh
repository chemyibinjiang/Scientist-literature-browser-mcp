#!/bin/sh
set -eu

display="${DISPLAY:-:99}"
screen="${LITERATURE_BROWSER_SCREEN:-1440x1000x24}"
x_max_clients="${LITERATURE_BROWSER_X_MAX_CLIENTS:-2048}"
runtime_root="${LITERATURE_ENGINE_RUNTIME_ROOT:-/tmp/literature-engine}"
headless="${LITERATURE_BROWSER_HEADLESS:-false}"

mkdir -p "$runtime_root" "$runtime_root/runtime" "$runtime_root/workers"
chmod 0700 "$runtime_root" "$runtime_root/runtime"

if [ "$headless" = "true" ]; then
    export LITERATURE_BROWSER_HEADLESS=true
    export LITERATURE_BROWSER_CDP_URL=
    exec python3 /app/literature_browser_engine.py
fi

export DISPLAY="$display"
export LITERATURE_BROWSER_HEADLESS=false
export LITERATURE_BROWSER_CDP_URL=

Xvfb "$display" -screen 0 "$screen" -maxclients "$x_max_clients" -nolisten tcp >"$runtime_root/xvfb.log" 2>&1 &
xvfb_pid=$!
engine_pid=""

cleanup() {
    [ -z "$engine_pid" ] || kill "$engine_pid" 2>/dev/null || true
    kill "$xvfb_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 50); do
    if xdpyinfo -display "$display" >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

openbox-session >"$runtime_root/openbox.log" 2>&1 &

python3 /app/literature_browser_engine.py &
engine_pid=$!
wait "$engine_pid"
