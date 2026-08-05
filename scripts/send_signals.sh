#!/bin/sh
# Send test signals to the engine ingress port.
# Sends enough samples to trigger HIGH_MEAN_LATENCY (>=10, mean>500)
# and ERROR_RATE_SPIKE (>=5, mean>0.05).
# Usage: ./send_signals.sh [host] [port]

HOST=${1:-localhost}
PORT=${2:-8443}

for i in $(seq 1 15); do
    printf '{"signal_id":"svc-test","attrs":[{"name":"latency_ms","value":600},{"name":"error_rate","value":0.10}]}\n'
    sleep 0.05
done | nc "$HOST" "$PORT"
