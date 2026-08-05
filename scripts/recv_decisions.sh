#!/bin/sh
# Listen for decisions from the engine egress port.
# The engine connects outbound to this port, so start this before the engine.
# Requires socat and jq.
# Usage: ./recv_decisions.sh [port]

PORT=${1:-9443}

socat TCP-LISTEN:"$PORT",reuseaddr,fork - | jq -R 'fromjson?'
