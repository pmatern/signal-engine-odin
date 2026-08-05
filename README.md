# signal-engine

A single-threaded, event-driven signal processing engine. It accepts a stream of named signal events over TCP, maintains rolling statistics per signal stream, evaluates rules against those statistics, and forwards decisions to a downstream egress target.

## Architecture

```
ingress (TCP, NDJSON)
    |
    v
JSON parse -> Signal
    |
    v
analyzer_update -> SignalContext (rolling stats per signal_id)
    |
    v
rules_evaluate -> DecisionSet
    |
    v
egress (TCP, NDJSON)
```

**Ingress** listens for inbound TCP connections. Each connection sends `Signal` objects as newline-delimited JSON (one JSON object per line).

**Analyzer** maintains a `SignalContext` per `signal_id`, tracking mean, variance, min, max, and p95 for each named attribute using Welford's algorithm and the P² estimator. Contexts are stored in memory and purged after a configurable TTL.

**Rules** evaluate the accumulated statistics and emit `Decision` values when thresholds are exceeded.

**Egress** connects outbound to a downstream target and forwards `Decision` objects in the same format — one JSON object per line.

## Wire format

Both ingress and egress use newline-delimited JSON — one complete JSON object per line.

Ingress (`Signal`):
```json
{"signal_id": "svc-a", "attrs": [{"name": "latency_ms", "value": 612.4}, {"name": "error_rate", "value": 0.03}]}
```

Egress (`Decision`):
```json
{"rule_id": "HIGH_MEAN_LATENCY", "action": "throttle_upstream", "confidence": 0.9}
```

## Configuration

Configuration is loaded from a JSON file (default: `config.json`). All fields are optional and fall back to defaults.

| Field | Default | Description |
|---|---|---|
| `listen_port` | `8443` | Port to accept ingress connections on |
| `egress_host` | `"localhost"` | Host to connect to for egress |
| `egress_port` | `9443` | Port to connect to for egress |
| `context_ttl_ms` | `300000` | Milliseconds before an idle signal context is purged |
| `log_level` | `"info"` | One of `debug`, `info`, `warning`, `error`, `fatal` |

## Building

```sh
odin build . -out:signal-engine
```

## Running

```sh
./signal-engine config.json
```

## Testing

```sh
odin test .
```

Manual end-to-end testing scripts are in `scripts/`. Start the receiver before the engine, since the engine connects outbound to the egress port on startup.

```sh
./scripts/recv_decisions.sh   # terminal 1 — listens on egress port, requires socat and jq
./signal-engine config.json   # terminal 2
./scripts/send_signals.sh     # terminal 3 — sends 15 test signals
```

## Adding rules

Rules live in `rules.odin`. Each rule is a procedure that receives a `^SignalContext` and a `^DecisionSet`. Call `find_stat` to look up an attribute by name and `emit` to record a decision.

```odin
rule_my_rule :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
    if stat := find_stat(sc, "my_attr"); stat != nil && stat.count >= 10 && stat.mean > 100.0 {
        emit(ds, "MY_RULE_ID", "my_action", 0.8)
    }
}
```

Then register it in `rules_evaluate`:

```odin
rules_evaluate :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
    rule_high_mean_latency(sc, ds)
    rule_high_variance(sc, ds)
    rule_error_rate_spike(sc, ds)
    rule_my_rule(sc, ds)          // add here
    ...
}
```

If the signal stream does not include the attribute name the rule looks for, `find_stat` returns nil and the rule silently produces no decisions. Expected attribute names used by the built-in rules are declared as constants at the top of `rules.odin`.

Rules fire on every signal once their minimum sample count is reached — they are continuous threshold checks on rolling statistics, not one-shot alerts.
