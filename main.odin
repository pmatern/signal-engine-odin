package signal_engine

import "core:encoding/json"
import "core:fmt"
import "core:log"
import "core:os"

// App_State threads all shared mutable state through loop.user_data so that
// callbacks don't need globals.
App_State :: struct {
	store:      ^Context_Store,
	metrics:    Engine_Metrics,
	statsd:     StatsD,
	has_statsd: bool,
}

main :: proc() {
	config_path := len(os.args) > 1 ? os.args[1] : "config.json"
	cfg, ok := load_config_from_file(config_path)
	if !ok {
		fmt.eprintln("failed to load config at: ", config_path)
		os.exit(1)
	}

	level, lok := parse_log_level(cfg.log_level)
	if !lok {
		fmt.eprintfln("unknown log level: %q", cfg.log_level)
		os.exit(1)
	}

	context.logger = log.create_console_logger(lowest = level)
	defer log.destroy_console_logger(context.logger)

	loop, cok := loop_create(&cfg)
	if !cok {
		fmt.eprintln("failed to create runloop")
		os.exit(1)
	}
	defer loop_destroy(loop)

	loop.on_ingress_data = on_ingress_data
	loop.on_egress_data = on_egress_data
	loop.on_tick = on_tick

	if !loop_egress_connect(loop) {
		fmt.eprintfln("failed to connect to: %s[%d], will retry", cfg.egress_host, cfg.egress_port)
	}

	app := App_State{}
	app.store = context_store_create()
	defer context_store_destroy(app.store)

	if cfg.statsd_port != 0 {
		app.statsd, app.has_statsd = statsd_create(cfg.statsd_host, cfg.statsd_port)
		if !app.has_statsd {
			log.warnf("failed to connect to StatsD at %s:%d", cfg.statsd_host, cfg.statsd_port)
		} else {
			defer statsd_destroy(app.statsd)
		}
	}

	loop.user_data = &app

	if !loop_listen(loop) {
		fmt.eprintln("failed to listen at: ", cfg.listen_port)
		os.exit(1)
	}

	// TODO: TLS, signal handling for graceful shutdown, and metrics are deferred.

	loop_run(loop)
}

on_tick :: proc(loop: ^Run_Loop, ud: rawptr) {
	app := (^App_State)(ud)
	n := context_purge_stale(app.store, loop.cfg.context_ttl_ms)
	if app.has_statsd {
		app.metrics.purged_contexts += n
		app.metrics.active_contexts = context_count(app.store)
		metrics_flush(&app.metrics, app.statsd)
	}
}

on_ingress_data :: proc(conn: ^Connection, data: []u8, ud: rawptr) -> int {
	app := (^App_State)(ud)
	consumed: int = 0
	/// redirect all default allocations to temp; explicit allocators (store.allocator, loop.allocator) are unaffected
	context.allocator = context.temp_allocator

	for {
		pos := -1
		for i in consumed ..< len(data) {
			if data[i] == '\n' {
				pos = i
				break
			}
		}
		if pos < 0 do break

		line := string(data[consumed:pos])
		sig := Signal{}
		err := json.unmarshal_string(line, &sig)

		if err != nil {
			log.warn("unable to parse input line to Signal:", err)
			app.metrics.parse_errors += 1
		} else {
			app.metrics.signals_received += 1
			ctx := context_get_or_create(app.store, sig.signal_id)
			analyzer_update(&sig, ctx)

			ds := make(DecisionSet, context.temp_allocator)
			rules_evaluate(ctx, &ds)

			emit_decisions(conn.loop, ds, &app.metrics)
		}

		consumed = pos + 1 // includes the \n
	}

	free_all(context.temp_allocator)
	return consumed
}

emit_decisions :: proc(loop: ^Run_Loop, ds: DecisionSet, m: ^Engine_Metrics) {
	for d in ds {
		json_bytes, merr := json.marshal(d, {}, context.temp_allocator)
		if merr != nil {
			log.warn("failed to marshal Decision:", merr)
			continue
		}
		buf := make([]u8, len(json_bytes) + 1, context.temp_allocator)
		copy(buf, json_bytes)
		buf[len(json_bytes)] = '\n'
		if !loop_send_egress(loop, buf) {
			log.warn("egress not connected, dropping decision")
			m.egress_drops += 1
		} else {
			m.decisions_emitted += 1
		}
	}
}

on_egress_data :: proc(conn: ^Connection, data: []u8, ud: rawptr) {
	// only if the egress protocol sends back acks or responses
}

parse_log_level :: proc(s: string) -> (log.Level, bool) {
	switch s {
	case "debug":
		return .Debug, true
	case "info":
		return .Info, true
	case "warning":
		return .Warning, true
	case "error":
		return .Error, true
	case "fatal":
		return .Fatal, true
	}
	return .Info, false
}
