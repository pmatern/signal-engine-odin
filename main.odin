package signal_engine

import "core:encoding/json"
import "core:fmt"
import "core:log"
import "core:os"

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

	store := context_store_create()
	defer context_store_destroy(store)
	loop.user_data = store

	if !loop_listen(loop) {
		fmt.eprintln("failed to listen at: ", cfg.listen_port)
		os.exit(1)
	}

	// TODO: TLS, signal handling for graceful shutdown, and metrics are deferred.

	loop_run(loop)
}

on_tick :: proc(loop: ^Run_Loop, ud: rawptr) {
	context_purge_stale((^Context_Store)(ud), loop.cfg.context_ttl_ms)
}

on_ingress_data :: proc(conn: ^Connection, data: []u8, ud: rawptr) -> int {
	store := (^Context_Store)(ud)
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
		} else {
			ctx := context_get_or_create(store, sig.signal_id)
			analyzer_update(&sig, ctx)

			ds := make(DecisionSet, context.temp_allocator)
			rules_evaluate(ctx, &ds)

			emit_decisions(conn.loop, ds)
		}

		consumed = pos + 1 // includes the \n
	}

	free_all(context.temp_allocator)
	return consumed
}

emit_decisions :: proc(loop: ^Run_Loop, ds: DecisionSet) {
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
