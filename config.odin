package signal_engine

import "base:runtime"
import "core:encoding/json"
import "core:fmt"
import "core:os"

EngineConfig :: struct {
	listen_port:    int,
	egress_port:    int,
	egress_host:    string,
	worker_threads: int,
	max_inflight:   int,
	context_ttl_ms: u64,
	statsd_host:    string,
	statsd_port:    int, // 0 = disabled
	cert_path:      string,
	key_path:       string,
	ca_cert_path:   string,
	log_level:      string,
	allocator:      runtime.Allocator,
}

_make_default_config :: proc() -> EngineConfig {
	return EngineConfig {
		listen_port = 8443,
		egress_port = 9443,
		egress_host = "localhost",
		worker_threads = 4,
		max_inflight = 1024,
		context_ttl_ms = 300_000,
		statsd_host = "127.0.0.1",
		statsd_port = 0, // disabled by default; set to 8125 to enable
		cert_path = "certs/server.crt",
		key_path = "certs/server.key",
		ca_cert_path = "certs/ca.crt",
		log_level = "info",
	}
}

load_config_from_file :: proc(
	config_file: string,
	allocator := context.allocator,
) -> (
	EngineConfig,
	bool,
) {
	cfg := _make_default_config()
	cfg.allocator = allocator

	data, err := os.read_entire_file_from_path(config_file, context.temp_allocator)
	if err != nil {
		fmt.eprintln("error reading config file:", err)
		return cfg, false
	}

	jerr := json.unmarshal(data, &cfg, json.DEFAULT_SPECIFICATION, allocator)
	if jerr != nil {
		fmt.eprintln("failed to parse json config:", jerr)
		return cfg, false
	}

	return cfg, true
}

destroy_config :: proc(cfg: ^EngineConfig) {
	context.allocator = cfg.allocator
	delete(cfg.egress_host)
	delete(cfg.statsd_host)
	delete(cfg.cert_path)
	delete(cfg.key_path)
	delete(cfg.ca_cert_path)
	delete(cfg.log_level)
}
