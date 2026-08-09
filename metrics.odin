package signal_engine

import "core:fmt"
import "core:log"
import "core:net"

StatsD :: struct {
	sock: net.UDP_Socket,
	dest: net.Endpoint,
}

statsd_create :: proc(host: string, port: int) -> (StatsD, bool) {
	ret := StatsD{}

	address := net.parse_address(host)
	if address == nil {
		log.warn("Failed to parse StatsD server IP address")
		return ret, false
	}

	dest := net.Endpoint{address, port}

	sock, sock_err := net.make_unbound_udp_socket(net.Address_Family.IP4)
	if sock_err != .None {
		log.warnf("Failed to create StatD UDP socket: %v", sock_err)
		return ret, false
	}

	return StatsD{sock, dest}, true
}

statsd_destroy :: proc(sd: StatsD) {
	net.close(sd.sock)
}

statsd_emit :: proc(sd: StatsD, payload: string) {
	payload_bytes := transmute([]u8)payload
	_, send_err := net.send_udp(sd.sock, payload_bytes, sd.dest)
	if send_err != .None {
		log.warnf("Failed to send UDP packet: %v", send_err)
	}
}

timer_emit :: proc(sd: StatsD, name: string, value: f64) {
	statsd_emit(sd, fmt.tprintf("%s:%f|ms", name, value))
}

counter_emit :: proc(sd: StatsD, name: string, value: int) {
	statsd_emit(sd, fmt.tprintf("%s:%d|c", name, value))
}

gauge_emit :: proc(sd: StatsD, name: string, value: f64) {
	statsd_emit(sd, fmt.tprintf("%s:%f|g", name, value))
}

gauge_emit_relative :: proc(sd: StatsD, name: string, value: f64) {
	if value >= 0 {
		statsd_emit(sd, fmt.tprintf("%s:+%f|g", name, value))
	} else {
		statsd_emit(sd, fmt.tprintf("%s:%f|g", name, value))
	}
}

set_emit_string :: proc(sd: StatsD, name: string, value: string) {
	statsd_emit(sd, fmt.tprintf("%s:%s|s", name, value))
}

set_emit_integer :: proc(sd: StatsD, name: string, value: int) {
	statsd_emit(sd, fmt.tprintf("%s:%d|s", name, value))
}

Engine_Metrics :: struct {
	// Counters: emitted as the delta since the last flush, then zeroed.
	signals_received:  int,
	parse_errors:      int,
	decisions_emitted: int,
	egress_drops:      int,
	purged_contexts:   int,
	// Gauges: absolute current value; caller sets before calling metrics_flush.
	active_contexts: int,
}

// Emits all metrics to sd and resets counters. Gauges are not reset.
// Called from on_tick once per poll cycle.
metrics_flush :: proc(m: ^Engine_Metrics, sd: StatsD) {
	counter_emit(sd, "signal_engine.signals_received",  m.signals_received)
	counter_emit(sd, "signal_engine.parse_errors",      m.parse_errors)
	counter_emit(sd, "signal_engine.decisions_emitted", m.decisions_emitted)
	counter_emit(sd, "signal_engine.egress_drops",      m.egress_drops)
	counter_emit(sd, "signal_engine.purged_contexts",   m.purged_contexts)
	m.signals_received  = 0
	m.parse_errors      = 0
	m.decisions_emitted = 0
	m.egress_drops      = 0
	m.purged_contexts   = 0

	gauge_emit(sd, "signal_engine.active_contexts", f64(m.active_contexts))
}
