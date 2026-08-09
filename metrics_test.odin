package signal_engine

import "core:net"
import "core:testing"

// Each test uses a distinct port so they can run in parallel without colliding.
// Ports 19800-19805 are non-standard and unlikely to be in use on dev machines.
_TEST_PORT_COUNTER :: 19800
_TEST_PORT_GAUGE   :: 19801
_TEST_PORT_REL     :: 19802
_TEST_PORT_TIMER   :: 19803
_TEST_PORT_SET     :: 19804
_TEST_PORT_FLUSH   :: 19805

// Bind a loopback UDP server on the given port.
_udp_server :: proc(port: int) -> (server: net.UDP_Socket, ok: bool) {
	s, err := net.make_bound_udp_socket(net.IP4_Address{127, 0, 0, 1}, port)
	return s, err == nil
}

// Read one UDP packet into buf and return it as a string view into buf.
_recv :: proc(server: net.UDP_Socket, buf: []u8) -> string {
	n, _, _ := net.recv_udp(server, buf)
	return string(buf[:n])
}

// Discard n packets (used to drain metrics_flush output before inspecting struct state).
_drain :: proc(server: net.UDP_Socket, n: int) {
	buf: [256]u8
	for _ in 0 ..< n {
		net.recv_udp(server, buf[:])
	}
}

@(test)
test_counter :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_COUNTER)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_COUNTER)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	buf: [256]u8
	counter_emit(sd, "x", 7)
	testing.expect_value(t, _recv(server, buf[:]), "x:7|c")
}

@(test)
test_gauge :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_GAUGE)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_GAUGE)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	buf: [256]u8
	gauge_emit(sd, "x", 3.14)
	testing.expect_value(t, _recv(server, buf[:]), "x:3.140|g")
}

@(test)
test_relative_gauge :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_REL)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_REL)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	buf: [256]u8
	gauge_emit_relative(sd, "x", 1.5)
	testing.expect_value(t, _recv(server, buf[:]), "x:+1.500|g")

	gauge_emit_relative(sd, "x", -1.5)
	testing.expect_value(t, _recv(server, buf[:]), "x:-1.500|g")
}

@(test)
test_timer :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_TIMER)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_TIMER)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	buf: [256]u8
	timer_emit(sd, "x", 50.0)
	testing.expect_value(t, _recv(server, buf[:]), "x:50.000|ms")
}

@(test)
test_set :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_SET)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_SET)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	buf: [256]u8
	set_emit_string(sd, "x", "foo")
	testing.expect_value(t, _recv(server, buf[:]), "x:foo|s")

	set_emit_integer(sd, "x", 7)
	testing.expect_value(t, _recv(server, buf[:]), "x:7|s")
}

@(test)
test_metrics_flush_resets_counters :: proc(t: ^testing.T) {
	server, ok := _udp_server(_TEST_PORT_FLUSH)
	if !testing.expect(t, ok) {return}
	defer net.close(server)

	sd, sok := statsd_create("127.0.0.1", _TEST_PORT_FLUSH)
	if !testing.expect(t, sok) {return}
	defer statsd_destroy(sd)

	m := Engine_Metrics{
		signals_received  = 10,
		parse_errors      = 2,
		decisions_emitted = 8,
		egress_drops      = 1,
		purged_contexts   = 3,
		active_contexts   = 5,
	}
	metrics_flush(&m, sd)
	_drain(server, 6) // 5 counters + 1 gauge

	// Counters zeroed; gauge preserved.
	testing.expect_value(t, m.signals_received,  0)
	testing.expect_value(t, m.parse_errors,      0)
	testing.expect_value(t, m.decisions_emitted, 0)
	testing.expect_value(t, m.egress_drops,      0)
	testing.expect_value(t, m.purged_contexts,   0)
	testing.expect_value(t, m.active_contexts,   5)
}
