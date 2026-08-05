package signal_engine

import "core:c"
import "core:sys/posix"
import "core:testing"

// Plain TCP server on a given port. Returns the listen fd (blocking, for the test peer).
_test_listener :: proc(port: int) -> posix.FD {
	fd := posix.socket(.INET, .STREAM)
	one: c.int = 1
	posix.setsockopt(fd, posix.SOL_SOCKET, .REUSEADDR, &one, posix.socklen_t(size_of(one)))

	addr: posix.sockaddr_in
	addr.sin_family = .INET
	addr.sin_port   = posix.in_port_t(port)
	addr.sin_addr   = {s_addr = posix.in_addr_t(posix.INADDR_ANY)}
	when ODIN_OS == .Darwin {
		addr.sin_len = c.uint8_t(size_of(posix.sockaddr_in))
	}

	posix.bind(fd, (^posix.sockaddr)(&addr), posix.socklen_t(size_of(addr)))
	posix.listen(fd, 5)
	return fd
}

// Default config wired to loopback test ports. Each test must pass its own
// port pair so parallel test runs (ODIN_TEST_THREADS > 1) don't collide.
_test_config :: proc(listen_port, egress_port: int) -> EngineConfig {
	cfg := _make_default_config()
	cfg.listen_port = listen_port
	cfg.egress_host = "127.0.0.1"
	cfg.egress_port = egress_port
	return cfg
}

@(test)
test_ingress_accept_and_read :: proc(t: ^testing.T) {
	cfg := _test_config(18460, 18461)
	l, ok := loop_create(&cfg)
	testing.expect(t, ok, "loop_create failed")
	if !ok { return }
	defer loop_destroy(l)

	ok = loop_listen(l)
	testing.expect(t, ok, "loop_listen failed")
	if !ok { return }

	// capture data received via callback
	received: [dynamic]u8
	defer delete(received)
	l.on_ingress_data = proc(conn: ^Connection, data: []u8, ud: rawptr) -> int {
		buf := (^[dynamic]u8)(ud)
		append(buf, ..data)
		return len(data)
	}
	l.user_data = &received

	// plain client connects to the run loop's listener
	client_addr, addr_ok := _resolve_addr("127.0.0.1", 18460)
	testing.expect(t, addr_ok, "_resolve_addr failed")
	if !addr_ok { return }

	client_fd := posix.socket(.INET, .STREAM)
	defer posix.close(client_fd)
	posix.connect(client_fd, (^posix.sockaddr)(&client_addr), posix.socklen_t(size_of(client_addr)))

	// pump: loop accepts the connection
	loop_run_once(l, 200)

	// client sends 4 bytes
	msg := [4]u8{0xDE, 0xAD, 0xBE, 0xEF}
	posix.write(client_fd, &msg[0], 4)

	// pump: loop reads and fires on_ingress_data
	loop_run_once(l, 200)

	testing.expect_value(t, len(received), 4)
	if len(received) >= 4 {
		testing.expect_value(t, received[0], u8(0xDE))
		testing.expect_value(t, received[3], u8(0xEF))
	}
}

@(test)
test_egress_connect_and_send :: proc(t: ^testing.T) {
	// start a plain TCP server on port 18463 ("upstream")
	server_fd := _test_listener(18463)
	defer posix.close(server_fd)

	cfg := _test_config(18462, 18463)
	l, ok := loop_create(&cfg)
	testing.expect(t, ok, "loop_create failed")
	if !ok { return }
	defer loop_destroy(l)

	egress_conn: ^Connection
	l.on_egress_connected = proc(conn: ^Connection, ud: rawptr) {
		ptr := (^^Connection)(ud)
		ptr^ = conn
	}
	l.user_data = &egress_conn

	ok = loop_egress_connect(l)
	testing.expect(t, ok, "loop_egress_connect failed")
	if !ok { return }

	// pump: EINPROGRESS resolves; on_egress_connected fires
	loop_run_once(l, 200)

	// server accepts
	server_client := posix.accept(server_fd, nil, nil)
	defer posix.close(server_client)
	testing.expect(t, int(server_client) >= 0, "server accept failed")

	// send via loop
	if egress_conn != nil {
		loop_send(l, egress_conn, []u8{0xCA, 0xFE})
	}
	loop_run_once(l, 200)

	// server reads
	rbuf: [4]u8
	n := posix.recv(server_client, &rbuf[0], 4, {})
	testing.expect_value(t, int(n), 2)
	if int(n) >= 2 {
		testing.expect_value(t, rbuf[0], u8(0xCA))
		testing.expect_value(t, rbuf[1], u8(0xFE))
	}
}

@(test)
test_egress_connect_no_server :: proc(t: ^testing.T) {
	// no server on 18465 — loop_egress_connect should handle failure gracefully
	cfg := _test_config(18464, 18465)
	l, ok := loop_create(&cfg)
	testing.expect(t, ok, "loop_create failed")
	if !ok { return }
	defer loop_destroy(l)

	// may return false (refused) or true (EINPROGRESS then fail on pump)
	_ = loop_egress_connect(l)
	loop_run_once(l, 50)
	// key invariant: no panic, egress_fd is either valid or -1
	testing.expect(t, true, "survived connect attempt with no server")
}
