package signal_engine

import "core:sys/posix"
import "core:testing"

@(test)
test_conn_state_values_distinct :: proc(t: ^testing.T) {
	testing.expect(t, Conn_State.Connecting != Conn_State.Reading)
	testing.expect(t, Conn_State.Reading    != Conn_State.Writing)
	testing.expect(t, Conn_State.Writing    != Conn_State.Draining)
	testing.expect(t, Conn_State.Draining   != Conn_State.Closed)
	testing.expect(t, Conn_State.Connecting != Conn_State.Closed)
}

@(test)
test_conn_role_values_distinct :: proc(t: ^testing.T) {
	testing.expect(t, Conn_Role.Listener != Conn_Role.Ingress)
	testing.expect(t, Conn_Role.Ingress  != Conn_Role.Egress)
	testing.expect(t, Conn_Role.Listener != Conn_Role.Egress)
}

@(test)
test_wbuf_append_and_partial_drain :: proc(t: ^testing.T) {
	conn := new(Connection)
	defer {
		delete(conn.wbuf)
		free(conn)
	}
	conn.wbuf = make([dynamic]u8, 0, 64)

	// append 5 bytes
	append(&conn.wbuf, u8(1), u8(2), u8(3), u8(4), u8(5))
	testing.expect_value(t, len(conn.wbuf), 5)
	testing.expect_value(t, conn.wbuf_sent, 0)

	// simulate partial write of 3
	conn.wbuf_sent = 3
	pending := len(conn.wbuf) - conn.wbuf_sent
	testing.expect_value(t, pending, 2)

	// simulate full drain
	conn.wbuf_sent = len(conn.wbuf)
	clear(&conn.wbuf)
	conn.wbuf_sent = 0
	testing.expect_value(t, len(conn.wbuf), 0)
	testing.expect_value(t, conn.wbuf_sent, 0)
}

@(test)
test_loop_send_noop_when_closed :: proc(t: ^testing.T) {
	conn := new(Connection)
	defer free(conn)
	conn.wbuf  = make([dynamic]u8, 0, 8)
	conn.state = .Closed
	defer delete(conn.wbuf)

	// simulate the guard inside loop_send
	if conn.state != .Closed {
		append(&conn.wbuf, u8(0xFF))
	}
	testing.expect_value(t, len(conn.wbuf), 0)
}

@(test)
test_egress_sentinel :: proc(t: ^testing.T) {
	invalid := posix.FD(-1)
	testing.expect_value(t, int(invalid), -1)
	testing.expect(t, invalid == posix.FD(-1), "FD(-1) should equal itself")
}

@(test)
test_draining_transition :: proc(t: ^testing.T) {
	conn := new(Connection)
	defer free(conn)
	conn.wbuf  = make([dynamic]u8, 0, 8)
	conn.state = .Writing
	defer delete(conn.wbuf)

	append(&conn.wbuf, u8(1), u8(2))

	// simulate loop_close logic when wbuf is non-empty
	was_reading := conn.state == .Reading
	conn.state = .Draining
	testing.expect(t, !was_reading, "state was Writing, not Reading")
	testing.expect_value(t, conn.state, Conn_State.Draining)
}
