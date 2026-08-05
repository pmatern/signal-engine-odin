package signal_engine

import "base:runtime"
import "core:c"
import "core:fmt"
import "core:log"
import "core:strings"
import "core:sys/posix"

Conn_Role :: enum u8 {
	Listener,
	Ingress,
	Egress,
}

Conn_State :: enum u8 {
	Connecting, // egress only: connect() returned EINPROGRESS
	Reading,    // armed for read
	Writing,    // wbuf non-empty, armed for write
	Draining,   // close pending; flush wbuf then close
	Closed,
}

Connection :: struct {
	fd:   posix.FD,
	role: Conn_Role,
	state: Conn_State,
	// Ingress framing buffer. _on_read appends each recv chunk here, then
	// fires on_ingress_data with the full accumulated slice. The callback
	// owns framing: it scans for complete messages and returns the number of
	// bytes consumed. _on_read then compacts (copies tail to front) so the
	// next call always sees unprocessed bytes starting at index 0.
	rbuf: [dynamic]u8,
	// Egress write buffer. loop_send appends here; _on_write drains from
	// wbuf[wbuf_sent:]. Uses an offset rather than compacting on each partial
	// write — decisions are small and infrequent so the buffer drains fully
	// between sends. Compacted (clear + reset wbuf_sent) only on full drain.
	wbuf:      [dynamic]u8,
	wbuf_sent: int,
	tls:       rawptr, // mbedTLS context placeholder
	loop:      ^Run_Loop,
}

Run_Loop :: struct {
	cfg:                  ^EngineConfig,
	allocator:            runtime.Allocator,
	listen_fd:            posix.FD,
	egress_fd:            posix.FD,
	conns:                map[int]^Connection,
	poll_fd:              _Poll_Handle,
	on_ingress_data:      proc(conn: ^Connection, data: []u8, ud: rawptr) -> int,
	on_ingress_connected: proc(conn: ^Connection, ud: rawptr),
	on_ingress_closed:    proc(conn: ^Connection, ud: rawptr),
	on_egress_connected:  proc(conn: ^Connection, ud: rawptr),
	on_egress_data:       proc(conn: ^Connection, data: []u8, ud: rawptr),
	on_egress_closed:     proc(conn: ^Connection, ud: rawptr),
	on_tick:              proc(loop: ^Run_Loop, ud: rawptr),
	user_data:            rawptr,
	_rbuf:                [65536]u8,
}

loop_create :: proc(cfg: ^EngineConfig, allocator := context.allocator) -> (l: ^Run_Loop, ok: bool) {
	l = new(Run_Loop, allocator)
	l.cfg       = cfg
	l.allocator = allocator
	l.listen_fd = posix.FD(-1)
	l.egress_fd = posix.FD(-1)
	l.conns     = make(map[int]^Connection, 64, allocator)
	if !_poll_init(l) {
		delete(l.conns)
		free(l, allocator)
		return nil, false
	}
	return l, true
}

loop_destroy :: proc(l: ^Run_Loop) {
	for _, conn in l.conns {
		posix.close(conn.fd)
		delete(conn.rbuf)
		delete(conn.wbuf)
		free(conn, l.allocator)
	}
	delete(l.conns)
	if l.listen_fd >= 0 {
		posix.close(l.listen_fd)
	}
	_poll_destroy(l)
	free(l, l.allocator)
}

loop_listen :: proc(l: ^Run_Loop) -> bool {
	fd := posix.socket(.INET, .STREAM)
	if fd < 0 {
		fmt.eprintln("socket() failed:", posix.errno())
		return false
	}

	one: c.int = 1
	posix.setsockopt(fd, posix.SOL_SOCKET, .REUSEADDR, &one, posix.socklen_t(size_of(one)))

	addr: posix.sockaddr_in
	addr.sin_family = .INET
	addr.sin_port   = posix.in_port_t(l.cfg.listen_port)
	addr.sin_addr   = {s_addr = posix.in_addr_t(posix.INADDR_ANY)}
	when ODIN_OS == .Darwin {
		addr.sin_len = c.uint8_t(size_of(posix.sockaddr_in))
	}

	if posix.bind(fd, (^posix.sockaddr)(&addr), posix.socklen_t(size_of(addr))) != .OK {
		fmt.eprintln("bind() failed:", posix.errno())
		posix.close(fd)
		return false
	}

	if posix.listen(fd, 128) != .OK {
		fmt.eprintln("listen() failed:", posix.errno())
		posix.close(fd)
		return false
	}

	if !_set_nonblocking(fd) {
		posix.close(fd)
		return false
	}

	l.listen_fd = fd
	_poll_arm_read(l, fd, nil)
	return true
}

loop_egress_connect :: proc(l: ^Run_Loop) -> bool {
	target, ok := _resolve_addr(l.cfg.egress_host, l.cfg.egress_port)
	if !ok {
		return false
	}

	fd := posix.socket(.INET, .STREAM)
	if fd < 0 {
		fmt.eprintln("socket() failed:", posix.errno())
		return false
	}

	if !_set_nonblocking(fd) {
		posix.close(fd)
		return false
	}

	if posix.connect(fd, (^posix.sockaddr)(&target), posix.socklen_t(size_of(target))) == .OK {
		conn, conn_ok := _conn_new(l, fd, .Egress, .Reading)
		if !conn_ok {
			posix.close(fd)
			return false
		}
		l.egress_fd = fd
		_poll_arm_read(l, fd, conn)
		if l.on_egress_connected != nil {
			l.on_egress_connected(conn, l.user_data)
		}
		return true
	}

	if posix.errno() == .EINPROGRESS {
		conn, conn_ok := _conn_new(l, fd, .Egress, .Connecting)
		if !conn_ok {
			posix.close(fd)
			return false
		}
		l.egress_fd = fd
		_poll_arm_write(l, fd, conn)
		return true
	}

	fmt.eprintln("connect() failed:", posix.errno())
	posix.close(fd)
	return false
}

loop_send_egress :: proc(loop: ^Run_Loop, data: []u8) -> bool {
	if loop.egress_fd == posix.FD(-1) {
		return false
	}
	conn := loop.conns[int(loop.egress_fd)]
	if conn == nil {
		return false
	}
	loop_send(loop, conn, data)
	return true
}

loop_run_once :: proc(l: ^Run_Loop, timeout_ms: int) -> bool {
	return _poll_wait_events(l, timeout_ms)
}

loop_run :: proc(l: ^Run_Loop) {
	for {
		timeout_ms := 5000
		if l.egress_fd == posix.FD(-1) {
			if !loop_egress_connect(l) {
				log.warn("egress reconnect failed")
			}
			timeout_ms = 100
		}
		if !loop_run_once(l, timeout_ms) {
			break
		} else {
			l.on_tick(l, l.user_data)
		}
	}
}

loop_send :: proc(l: ^Run_Loop, conn: ^Connection, data: []u8) {
	if conn.state == .Closed {
		return
	}
	append(&conn.wbuf, ..data)
	if conn.state == .Reading {
		conn.state = .Writing
		_poll_arm_write(l, conn.fd, conn)
	}
}

loop_close :: proc(l: ^Run_Loop, conn: ^Connection) {
	if conn.state == .Closed {
		return
	}
	//todo check rbuf state if necessary
	if len(conn.wbuf) - conn.wbuf_sent == 0 || conn.state == .Connecting {
		_conn_destroy(l, conn)
		return
	}
	was_reading := conn.state == .Reading
	conn.state = .Draining
	if was_reading {
		_poll_arm_write(l, conn.fd, conn)
	}
}

// --- internal ---

_conn_new :: proc(l: ^Run_Loop, fd: posix.FD, role: Conn_Role, initial_state: Conn_State) -> (conn: ^Connection, ok: bool) {
	conn = new(Connection, l.allocator)
	conn.fd    = fd
	conn.role  = role
	conn.state = initial_state
	conn.rbuf  = make([dynamic]u8, 0, 256, l.allocator) //todo are these initial capacities anything we want to configure?
	conn.wbuf  = make([dynamic]u8, 0, 256, l.allocator)
	conn.loop  = l
	l.conns[int(fd)] = conn
	return conn, true
}

_conn_destroy :: proc(l: ^Run_Loop, conn: ^Connection) {
	conn.state = .Closed
	_poll_remove(l, conn.fd)
	posix.close(conn.fd)
	if conn.role == .Egress {
		l.egress_fd = posix.FD(-1)
	}
	delete_key(&l.conns, int(conn.fd))
	switch conn.role {
	case .Ingress:
		if l.on_ingress_closed != nil {
			l.on_ingress_closed(conn, l.user_data)
		}
	case .Egress:
		if l.on_egress_closed != nil {
			l.on_egress_closed(conn, l.user_data)
		}
	case .Listener:
	}
	delete(conn.rbuf)
	delete(conn.wbuf)
	free(conn, l.allocator)
}

_on_accept :: proc(l: ^Run_Loop) {
	client_fd := posix.accept(l.listen_fd, nil, nil)
	if client_fd < 0 {
		if posix.errno() != .EAGAIN {
			log.error("accept() failed:", posix.errno())
		}
		return
	}
	if !_set_nonblocking(client_fd) {
		posix.close(client_fd)
		return
	}
	conn, ok := _conn_new(l, client_fd, .Ingress, .Reading)
	if !ok {
		posix.close(client_fd)
		return
	}
	_poll_arm_read(l, client_fd, conn)
	if l.on_ingress_connected != nil {
		l.on_ingress_connected(conn, l.user_data)
	}
}

_on_read :: proc(l: ^Run_Loop, conn: ^Connection) {
	n := posix.recv(conn.fd, &l._rbuf[0], c.size_t(size_of(l._rbuf)), {})

	if n == 0 {
		_conn_destroy(l, conn)
		return
	}

	if n < 0 {
		err := posix.errno()
		if err == .EAGAIN || err == .EWOULDBLOCK {
			_poll_arm_read(l, conn.fd, conn)
			return
		}
		if err != .ECONNRESET {
			log.errorf("recv error on fd %d: %v", conn.fd, err)
		}
		_conn_destroy(l, conn)
		return
	}

	// _rbuf is a shared scratch buffer for the recv syscall — valid only for
	// this callback invocation. Append into the per-connection rbuf so the
	// callback sees all unprocessed bytes, not just this chunk.
	switch conn.role {
	case .Ingress:
		append(&conn.rbuf, ..l._rbuf[:n])
		if l.on_ingress_data != nil {
			consumed := l.on_ingress_data(conn, conn.rbuf[:], l.user_data)
			if consumed > 0 {
				// Compact: copy unprocessed tail to front and shrink length.
				copy(conn.rbuf[:], conn.rbuf[consumed:])
				resize(&conn.rbuf, len(conn.rbuf) - consumed)
			}
		}
	case .Egress:
		if l.on_egress_data != nil {
			l.on_egress_data(conn, l._rbuf[:n], l.user_data)
		}
	case .Listener:
	}

	if conn.state == .Reading {
		_poll_arm_read(l, conn.fd, conn)
	}
}

_on_write :: proc(l: ^Run_Loop, conn: ^Connection) {
	if conn.state == .Connecting {
		so_err: c.int
		optlen := posix.socklen_t(size_of(c.int))
		if posix.getsockopt(conn.fd, posix.SOL_SOCKET, .ERROR, &so_err, &optlen) != .OK || so_err != 0 {
			log.errorf("egress connect failed so_err=%d", so_err)
			_conn_destroy(l, conn)
			return
		}
		if len(conn.wbuf) > conn.wbuf_sent {
			conn.state = .Writing
			_poll_arm_write(l, conn.fd, conn)
		} else {
			conn.state = .Reading
			_poll_arm_read(l, conn.fd, conn)
		}
		if l.on_egress_connected != nil {
			l.on_egress_connected(conn, l.user_data)
		}
		return
	}

	if conn.wbuf_sent >= len(conn.wbuf) {
		conn.state = .Reading
		_poll_arm_read(l, conn.fd, conn)
		return
	}

	pending := conn.wbuf[conn.wbuf_sent:]
	n := posix.write(conn.fd, raw_data(pending), c.size_t(len(pending)))

	if n < 0 {
		err := posix.errno()
		if err == .EAGAIN || err == .EWOULDBLOCK {
			_poll_arm_write(l, conn.fd, conn)
			return
		}
		log.errorf("write error on fd %d: %v", conn.fd, err)
		_conn_destroy(l, conn)
		return
	}

	conn.wbuf_sent += int(n)

	if conn.wbuf_sent >= len(conn.wbuf) {
		clear(&conn.wbuf)
		conn.wbuf_sent = 0
		if conn.state == .Draining {
			_conn_destroy(l, conn)
			return
		}
		conn.state = .Reading
		_poll_arm_read(l, conn.fd, conn)
	} else {
		_poll_arm_write(l, conn.fd, conn)
	}
}

_set_nonblocking :: proc(fd: posix.FD) -> bool {
	flags := posix.fcntl(fd, .GETFL)
	if flags < 0 {
		fmt.eprintln("fcntl GETFL failed:", posix.errno())
		return false
	}
	new_flags := transmute(posix.O_Flags)flags | {.NONBLOCK}
	if posix.fcntl(fd, .SETFL, transmute(c.int)new_flags) < 0 {
		fmt.eprintln("fcntl SETFL failed:", posix.errno())
		return false
	}
	return true
}

_resolve_addr :: proc(host: string, port: int) -> (posix.sockaddr_in, bool) {
	host_cstr := strings.clone_to_cstring(host, context.temp_allocator)

	port_buf: [16]byte
	port_s := fmt.bprintf(port_buf[:len(port_buf)-1], "%d", port)
	port_buf[len(port_s)] = 0
	port_cstr := cstring(raw_data(port_buf[:]))

	hints: posix.addrinfo
	hints.ai_family   = .INET
	hints.ai_socktype = .STREAM

	result: ^posix.addrinfo
	if posix.getaddrinfo(host_cstr, port_cstr, &hints, &result) != posix.Info_Errno(0) {
		fmt.eprintfln("getaddrinfo failed for %s:%d", host, port)
		return {}, false
	}
	defer posix.freeaddrinfo(result)

	return (^posix.sockaddr_in)(result.ai_addr)^, true
}
