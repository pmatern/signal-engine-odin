#+build linux
package signal_engine

import "core:c"
import "core:fmt"
import "core:log"
import "core:sys/linux"
import "core:sys/posix"

_Poll_Handle :: linux.Fd

_MAX_EVENTS :: 64

_poll_init :: proc(l: ^Run_Loop) -> bool {
	fd, err := linux.epoll_create1({})
	if err != .NONE {
		fmt.eprintln("epoll_create1() failed:", err)
		return false
	}
	l.poll_fd = fd
	return true
}

_poll_destroy :: proc(l: ^Run_Loop) {
	posix.close(posix.FD(c.int(l.poll_fd)))
}

_poll_arm_read :: proc(l: ^Run_Loop, fd: posix.FD, conn: ^Connection) -> bool {
	ev := linux.EPoll_Event{
		events = {.IN, .RDHUP, .ONESHOT},
		data   = {ptr = conn},
	}
	err := linux.epoll_ctl(l.poll_fd, .ADD, linux.Fd(fd), &ev)
	if err == .EEXIST {
		err = linux.epoll_ctl(l.poll_fd, .MOD, linux.Fd(fd), &ev)
	}
	if err != .NONE {
		log.errorf("epoll_ctl arm_read failed fd=%d: %v", fd, err)
		return false
	}
	return true
}

_poll_arm_write :: proc(l: ^Run_Loop, fd: posix.FD, conn: ^Connection) -> bool {
	ev := linux.EPoll_Event{
		events = {.OUT, .ONESHOT},
		data   = {ptr = conn},
	}
	err := linux.epoll_ctl(l.poll_fd, .ADD, linux.Fd(fd), &ev)
	if err == .EEXIST {
		err = linux.epoll_ctl(l.poll_fd, .MOD, linux.Fd(fd), &ev)
	}
	if err != .NONE {
		log.errorf("epoll_ctl arm_write failed fd=%d: %v", fd, err)
		return false
	}
	return true
}

_poll_remove :: proc(l: ^Run_Loop, fd: posix.FD) {
	linux.epoll_ctl(l.poll_fd, .DEL, linux.Fd(fd), nil)
}

_poll_wait_events :: proc(l: ^Run_Loop, timeout_ms: int) -> bool {
	events: [_MAX_EVENTS]linux.EPoll_Event

	n, err := linux.epoll_wait(l.poll_fd, raw_data(events[:]), _MAX_EVENTS, i32(timeout_ms))
	if err == .EINTR {
		return true
	}
	if err != .NONE {
		log.errorf("epoll_wait failed: %v", err)
		return false
	}

	for i in 0..<int(n) {
		ev   := events[i]
		conn := (^Connection)(ev.data.ptr)

		is_err   := .ERR in ev.events || .HUP in ev.events
		is_read  := .IN in ev.events || .RDHUP in ev.events
		is_write := .OUT in ev.events

		if conn == nil {
			// listener fd (registered with nil ptr)
			if is_err {
				log.error("error on listen fd")
				continue
			}
			_on_accept(l)
			_poll_arm_read(l, l.listen_fd, nil)
			continue
		}

		// guard: connection may have been destroyed earlier in this batch
		if _, exists := l.conns[int(conn.fd)]; !exists {
			continue
		}

		if is_err {
			_conn_destroy(l, conn)
			continue
		}

		if is_write {
			_on_write(l, conn)
		} else if is_read {
			_on_read(l, conn)
		}
	}

	return true
}
