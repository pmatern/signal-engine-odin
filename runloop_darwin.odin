#+build darwin
package signal_engine

import "core:c"
import "core:fmt"
import "core:log"
import "core:sys/kqueue"
import "core:sys/posix"

_Poll_Handle :: kqueue.KQ

_MAX_EVENTS :: 64

_poll_init :: proc(l: ^Run_Loop) -> bool {
	kq, err := kqueue.kqueue()
	if err != .NONE {
		fmt.eprintln("kqueue() failed:", err)
		return false
	}
	l.poll_fd = kq
	return true
}

_poll_destroy :: proc(l: ^Run_Loop) {
	posix.close(l.poll_fd)
}

_poll_arm_read :: proc(l: ^Run_Loop, fd: posix.FD, conn: ^Connection) -> bool {
	ev := kqueue.KEvent{
		ident  = uintptr(fd),
		filter = .Read,
		flags  = {.Add, .One_Shot},
		udata  = conn,
	}
	_, err := kqueue.kevent(l.poll_fd, []kqueue.KEvent{ev}, []kqueue.KEvent{}, nil)
	if err != .NONE {
		log.errorf("kevent arm_read failed fd=%d: %v", fd, err)
		return false
	}
	return true
}

_poll_arm_write :: proc(l: ^Run_Loop, fd: posix.FD, conn: ^Connection) -> bool {
	ev := kqueue.KEvent{
		ident  = uintptr(fd),
		filter = .Write,
		flags  = {.Add, .One_Shot},
		udata  = conn,
	}
	_, err := kqueue.kevent(l.poll_fd, []kqueue.KEvent{ev}, []kqueue.KEvent{}, nil)
	if err != .NONE {
		log.errorf("kevent arm_write failed fd=%d: %v", fd, err)
		return false
	}
	return true
}

_poll_remove :: proc(l: ^Run_Loop, fd: posix.FD) {
	evs := [2]kqueue.KEvent{
		{ident = uintptr(fd), filter = .Read,  flags = {.Delete}},
		{ident = uintptr(fd), filter = .Write, flags = {.Delete}},
	}
	kqueue.kevent(l.poll_fd, evs[:], []kqueue.KEvent{}, nil)
}

_poll_wait_events :: proc(l: ^Run_Loop, timeout_ms: int) -> bool {
	events: [_MAX_EVENTS]kqueue.KEvent

	ts: posix.timespec
	ts_ptr: ^posix.timespec = nil
	if timeout_ms >= 0 {
		ts.tv_sec  = posix.time_t(timeout_ms / 1000)
		ts.tv_nsec = c.long((timeout_ms % 1000) * 1_000_000)
		ts_ptr = &ts
	}

	n, err := kqueue.kevent(l.poll_fd, []kqueue.KEvent{}, events[:], ts_ptr)
	if err == .EINTR {
		return true
	}
	if err != .NONE {
		log.errorf("kevent wait failed: %v", err)
		return false
	}

	for i in 0..<int(n) {
		ev := events[i]

		// guard: connection may have been destroyed earlier in this batch
		fd := posix.FD(ev.ident)

		if .Error in ev.flags {
			if fd == l.listen_fd {
				log.error("error on listen fd")
				continue
			}
			if conn, exists := l.conns[int(fd)]; exists {
				_conn_destroy(l, conn)
			}
			continue
		}

		if l.listen_fd >= 0 && ev.ident == uintptr(l.listen_fd) {
			_on_accept(l)
			_poll_arm_read(l, l.listen_fd, nil)
			continue
		}

		conn, exists := l.conns[int(fd)]
		if !exists {
			continue
		}

		if .EOF in ev.flags {
			_conn_destroy(l, conn)
			continue
		}

		#partial switch ev.filter {
		case .Read:
			_on_read(l, conn)
		case .Write:
			_on_write(l, conn)
		}
	}

	return true
}
