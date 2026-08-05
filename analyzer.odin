package signal_engine

import "core:log"

// P² incremental estimator for p = 0.95.
// Desired position increments per observation: [0, p/2, p, (1+p)/2, 1]
_P2_DN := [5]f64{0.0, 0.475, 0.95, 0.975, 1.0}

_p2_update :: proc(s: ^AttrStats, x: f64) {
	if s.p2_init < 5 {
		// Collect first 5 samples with online insertion sort into p2_q[0..p2_init-1].
		j := s.p2_init
		for j > 0 && s.p2_q[j - 1] > x {
			s.p2_q[j] = s.p2_q[j - 1]
			j -= 1
		}
		s.p2_q[j] = x
		s.p2_init += 1
		if s.p2_init == 5 {
			for &n, i in s.p2_n {
				n = f64(i + 1)
				// Desired position after exactly 5 observations: 1 + 4 * fraction[i]
				s.p2_d[i] = 1.0 + 4.0 * _P2_DN[i]
			}
		}
		idx := int(0.95 * f64(s.p2_init - 1))
		s.p95 = s.p2_q[idx]
		return
	}

	// Find cell k where x lands.
	k: int
	if x < s.p2_q[0] {
		s.p2_q[0] = x
		k = 0
	} else if x >= s.p2_q[4] {
		s.p2_q[4] = x
		k = 3
	} else {
		k = 3
		for i in 0..<4 {
			if x < s.p2_q[i + 1] {
				k = i
				break
			}
		}
	}

	// Increment actual positions of markers above k.
	for &n in s.p2_n[k + 1:] {
		n += 1
	}

	// Advance desired positions.
	for &d, i in s.p2_d {
		d += _P2_DN[i]
	}

	// Adjust interior markers (indices 1, 2, 3).
	for i in 1..=3 {
		delta := s.p2_d[i] - s.p2_n[i]
		sign: int = 1 if delta >= 0.0 else -1
		adelta := abs(delta)

		left_ok := sign == -1 && s.p2_n[i - 1] - s.p2_n[i] < -1.0
		right_ok := sign == 1 && s.p2_n[i + 1] - s.p2_n[i] > 1.0
		if adelta < 1.0 || (!left_ok && !right_ok) {
			continue
		}

		d := f64(sign)
		span := s.p2_n[i + 1] - s.p2_n[i - 1]
		q_new :=
			s.p2_q[i] +
			d / span *
				((s.p2_n[i] - s.p2_n[i - 1] + d) * (s.p2_q[i + 1] - s.p2_q[i]) / (s.p2_n[i + 1] - s.p2_n[i]) +
					(s.p2_n[i + 1] - s.p2_n[i] - d) * (s.p2_q[i] - s.p2_q[i - 1]) / (s.p2_n[i] - s.p2_n[i - 1]))

		if q_new > s.p2_q[i - 1] && q_new < s.p2_q[i + 1] {
			s.p2_q[i] = q_new
		} else {
			s.p2_q[i] += d * (s.p2_q[i + sign] - s.p2_q[i]) / (s.p2_n[i + sign] - s.p2_n[i])
		}
		s.p2_n[i] += d
	}

	s.p95 = s.p2_q[2]
}

_welford_update :: proc(s: ^AttrStats, x: f64) {
	s.count += 1

	delta := x - s.mean
	s.mean += delta / f64(s.count)
	s.m2 += delta * (x - s.mean)

	s.variance = s.m2 / f64(s.count - 1) if s.count > 1 else 0.0

	if s.count == 1 || x < s.min {
		s.min = x
	}
	if s.count == 1 || x > s.max {
		s.max = x
	}

	_p2_update(s, x)
}

_find_or_create_stat :: proc(ctx: ^SignalContext, name: string) -> (stat: ^AttrStats, ok: bool) {
	for &s in ctx.stats[:ctx.stat_count] {
		if _fixed_get(s.name[:]) == name {
			return &s, true
		}
	}
	if ctx.stat_count >= MAX_ATTRS {
		log.warnf("stat_count limit hit for signal_id=%s", _fixed_get(ctx.signal_id[:]))
		return nil, false
	}
	stat = &ctx.stats[ctx.stat_count]
	ctx.stat_count += 1
	stat^ = AttrStats{}
	_fixed_set(stat.name[:], name)
	return stat, true
}

analyzer_update :: proc(signal: ^Signal, ctx: ^SignalContext) {
	_fixed_set(ctx.signal_id[:], signal.signal_id)
	for &attr in signal.attrs {
		stat, ok := _find_or_create_stat(ctx, attr.name)
		if ok {
			_welford_update(stat, attr.value)
		}
	}
}
