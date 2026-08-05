package signal_engine

MAX_SIGNAL_ID :: 64
MAX_ATTR_NAME :: 64
MAX_ATTRS     :: 32
MAX_DECISIONS :: 16

Attribute :: struct {
	name:  string,
	value: f64,
}

Signal :: struct {
	signal_id:    string,
	timestamp_ms: u64,
	attrs:        []Attribute,
}

// Per-attribute rolling statistics maintained across payloads.
AttrStats :: struct {
	name:     [MAX_ATTR_NAME]u8,
	mean:     f64,
	m2:       f64, // Welford running sum of squared deviations
	variance: f64, // sample variance = m2 / (count-1), updated each sample
	min:      f64,
	max:      f64,
	p95:      f64, // P95 estimate via P² algorithm
	count:    int,
	// P² incremental percentile estimator state (O(1) per sample).
	p2_q:    [5]f64, // marker heights
	p2_n:    [5]f64, // actual marker positions (stored as f64 for arithmetic)
	p2_d:    [5]f64, // desired marker positions (cumulative)
	p2_init: int,    // 0..4 = collecting initial 5 samples, 5 = P² running
}

// Full context for one signal stream, handed to the rules engine.
SignalContext :: struct {
	signal_id:  [MAX_SIGNAL_ID]u8,
	stats:      [MAX_ATTRS]AttrStats,
	stat_count: int,
}


// Bounded copy into a fixed-size buffer, zero-padding the remainder (like strncpy).
_fixed_set :: proc(dst: []u8, src: string) {
	n := min(len(src), len(dst))
	copy(dst[:n], src[:n])
	for i in n ..< len(dst) {
		dst[i] = 0
	}
}

// Reads a fixed-size buffer back out as a string, trimmed at the first NUL.
_fixed_get :: proc(buf: []u8) -> string {
	n := 0
	for n < len(buf) && buf[n] != 0 {
		n += 1
	}
	return string(buf[:n])
}

Decision :: struct {
    rule_id:    string,
    action:     string,
    confidence: f64,
}

DecisionSet :: [dynamic]Decision
