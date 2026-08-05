package signal_engine

import "core:testing"

// Helper: push n values through the analyzer for one attribute.
_feed :: proc(attr: string, values: []f64) -> SignalContext {
	ctx: SignalContext
	sig: Signal
	attr_buf := [1]Attribute{{name = attr}}
	sig.signal_id = "test"
	sig.attrs = attr_buf[:]
	for v in values {
		sig.attrs[0].value = v
		analyzer_update(&sig, &ctx)
	}
	return ctx
}

_expect_close :: proc(t: ^testing.T, got, want, eps: f64, loc := #caller_location) {
	testing.expectf(t, abs(got - want) <= eps, "expected %v to be within %v of %v", got, eps, want, loc = loc)
}

// --- Test 1: init phase (< 5 samples) ----------------------------------------
//
// With exactly 5 samples, p95 is read from the sorted init buffer at index
// floor(0.95 * 4) = 3. For sorted {10, 30, 50, 70, 80} that is 70.
@(test)
test_init_phase_p95 :: proc(t: ^testing.T) {
	vals := []f64{50.0, 10.0, 80.0, 30.0, 70.0}
	ctx := _feed("x", vals)
	s := &ctx.stats[0]

	testing.expect_value(t, s.count, 5)
	_expect_close(t, s.p95, 70.0, 1e-9)
	_expect_close(t, s.min, 10.0, 1e-9)
	_expect_close(t, s.max, 80.0, 1e-9)
}

// --- Test 2: constant stream --------------------------------------------------
//
// All samples equal → every marker converges to that value → p95 = value.
@(test)
test_constant_stream :: proc(t: ^testing.T) {
	vals: [500]f64
	for i in 0..<500 {
		vals[i] = 42.0
	}
	ctx := _feed("x", vals[:])
	_expect_close(t, ctx.stats[0].p95, 42.0, 1e-6)
}

// --- Test 3: uniform distribution convergence --------------------------------
//
// Cycling through {0, 1, ..., 99} gives a discrete uniform distribution.
// True P95: rank floor(0.95 * 99) = 94, so value 94.
// P² should converge to within ±3 after 1000 samples.
@(test)
test_uniform_convergence :: proc(t: ^testing.T) {
	vals: [1000]f64
	for i in 0..<1000 {
		vals[i] = f64(i % 100)
	}
	ctx := _feed("x", vals[:])
	p95 := ctx.stats[0].p95
	testing.expect(t, abs(p95 - 94.0) < 3.0)
}

// --- Test 4: bimodal distribution (interleaved) ------------------------------
//
// Every 10th sample is 200.0; the other 9 in each group are 1.0.
// Distribution: 90% at 1.0, 10% at 200.0 → true P95 = 200.0.
// Input must be interleaved so P² sees a stationary distribution.
@(test)
test_bimodal_interleaved_p95 :: proc(t: ^testing.T) {
	vals: [1000]f64
	for i in 0..<1000 {
		vals[i] = 200.0 if (i + 1) % 10 == 0 else 1.0
	}
	ctx := _feed("x", vals[:])
	testing.expect(t, ctx.stats[0].p95 >= 190.0)
}

// --- Test 5: monotone increasing ---------------------------------------------
//
// Samples 1..N in order. True P95 of {1..N} = ceil(0.95*N).
// For N=200: P95 = ceil(190) = 190.
// Monotone input stresses the marker adjustment logic maximally.
@(test)
test_monotone_increasing :: proc(t: ^testing.T) {
	N :: 200
	vals: [N]f64
	for i in 0..<N {
		vals[i] = f64(i + 1)
	}
	ctx := _feed("x", vals[:])
	p95 := ctx.stats[0].p95
	testing.expect(t, abs(p95 - 190.0) < 5.0)
}

// --- Test 6: welford mean and variance alongside p95 -------------------------
//
// For a constant stream of 1000 threes: mean=3, variance=0.
@(test)
test_welford_stats :: proc(t: ^testing.T) {
	vals: [1000]f64
	for i in 0..<1000 {
		vals[i] = 3.0
	}
	ctx := _feed("x", vals[:])
	s := &ctx.stats[0]
	_expect_close(t, s.mean, 3.0, 1e-9)
	_expect_close(t, s.variance, 0.0, 1e-9)
	testing.expect_value(t, s.count, 1000)
}

// --- Test 7: multiple attributes are tracked independently -------------------
//
// A signal with three attributes (latency_ms, error_rate, queue_depth) is fed
// 100 times with distinct value patterns. Each attribute must get its own
// AttrStats slot and must not bleed into the others.
@(test)
test_multi_attr_independence :: proc(t: ^testing.T) {
	ctx: SignalContext
	sig: Signal
	attr_buf_a: [3]Attribute
	attr_buf_a[0].name = "latency_ms"
	attr_buf_a[1].name = "error_rate"
	attr_buf_a[2].name = "queue_depth"
	sig.signal_id = "svc-a"
	sig.attrs = attr_buf_a[:]

	for i in 0..<100 {
		sig.attrs[0].value = 10.0 // latency_ms constant 10
		sig.attrs[1].value = 0.05 // error_rate constant 0.05
		sig.attrs[2].value = f64(i % 50) // queue_depth cycles 0..49
		analyzer_update(&sig, &ctx)
	}

	// Three distinct attributes should have been created.
	testing.expect_value(t, ctx.stat_count, 3)

	lat, err, qdep: ^AttrStats
	for i in 0..<ctx.stat_count {
		switch _fixed_get(ctx.stats[i].name[:]) {
		case "latency_ms":
			lat = &ctx.stats[i]
		case "error_rate":
			err = &ctx.stats[i]
		case "queue_depth":
			qdep = &ctx.stats[i]
		}
	}
	testing.expect(t, lat != nil)
	testing.expect(t, err != nil)
	testing.expect(t, qdep != nil)
	if lat == nil || err == nil || qdep == nil {
		return
	}

	// latency_ms: all 10.0 → mean=10, variance=0, min=max=10.
	testing.expect_value(t, lat.count, 100)
	_expect_close(t, lat.mean, 10.0, 1e-9)
	_expect_close(t, lat.variance, 0.0, 1e-9)
	_expect_close(t, lat.min, 10.0, 1e-9)
	_expect_close(t, lat.max, 10.0, 1e-9)

	// error_rate: all 0.05 → mean=0.05, variance=0.
	_expect_close(t, err.mean, 0.05, 1e-9)
	_expect_close(t, err.variance, 0.0, 1e-9)

	// queue_depth: cycles 0..49 (uniform) → mean=24.5, min=0, max=49.
	_expect_close(t, qdep.min, 0.0, 1e-9)
	_expect_close(t, qdep.max, 49.0, 1e-9)
	_expect_close(t, qdep.mean, 24.5, 1e-6)
}

// --- Test 8: same attribute across successive signals accumulates correctly --
//
// The context is reused across calls (as it is in production). Stats must
// accumulate, not reset, when the same signal_id is seen again.
@(test)
test_multi_signal_accumulation :: proc(t: ^testing.T) {
	ctx: SignalContext
	sig: Signal
	attr_buf_b: [2]Attribute
	attr_buf_b[0].name = "rps"
	attr_buf_b[1].name = "cpu_pct"
	sig.signal_id = "svc-b"
	sig.attrs = attr_buf_b[:]

	// First batch: 50 updates with rps=100, cpu_pct=30.
	for _ in 0..<50 {
		sig.attrs[0].value = 100.0
		sig.attrs[1].value = 30.0
		analyzer_update(&sig, &ctx)
	}
	// Second batch: 50 updates with rps=200, cpu_pct=60.
	for _ in 0..<50 {
		sig.attrs[0].value = 200.0
		sig.attrs[1].value = 60.0
		analyzer_update(&sig, &ctx)
	}

	testing.expect_value(t, ctx.stat_count, 2)

	rps, cpu: ^AttrStats
	for i in 0..<ctx.stat_count {
		switch _fixed_get(ctx.stats[i].name[:]) {
		case "rps":
			rps = &ctx.stats[i]
		case "cpu_pct":
			cpu = &ctx.stats[i]
		}
	}
	testing.expect(t, rps != nil)
	testing.expect(t, cpu != nil)
	if rps == nil || cpu == nil {
		return
	}

	// Both attributes saw 100 samples total.
	testing.expect_value(t, rps.count, 100)
	testing.expect_value(t, cpu.count, 100)

	// rps: 50 at 100, 50 at 200 → mean=150, min=100, max=200.
	_expect_close(t, rps.mean, 150.0, 1e-9)
	_expect_close(t, rps.min, 100.0, 1e-9)
	_expect_close(t, rps.max, 200.0, 1e-9)

	// cpu_pct: 50 at 30, 50 at 60 → mean=45, min=30, max=60.
	_expect_close(t, cpu.mean, 45.0, 1e-9)
	_expect_close(t, cpu.min, 30.0, 1e-9)
	_expect_close(t, cpu.max, 60.0, 1e-9)
}
