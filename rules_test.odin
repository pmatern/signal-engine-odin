package signal_engine

import "core:testing"

@(test)
test_rules_evaluate_empty :: proc(t: ^testing.T) {
	ctx := SignalContext{}
	ds := make(DecisionSet)
	defer delete(ds)

	rules_evaluate(&ctx, &ds)
	testing.expect(t, len(ds) == 0)
}

@(test)
test_rules_evaluate_high_latency :: proc(t: ^testing.T) {
	ctx := SignalContext{}
	ds := make(DecisionSet)
	defer delete(ds)

	latency, ok := _find_or_create_stat(&ctx, ATTR_LATENCY_MS)
	testing.expect(t, ok)

	latency.count = 100
	latency.mean = 750.0

	rules_evaluate(&ctx, &ds)
	testing.expect(t, len(ds) == 1)

	testing.expect_value(t, ds[0].rule_id, "HIGH_MEAN_LATENCY")
  	testing.expect_value(t, ds[0].action, "throttle_upstream")
}

@(test)
test_rules_evaluate_high_variance :: proc(t: ^testing.T) {
	ctx := SignalContext{}
	ds := make(DecisionSet)
	defer delete(ds)

	latency, ok := _find_or_create_stat(&ctx, ATTR_LATENCY_MS)
	testing.expect(t, ok)

	latency.count = 45
	latency.variance = 10400.0

	rules_evaluate(&ctx, &ds)
	testing.expect(t, len(ds) == 1)

	testing.expect_value(t, ds[0].rule_id, "HIGH_VARIANCE")
  	testing.expect_value(t, ds[0].action, "enable_jitter_buffer")
}

@(test)
test_rules_evaluate_error_rate_spike :: proc(t: ^testing.T) {
	ctx := SignalContext{}
	ds := make(DecisionSet)
	defer delete(ds)

	error_rate, ok := _find_or_create_stat(&ctx, ATTR_ERROR_RATE)
	testing.expect(t, ok)

	error_rate.count = 100
	error_rate.mean = 750.0

	rules_evaluate(&ctx, &ds)
	testing.expect(t, len(ds) == 1)

	testing.expect_value(t, ds[0].rule_id, "ERROR_RATE_SPIKE")
  	testing.expect_value(t, ds[0].action, "alert-oncall")
}
