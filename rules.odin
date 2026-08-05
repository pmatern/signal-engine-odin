package signal_engine
import "core:log"

// Expected attribute names in incoming signal streams. A rule silently
// produces no decisions if its attribute is absent.
ATTR_LATENCY_MS  :: "latency_ms"
ATTR_ERROR_RATE  :: "error_rate"

rule_high_mean_latency :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
	if stat := find_stat(sc, ATTR_LATENCY_MS); stat != nil && stat.count >= 10 && stat.mean > 500.0 {
    	emit(ds, "HIGH_MEAN_LATENCY", "throttle_upstream", 0.9)
	}
}

rule_high_variance :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
	if stat := find_stat(sc, ATTR_LATENCY_MS); stat != nil && stat.count >= 20 && stat.variance > 10000.0 {
		emit(ds, "HIGH_VARIANCE", "enable_jitter_buffer", 0.75)
	}
}

rule_error_rate_spike :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
	if stat := find_stat(sc, ATTR_ERROR_RATE); stat != nil && stat.count >= 5 && stat.mean > 0.05 {
		emit(ds, "ERROR_RATE_SPIKE", "alert-oncall", 0.95)
	}
}

find_stat :: proc(sc: ^SignalContext, name: string) -> ^AttrStats {
	for &stat in sc.stats[:sc.stat_count] {
    	if _fixed_get(stat.name[:]) == name {
     		return &stat
     	}
	}
	return nil
}

emit :: proc(ds: ^DecisionSet, rule: string, action: string, confidence: f64) {
	if len(ds^) >= MAX_DECISIONS {
		log.warn("MAX_DECISIONS reached, dropping rule=%s", rule)
		return
	}
	append(ds, Decision{rule_id = rule, action = action, confidence = confidence})
}


rules_evaluate :: proc(sc: ^SignalContext, ds: ^DecisionSet) {
	rule_high_mean_latency(sc, ds)
	rule_high_variance(sc, ds)
	rule_error_rate_spike(sc, ds)
	log.debug("signal_id=%s generated %d decisions", _fixed_get(sc.signal_id[:]), len(ds^))
}
