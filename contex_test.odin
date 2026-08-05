package signal_engine

import "core:testing"
import "core:time"

// --- create_or_get returns non-nil for a new id ------------------------------

@(test)
test_create_returns_nonnull :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	testing.expect(t, context_get_or_create(store, "svc-a") != nil)
}

// --- second call with same id returns the same pointer -----------------------

@(test)
test_same_id_same_ptr :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	a := context_get_or_create(store, "svc-b")
	b := context_get_or_create(store, "svc-b")
	testing.expect_value(t, a, b)
}

// --- context_count tracks every distinct id ----------------------------------

@(test)
test_count_reflects_inserts :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	testing.expect_value(t, context_count(store), 0)
	context_get_or_create(store, "x")
	testing.expect_value(t, context_count(store), 1)
	context_get_or_create(store, "y")
	testing.expect_value(t, context_count(store), 2)
	context_get_or_create(store, "x") // duplicate — no new entry
	testing.expect_value(t, context_count(store), 2)
}

// --- independent stores have independent counts -------------------------------
//
// The C version has a single global store and tests that destroy+reinit
// resets the count to 0. There's no global here, so the equivalent invariant
// is that a fresh store starts empty regardless of what other stores hold.

@(test)
test_independent_stores_have_independent_counts :: proc(t: ^testing.T) {
	store_a := context_store_create()
	defer context_store_destroy(store_a)
	context_get_or_create(store_a, "svc-c")
	context_get_or_create(store_a, "svc-d")
	testing.expect_value(t, context_count(store_a), 2)

	store_b := context_store_create()
	defer context_store_destroy(store_b)
	testing.expect_value(t, context_count(store_b), 0)
}

// --- purge_stale removes entries older than the TTL --------------------------
//
// Sleep 2ms so the entry is at least 1ms old, then purge with TTL = 1.

@(test)
test_purge_stale_removes_old :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	context_get_or_create(store, "stale")
	testing.expect_value(t, context_count(store), 1)
	time.sleep(2 * time.Millisecond)
	context_purge_stale(store, 1)
	testing.expect_value(t, context_count(store), 0)
}

// --- purge_stale with a large TTL preserves recent entries -------------------

@(test)
test_purge_preserves_recent :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	context_get_or_create(store, "fresh")
	testing.expect_value(t, context_count(store), 1)
	context_purge_stale(store, 60000) // 60s TTL — nothing should be evicted
	testing.expect_value(t, context_count(store), 1)
}

// --- purge removes only stale entries, not fresh ones ------------------------

@(test)
test_purge_selective :: proc(t: ^testing.T) {
	store := context_store_create()
	defer context_store_destroy(store)

	context_get_or_create(store, "old")
	time.sleep(2 * time.Millisecond)
	// Touch "new" after the sleep so it has a recent timestamp.
	context_get_or_create(store, "new")
	testing.expect_value(t, context_count(store), 2)

	context_purge_stale(store, 1) // "old" is > 1ms old; "new" is not
	testing.expect_value(t, context_count(store), 1)
	testing.expect(t, context_get_or_create(store, "new") != nil)
}
