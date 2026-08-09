package signal_engine

import "base:runtime"
import "core:log"
import "core:strings"
import "core:time"

Context_Entry :: struct {
	ctx:       SignalContext,
	last_seen: time.Tick,
}

Context_Store :: struct {
	entries:   map[string]^Context_Entry,
	last_purge:  time.Tick,
	allocator: runtime.Allocator,
}

context_store_create :: proc(allocator := context.allocator) -> ^Context_Store {
	store := new(Context_Store, allocator)
	store.allocator = allocator
	store.entries = make(map[string]^Context_Entry, allocator)
	store.last_purge = time.tick_now()
	return store
}

context_store_destroy :: proc(store: ^Context_Store) {
	for key, entry in store.entries {
		delete(key, store.allocator)
		free(entry, store.allocator)
	}
	delete(store.entries)
	free(store, store.allocator)
}

// Looks up or creates a SignalContext for the given signal_id.
// Updates last_seen on every call. The returned pointer stays valid for the
// life of the entry, regardless of later inserts (entries are individually
// heap-allocated; the map only ever stores pointers to them).
// NOT thread-safe — must only be called on the event loop thread.
context_get_or_create :: proc(store: ^Context_Store, signal_id: string) -> ^SignalContext {
	if entry, ok := store.entries[signal_id]; ok {
		entry.last_seen = time.tick_now()
		return &entry.ctx
	}

	entry := new(Context_Entry, store.allocator)
	entry.last_seen = time.tick_now()
	key := strings.clone(signal_id, store.allocator)
	store.entries[key] = entry
	log.debugf("created context for signal_id=%s", signal_id)
	return &entry.ctx
}

// Returns the number of active contexts (event loop thread only).
context_count :: proc(store: ^Context_Store) -> int {
	return len(store.entries)
}

// Removes all entries not seen within max_age_ms milliseconds.
// Returns the number of entries purged. Call periodically from the event loop thread.
context_purge_stale :: proc(store: ^Context_Store, max_age_ms: u64) -> int {
	max_age := time.Duration(max_age_ms) * time.Millisecond

	//short circuit if we recently ran the purge
	if time.tick_since(store.last_purge) < max_age {
		return 0
	} else {
		store.last_purge = time.tick_now()
	}

	stale := make([dynamic]string, 0, 0, store.allocator)
	defer delete(stale)

	for key, entry in store.entries {
		if time.tick_since(entry.last_seen) > max_age {
			append(&stale, key)
		}
	}

	for key in stale {
		entry := store.entries[key]
		log.infof("purging stale context: signal_id=%s", key)
		delete_key(&store.entries, key)
		delete(key, store.allocator)
		free(entry, store.allocator)
	}

	return len(stale)
}
