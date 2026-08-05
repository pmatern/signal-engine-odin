package signal_engine

import "core:testing"

@(test)
test_default_config :: proc(t: ^testing.T) {
	cfg, ok := load_config_from_file("./test-data/config.json")
	testing.expect(t, ok)

	defer destroy_config(&cfg)
	testing.expect(t, &cfg != nil)
}
