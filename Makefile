BUILD_UTILS   := build_utils
DOCKER         = $(shell which docker 2>/dev/null)
PUML           = $(shell which puml 2>/dev/null)
REBAR          = $(shell which rebar3 2>/dev/null)
DOCKROSS       = ./dockross
.PHONY: d_sh d_% test

## Execute within dockross build image -p 4100:4100

d_sh: $(DOCKROSS)
	$(DOCKROSS) bash

d_%: $(DOCKROSS)
	$(DOCKROSS) make $*

$(DOCKROSS):
	$(DOCKER) build -t raft-server-erlang-build-image $(BUILD_UTILS)
	$(DOCKER) run raft-server-erlang-build-image > $(DOCKROSS)
	chmod +x $(DOCKROSS)

## Normal targets: execute with local rebar3

test:
	$(REBAR) ct

clean:
	rm -rf $(SVG_FILES) $(DOCKROSS)
