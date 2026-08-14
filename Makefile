# Build the projector on the machine it runs on.
#
# The binary is no longer committed — it is built here from source. That keeps the repo
# reviewable (a diff is a diff, not a 6 MB blob) and guarantees the binary matches the
# architecture and libc of the machine that runs it.
#
# Requires: Go >= 1.25.5, and the CaneBot_FSM_go checkout as a sibling directory:
#
#     canebot-projector-repo/
#     CaneBot_FSM_go/
#
# The projector imports the controller's own step metadata and IO pin definitions from
# there, so step titles and sensor bit order come from firmware rather than a second copy
# that drifts. If the sibling is missing the build fails immediately and says so.

BIN := projector/projector
PKG := ./cmd/projector
FSM := ../CaneBot_FSM_go

.PHONY: all projector test vet clean check-fsm

all: projector

check-fsm:
	@test -d $(FSM) || { \
	  echo "ERROR: $(FSM) not found."; \
	  echo "The projector builds against the controller's fsm and io packages."; \
	  echo "Clone CaneBot_FSM_go as a sibling of this repo, or adjust the replace"; \
	  echo "directive in go.mod."; \
	  exit 1; }

projector: check-fsm
	go build -o $(BIN) $(PKG)
	@echo "built $(BIN)"

test: check-fsm
	go test ./...

vet: check-fsm
	go vet ./...

clean:
	rm -f $(BIN)
