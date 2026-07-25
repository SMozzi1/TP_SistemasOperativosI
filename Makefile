# ─── C COMPILER CONFIGURATION ─────────────────────────────────────
CC = gcc
CFLAGS = -Wall -Wextra -g -pthread -D_GNU_SOURCE -I. -IResourceManager
LDFLAGS = -pthread

# ─── OUTPUT LAYOUT ────────────────────────────────────────────────
# Everything generated lives under build/:
#   build/servidor          the linked executable
#   build/obj/foo.o         every object file, together in one folder
#   build/erlang_beams/*.beam   the compiled Erlang modules
BUILD_DIR = build
OBJ_DIR   = $(BUILD_DIR)/obj
BEAM_DIR  = $(BUILD_DIR)/erlang_beams
TARGET    = $(BUILD_DIR)/servidor

# ─── C SOURCES ────────────────────────────────────────────────────
SRCS = main.c \
       agent/agent.c \
       agent/communications.c \
       agent/utils.c \
       agent/loopfunc.c \
       agent/read_instructions.c \
       agent/timer.c \
       ResourceManager/hash.c \
       ResourceManager/hashtable.c \
       ResourceManager/resource_queue.c

# path/foo.c -> build/obj/foo.o  (all objects share one folder)
OBJS = $(foreach s,$(SRCS),$(OBJ_DIR)/$(notdir $(s:.c=.o)))

# ─── ERLANG CONFIGURATION ─────────────────────────────────────────
ERLC = erlc
ERL_SRCS = Scheduler_Erlang/scheduler.erl Scheduler_Erlang/scheduler_utils.erl
# path/foo.erl -> build/erlang_beams/foo.beam
BEAM_FILES = $(foreach e,$(ERL_SRCS),$(BEAM_DIR)/$(notdir $(e:.erl=.beam)))

# ─── BUILD RULES ──────────────────────────────────────────────────

# Default rule: build the C executable and the Erlang beams.
all: $(TARGET) $(BEAM_FILES)

# Link every object into build/servidor.
$(TARGET): $(OBJS)
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

# Generate one compile rule per source: build/obj/<name>.o : <src>.
# (The object path does not encode the source directory, so a single pattern
#  rule cannot map it back — we emit an explicit rule per source instead.)
define OBJ_RULE
$(OBJ_DIR)/$(notdir $(1:.c=.o)): $(1)
	@mkdir -p $(OBJ_DIR)
	$$(CC) $$(CFLAGS) -c $$< -o $$@
endef
$(foreach s,$(SRCS),$(eval $(call OBJ_RULE,$(s))))

# Generate one rule per Erlang source: build/erlang_beams/<name>.beam : <src>.
define BEAM_RULE
$(BEAM_DIR)/$(notdir $(1:.erl=.beam)): $(1)
	@mkdir -p $(BEAM_DIR)
	$$(ERLC) -o $(BEAM_DIR) $$<
endef
$(foreach e,$(ERL_SRCS),$(eval $(call BEAM_RULE,$(e))))

# Remove the whole build/ tree (objects, executable and beams all live there).
clean:
	rm -rf $(BUILD_DIR)
	rm -f erl_crash.dump

.PHONY: all clean
