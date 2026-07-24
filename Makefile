# ─── C COMPILER CONFIGURATION ─────────────────────────────────────
CC = gcc
CFLAGS = -Wall -Wextra -g -pthread -D_GNU_SOURCE -I. -IResourceManager
LDFLAGS = -pthread

# ─── OUTPUT LAYOUT ────────────────────────────────────────────────
# The final executable lives in build/, and every source path/foo.c is
# compiled into its own subfolder: build/foo/foo.o
BUILD_DIR = build
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

# path/foo.c -> build/foo/foo.o
OBJS = $(foreach s,$(SRCS),$(BUILD_DIR)/$(basename $(notdir $(s)))/$(notdir $(s:.c=.o)))

# ─── ERLANG CONFIGURATION ─────────────────────────────────────────
ERLC = erlc
ERL_SRCS = Scheduler_Erlang/scheduler.erl Scheduler_Erlang/scheduler_utils.erl
BEAM_FILES = $(ERL_SRCS:.erl=.beam)

# ─── BUILD RULES ──────────────────────────────────────────────────

# Default rule: build the C executable and the Erlang beams.
all: $(TARGET) $(BEAM_FILES)

# Link every object into build/servidor.
$(TARGET): $(OBJS)
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

# Generate one compile rule per source: build/<name>/<name>.o : <src>.
# (The object path does not encode the source directory, so a single pattern
#  rule cannot map it back — we emit an explicit rule per source instead.)
define OBJ_RULE
$(BUILD_DIR)/$(basename $(notdir $(1)))/$(notdir $(1:.c=.o)): $(1)
	@mkdir -p $$(dir $$@)
	$$(CC) $$(CFLAGS) -c $$< -o $$@
endef
$(foreach s,$(SRCS),$(eval $(call OBJ_RULE,$(s))))

# Erlang .erl -> .beam
%.beam: %.erl
	$(ERLC) $<

# Remove the whole build/ tree plus the Erlang beams.
clean:
	rm -rf $(BUILD_DIR)
	rm -f $(BEAM_FILES) erl_crash.dump

.PHONY: all clean
