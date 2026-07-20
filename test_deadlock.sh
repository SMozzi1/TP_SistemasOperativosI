#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# test_deadlock.sh
#
# Runs two local instances of the C server (different ports) with their
# respective Erlang schedulers, and verifies that the system DOES NOT deadlock:
#   1. Both servers start and listen.
#   2. Both Erlang schedulers connect successfully.
#   3. UDP broadcasts discover cross-node resources.
#   4. Jobs request remote resources and the system processes them.
#   5. After an observation period, all processes terminate cleanly.
#
# SUCCESS Criteria:
#   - Both C servers respond to TCP connection (they are not hung).
#   - The Erlang scheduler of at least one node processes at least one job with
#     a GRANTED, DENIED, or TIMEOUT state (i.e., the system made progress).
#   - All processes can be terminated with SIGTERM/SIGKILL without becoming
#     zombies or remaining in D state (uninterruptible sleep).
#
# FAILURE Criteria (suspected deadlock):
#   - A C server cannot be contacted after starting.
#   - No jobs receive a response during the observation window.
#   - A process remains in D state or does not respond to SIGKILL.
# ──────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ─── Configuration ────────────────────────────────────────────────────────────
PORT1=4200
PORT2=4201
OBSERVE_SEC=60
LOG_DIR="/tmp/deadlock_test_$$"
PIDS=()
FAILURES=0

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ─── Helpers ──────────────────────────────────────────────────────────────────
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC}  $*"; FAILURES=$((FAILURES + 1)); }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }

# ── Kill stale processes that might block ports ───────────────────────────────
kill_stale_processes() {
    info "Killing stale processes on ports $PORT1 and $PORT2..."
    local stale
    stale=$(pgrep -f "servidor $PORT1" 2>/dev/null) || true
    if [ -n "$stale" ]; then
        warn "Stale processes on port $PORT1: $stale"
        echo "$stale" | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
    stale=$(pgrep -f "servidor $PORT2" 2>/dev/null) || true
    if [ -n "$stale" ]; then
        warn "Stale processes on port $PORT2: $stale"
        echo "$stale" | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
}

cleanup() {
    info "Cleaning up processes and temporary files..."

    # Kill all known processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    sleep 2

    # Force kill any processes still alive
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            warn "Process $pid did not die with SIGTERM, sending SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    # Verify no process is left in D state
    for pid in "${PIDS[@]}"; do
        if [ -d "/proc/$pid" ]; then
            state=$(cat "/proc/$pid/status" 2>/dev/null | grep '^State:' | awk '{print $2}') || true
            if [ "$state" = "D" ]; then
                error "Process $pid left in D state - possible kernel-level deadlock"
            fi
        fi
    done

    # Kill remaining processes by name (fallback)
    pkill -f "servidor $PORT1" 2>/dev/null || true
    pkill -f "servidor $PORT2" 2>/dev/null || true

    # Preserve logs for investigation, clear only the temporal ones
    rm -f "$LOG_DIR/server1.log" "$LOG_DIR/server2.log" \
          "$LOG_DIR/erlang1.log" "$LOG_DIR/erlang2.log" \
          "$LOG_DIR/build.log" 2>/dev/null || true
    info "Cleanup completed."
}

trap cleanup EXIT

# ─── Verify dependencies ──────────────────────────────────────────────────────
check_deps() {
    info "Verifying dependencies..."
    for cmd in gcc make erlc nc; do
        if ! command -v "$cmd" &>/dev/null; then
            error "Missing dependency: $cmd"
            exit 1
        fi
    done
    pass "Dependencies verified"
}

# ─── Compile ──────────────────────────────────────────────────────────────────
build_project() {
    info "Compiling the project..."
    cd "$SCRIPT_DIR"
    make clean 2>/dev/null || true
    make all 2>&1 | tee "$LOG_DIR/build.log"
    if [ ! -f "$SCRIPT_DIR/servidor" ]; then
        error "C server compilation failed"
        exit 1
    fi
    pass "C server compiled successfully"
}

# ─── Create second instance of Erlang scheduler (different port) ──────────────
create_second_scheduler() {
    info "Creating Erlang scheduler for port $PORT2..."
    mkdir -p "$LOG_DIR/erlang2"

    sed "s/-define(PORT, 4200)/-define(PORT, $PORT2)/" \
        "$SCRIPT_DIR/Scheduler_Erlang/scheduler.erl" \
        > "$LOG_DIR/erlang2/scheduler.erl"

    cp "$SCRIPT_DIR/Scheduler_Erlang/scheduler_utils.erl" \
       "$LOG_DIR/erlang2/scheduler_utils.erl"

    cd "$SCRIPT_DIR/Scheduler_Erlang"
    erlc scheduler.erl scheduler_utils.erl 2>&1 || true
    cp scheduler.beam scheduler_utils.beam "$LOG_DIR/erlang2/" 2>/dev/null || true

    cd "$LOG_DIR/erlang2"
    erlc scheduler.erl scheduler_utils.erl 2>&1
    pass "Erlang schedulers compiled"
}

# ─── Verify if a port is listening ────────────────────────────────────────────
wait_for_port() {
    local port=$1
    local label=$2
    local timeout=${3:-10}
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            return 0
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    error "$label is not listening on port $port after ${timeout}s"
    return 1
}

# ─── Verify TCP connectivity to the server ────────────────────────────────────
test_tcp_connection() {
    local port=$1
    local label=$2

    if timeout 2 bash -c "echo > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
        pass "TCP connection to $label (port $port) successful"
        return 0
    else
        error "Could not connect to $label (port $port)"
        return 1
    fi
}

# ─── Verify C server processes are still alive ────────────────────────────────
check_servers_alive() {
    local alive=0
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            alive=$((alive + 1))
        fi
    done
    if [ $alive -ge 2 ]; then
        pass "Both C servers are still running ($alive processes alive)"
        return 0
    else
        error "Only $alive out of 2 C servers are still running"
        return 1
    fi
}

# ─── Count events in Erlang log ───────────────────────────────────────────────
count_erlang_events() {
    local logfile=$1
    local event_type=$2

    if [ ! -f "$logfile" ]; then
        echo 0
        return
    fi
    # grep -c returns exit 1 when nothing is found; with pipefail that kills the script.
    # We use an independent subshell to isolate the exit code.
    local count
    count=$(grep -c "$event_type" "$logfile" 2>/dev/null) || count=0
    echo "$count"
}

# ─── Analyze C server logs for issues ─────────────────────────────────────────
analyze_server_logs() {
    local logfile=$1
    local label=$2

    if [ ! -f "$logfile" ]; then
        warn "$label: log file not found"
        return 0
    fi

    local segfaults
    segfaults=$(grep -c "Segmentation fault\|SIGSEGV" "$logfile" 2>/dev/null) || segfaults=0
    if [ "$segfaults" -gt 0 ]; then
        error "$label: $segfaults segfaults detected"
        return 1
    fi

    local aborts
    aborts=$(grep -c "Aborted\|SIGABRT\|assert" "$logfile" 2>/dev/null) || aborts=0
    if [ "$aborts" -gt 0 ]; then
        error "$label: $aborts aborts/asserts detected"
        return 1
    fi

    return 0
}

# ──────────────────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "=============================================="
    echo "  DEADLOCK TEST - 2 Local Nodes"
    echo "=============================================="
    echo ""

    mkdir -p "$LOG_DIR"

    # ── Step 0: Kill stale processes and compile ──
    kill_stale_processes
    check_deps
    build_project
    create_second_scheduler

    # ── Step 1: Start C Server #1 (port $PORT1) ──
    info "Starting C Server #1 on port $PORT1..."
    "$SCRIPT_DIR/servidor" "$PORT1" > "$LOG_DIR/server1.log" 2>&1 &
    PIDS+=($!)
    info "  Server 1 PID = ${PIDS[0]}"

    # ── Step 2: Start C Server #2 (port $PORT2) ──
    info "Starting C Server #2 on port $PORT2..."
    "$SCRIPT_DIR/servidor" "$PORT2" > "$LOG_DIR/server2.log" 2>&1 &
    PIDS+=($!)
    info "  Server 2 PID = ${PIDS[1]}"

    # ── Step 3: Wait for both servers to listen ──
    sleep 2
    info "Verifying that servers are listening..."
    wait_for_port "$PORT1" "Server 1" 10 || true
    wait_for_port "$PORT2" "Server 2" 10 || true

    # ── Step 4: Verify TCP connectivity ──
    info "Verifying TCP connectivity..."
    test_tcp_connection "$PORT1" "Server 1" || true
    test_tcp_connection "$PORT2" "Server 2" || true

    # ── Step 5: Start Erlang Scheduler #1 ──
    info "Starting Erlang Scheduler #1 (connecting to port $PORT1)..."
    cd "$SCRIPT_DIR/Scheduler_Erlang"
    rm -f scheduler.log
    erl -noshell -pa "$SCRIPT_DIR/Scheduler_Erlang" \
        -eval "scheduler:start()." \
        > "$LOG_DIR/erlang1.log" 2>&1 &
    PIDS+=($!)
    info "  Erlang 1 PID = ${PIDS[2]}"

    # ── Step 6: Start Erlang Scheduler #2 ──
    info "Starting Erlang Scheduler #2 (connecting to port $PORT2)..."
    cd "$LOG_DIR/erlang2"
    rm -f scheduler.log
    erl -noshell -pa "$LOG_DIR/erlang2" \
        -eval "scheduler:start()." \
        > "$LOG_DIR/erlang2.log" 2>&1 &
    PIDS+=($!)
    info "  Erlang 2 PID = ${PIDS[3]}"

    # ── Step 7: Observation Window ──
    info "Waiting ${OBSERVE_SEC}s for schedulers to generate and process jobs..."
    info "(Schedulers generate jobs every ~5s; jobs can take up to 30s to timeout)"
    echo ""

    ELAPSED=0
    INTERVAL=10
    while [ $ELAPSED -lt $OBSERVE_SEC ]; do
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))

        check_servers_alive || true

        if [ -f "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" ]; then
            g1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "granted")
            d1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "denied")
            t1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "timeout")
            info "  [Erlang1 @:$PORT1] granted=$g1 denied=$d1 timeout=$t1"
        fi

        if [ -f "$LOG_DIR/erlang2/scheduler.log" ]; then
            g2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "granted")
            d2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "denied")
            t2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "timeout")
            info "  [Erlang2 @:$PORT2] granted=$g2 denied=$d2 timeout=$t2"
        fi

        echo ""
    done

    # ── Step 8: Final Analysis ──
    echo ""
    echo "=============================================="
    echo "  FINAL RESULTS"
    echo "=============================================="
    echo ""

    info "Verifying that C servers are still alive..."
    check_servers_alive || true

    info "Verifying final TCP connectivity..."
    test_tcp_connection "$PORT1" "Server 1" || true
    test_tcp_connection "$PORT2" "Server 2" || true

    info "Analyzing Erlang logs..."

    G1=0; D1=0; T1=0
    if [ -f "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" ]; then
        G1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "granted")
        D1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "denied")
        T1=$(count_erlang_events "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "timeout")
    fi
    TOTAL1=$((G1 + D1 + T1))
    info "  Erlang1 (port $PORT1): granted=$G1 denied=$D1 timeout=$T1 total=$TOTAL1"

    G2=0; D2=0; T2=0
    if [ -f "$LOG_DIR/erlang2/scheduler.log" ]; then
        G2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "granted")
        D2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "denied")
        T2=$(count_erlang_events "$LOG_DIR/erlang2/scheduler.log" "timeout")
    fi
    TOTAL2=$((G2 + D2 + T2))
    info "  Erlang2 (port $PORT2): granted=$G2 denied=$D2 timeout=$T2 total=$TOTAL2"

    info "Analyzing C server logs..."
    analyze_server_logs "$LOG_DIR/server1.log" "Server 1" || true
    analyze_server_logs "$LOG_DIR/server2.log" "Server 2" || true

    echo ""
    if [ $TOTAL1 -gt 0 ] && [ $TOTAL2 -gt 0 ]; then
        pass "Both schedulers processed at least one job (no deadlock)"
    elif [ $TOTAL1 -gt 0 ] || [ $TOTAL2 -gt 0 ]; then
        pass "At least one scheduler processed jobs (no deadlock)"
    else
        error "No scheduler processed jobs - possible deadlock or connection failure"
    fi

    info "Verifying that servers are not hanging..."
    check_servers_alive && pass "Servers are responding correctly" || true

    info "Terminating processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    sleep 3

    STILL_ALIVE=0
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            warn "Process $pid did not respond to SIGTERM, sending SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
            STILL_ALIVE=$((STILL_ALIVE + 1))
        fi
    done

    if [ $STILL_ALIVE -eq 0 ]; then
        pass "All processes terminated cleanly (no deadlock)"
    else
        error "$STILL_ALIVE processes could not be terminated - possible deadlock"
    fi

    # ── Copy logs for post-investigation ──
    FINAL_LOG_DIR="$SCRIPT_DIR/test_logs_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$FINAL_LOG_DIR"
    cp "$LOG_DIR/server1.log" "$FINAL_LOG_DIR/" 2>/dev/null || true
    cp "$LOG_DIR/server2.log" "$FINAL_LOG_DIR/" 2>/dev/null || true
    cp "$LOG_DIR/erlang1.log" "$FINAL_LOG_DIR/" 2>/dev/null || true
    cp "$LOG_DIR/erlang2.log" "$FINAL_LOG_DIR/" 2>/dev/null || true
    cp "$LOG_DIR/build.log"   "$FINAL_LOG_DIR/" 2>/dev/null || true
    cp "$SCRIPT_DIR/Scheduler_Erlang/scheduler.log" "$FINAL_LOG_DIR/erlang1_scheduler.log" 2>/dev/null || true
    cp "$LOG_DIR/erlang2/scheduler.log" "$FINAL_LOG_DIR/erlang2_scheduler.log" 2>/dev/null || true
    info "Logs saved to: $FINAL_LOG_DIR/"

    echo ""
    echo "=============================================="
    if [ $FAILURES -eq 0 ]; then
        echo -e "  ${GREEN}RESULT: NO DEADLOCK DETECTED${NC}"
    else
        echo -e "  ${RED}RESULT: $FAILURES FAILURE(S) DETECTED${NC}"
    fi
    echo "=============================================="
    echo ""
    echo "  Jobs processed (scheduler 1): $TOTAL1 (granted=$G1 denied=$D1 timeout=$T1)"
    echo "  Jobs processed (scheduler 2): $TOTAL2 (granted=$G2 denied=$D2 timeout=$T2)"
    echo "  Observation window: ${OBSERVE_SEC}s"
    echo ""

    rm -rf "$LOG_DIR"
    exit $FAILURES
}

main "$@"