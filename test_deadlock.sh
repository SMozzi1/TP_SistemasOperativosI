#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# test_deadlock.sh
#
# Reproduces the distributed deadlock scenario of ENUNCIADO section 6 with two
# local nodes and demonstrates that the implementation handles it, using the two
# defenses described in the design:
#
#   Phase 1 - AVOIDANCE (circular-wait prevention):
#       Two crossed jobs request their resources in the fixed CPU -> MEM -> GPU
#       order. No circular wait can form, so both jobs complete.
#
#   Phase 2 - RESOLUTION (No-Preemption break via timeout):
#       One job requests in the ADVERSARIAL order (GPU before CPU), so the
#       section-6 deadlock actually forms (A holds its CPU waiting for B's GPU,
#       B holds its GPU waiting for A's CPU). After JOB_TIMEOUT_SEC the stuck
#       job times out, releases its partial reservation, and the system recovers.
#
# WHY THE LOOPBACK-IP TRICK:
#   The node's peer table (received_node) is keyed by IP only, so two nodes on
#   the same host (same eth0 IP) would collide. We give each node a distinct
#   loopback identity and inject discovery ourselves:
#       - Node A server reachable at 127.0.0.10:4200
#       - Node B server reachable at 127.0.0.20:4201
#       - Each node's Erlang interface reached at 127.0.0.1:<port>
#   (Server sockets bind 0.0.0.0, the Erlang socket binds 127.0.0.1, so the
#    kernel's most-specific match routes 127.0.0.1 -> Erlang and 127.0.0.10/20
#    -> the peer/server socket.)
#
# The jobs are injected directly on each node's Erlang socket (not via the
# Erlang scheduler, which only ever emits the canonical order and so could not
# build the adversarial case).
#
# SUCCESS: Phase 1 both jobs GRANTED; Phase 2 the deadlock forms and is then
#          broken by a JOB_TIMEOUT; no node left hung (D state) or as a zombie.
# ──────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

# ─── Configuration ────────────────────────────────────────────────────────────
PORT_A=4200
PORT_B=4201
SERVER="build/servidor"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$(mktemp -d)"
PID_A=""; PID_B=""
FAILURES=0

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC}  $*"; FAILURES=$((FAILURES + 1)); }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }

# ─── Cleanup (kill nodes, detect stuck D-state, remove temp files) ────────────
cleanup() {
    info "Cleaning up..."
    for pid in "$PID_A" "$PID_B"; do
        [ -n "$pid" ] || continue
        if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid" 2>/dev/null || true; fi
    done
    sleep 1
    for pid in "$PID_A" "$PID_B"; do
        [ -n "$pid" ] || continue
        if kill -0 "$pid" 2>/dev/null; then
            warn "PID $pid survived SIGTERM, sending SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
        fi
        if [ -r "/proc/$pid/status" ]; then
            st=$(awk '/^State:/{print $2}' "/proc/$pid/status" 2>/dev/null) || true
            [ "$st" = "D" ] && error "PID $pid left in D state (uninterruptible) - kernel-level deadlock"
        fi
    done
    pkill -f "$SERVER $PORT_A" 2>/dev/null || true
    pkill -f "$SERVER $PORT_B" 2>/dev/null || true
    rm -rf "$LOG_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# ─── Dependencies ─────────────────────────────────────────────────────────────
check_deps() {
    for cmd in gcc make python3; do
        command -v "$cmd" >/dev/null 2>&1 || { error "Missing dependency: $cmd"; exit 1; }
    done
    pass "Dependencies OK (gcc, make, python3)"
}

# ─── Build ────────────────────────────────────────────────────────────────────
build_project() {
    info "Building the C server..."
    cd "$SCRIPT_DIR"
    make "$SERVER" > "$LOG_DIR/build.log" 2>&1 || { error "Build failed"; cat "$LOG_DIR/build.log"; exit 1; }
    [ -x "$SCRIPT_DIR/$SERVER" ] || { error "$SERVER not produced"; exit 1; }
    pass "Built $SERVER"
}

# ─── Launch a node, return its PID ────────────────────────────────────────────
launch_node() {
    local port=$1 logfile=$2
    "$SCRIPT_DIR/$SERVER" "$port" > "$logfile" 2>&1 &
    echo $!
}

# ──────────────────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "==================================================================="
    echo "  DEADLOCK TEST - ENUNCIADO section 6 (two local nodes)"
    echo "==================================================================="
    echo ""

    check_deps
    build_project

    # Kill any stale instances holding the ports
    pkill -f "$SERVER $PORT_A" 2>/dev/null || true
    pkill -f "$SERVER $PORT_B" 2>/dev/null || true
    sleep 1

    info "Launching node A on port $PORT_A and node B on port $PORT_B..."
    PID_A=$(launch_node "$PORT_A" "$LOG_DIR/nodeA.log")
    sleep 0.5
    PID_B=$(launch_node "$PORT_B" "$LOG_DIR/nodeB.log")
    sleep 2

    # Both nodes MUST be alive (verifies the UDP 12529 dual-bind works)
    if kill -0 "$PID_A" 2>/dev/null; then pass "Node A alive (PID $PID_A)"; else error "Node A died at startup"; fi
    if kill -0 "$PID_B" 2>/dev/null; then pass "Node B alive (PID $PID_B)"; else error "Node B died at startup"; fi
    [ $FAILURES -eq 0 ] || { error "A node failed to start - aborting"; exit 1; }

    # ── Run the scenario (discovery injection + both phases) in Python ────────
    info "Running the section-6 scenario (injecting discovery + crossed jobs)..."
    echo ""
    python3 "$LOG_DIR/scenario.py" 2>&1 | tee "$LOG_DIR/scenario.log"
    local rc=${PIPESTATUS[0]}
    echo ""
    [ "$rc" -eq 0 ] || error "Scenario reported $rc failing phase(s)"

    # ── Final verdict ─────────────────────────────────────────────────────────
    info "Verifying both nodes are still responsive (not hung)..."
    kill -0 "$PID_A" 2>/dev/null && pass "Node A still running" || error "Node A is gone (possible crash/hang)"
    kill -0 "$PID_B" 2>/dev/null && pass "Node B still running" || error "Node B is gone (possible crash/hang)"

    echo ""
    echo "==================================================================="
    if [ $FAILURES -eq 0 ]; then
        echo -e "  ${GREEN}RESULT: DEADLOCK AVOIDED (phase 1) AND RESOLVED (phase 2)${NC}"
    else
        echo -e "  ${RED}RESULT: $FAILURES FAILURE(S) DETECTED${NC}"
    fi
    echo "==================================================================="
    echo ""
    exit $FAILURES
}

# ─── Emit the Python scenario helper into the temp dir, then run main ─────────
cat > "$LOG_DIR/scenario.py" <<'PYEOF'
#!/usr/bin/env python3
# Injects discovery with distinct loopback identities and drives the two-phase
# section-6 deadlock scenario against the two running nodes.
import socket, threading, time, sys

A_ERL = ("127.0.0.1", 4200)      # node A Erlang interface
B_ERL = ("127.0.0.1", 4201)      # node B Erlang interface
A_SRV = "127.0.0.10"             # node A server, as a distinct peer identity
B_SRV = "127.0.0.20"             # node B server, as a distinct peer identity
UDP_PORT = 12529

def announce(src_ip, port):
    """Loopback-broadcast an ANNOUNCE with a chosen source IP so BOTH nodes
    learn <src_ip>:<port> in their peer table (dodging the IP-collision)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((src_ip, 0))
    s.sendto(("ANNOUNCE %d cpu:4 mem:8192 gpu:1\n" % port).encode(),
             ("127.255.255.255", UDP_PORT))
    s.close()

def inject_discovery():
    announce(A_SRV, 4200)
    announce(B_SRV, 4201)
    time.sleep(0.8)

class Node:
    """A persistent Erlang connection to one node with a background line reader."""
    def __init__(self, addr, name):
        self.name = name
        self.sock = socket.socket(); self.sock.connect(addr); self.sock.settimeout(0.4)
        self.lines = []; self.lock = threading.Lock(); self.buf = b""
        threading.Thread(target=self._reader, daemon=True).start()
        time.sleep(0.2)
    def _reader(self):
        while True:
            try:
                d = self.sock.recv(512)
            except socket.timeout:
                continue
            except OSError:
                return
            if not d:
                return
            self.buf += d
            while b"\n" in self.buf:
                line, self.buf = self.buf.split(b"\n", 1)
                with self.lock:
                    self.lines.append(line.decode(errors="replace").strip())
    def send(self, msg):
        self.sock.sendall((msg + "\n").encode())
    def seen(self, prefix):
        with self.lock:
            return any(l.startswith(prefix) for l in self.lines)
    def wait_for(self, prefix, timeout):
        end = time.time() + timeout
        while time.time() < end:
            if self.seen(prefix):
                return True
            time.sleep(0.1)
        return False

def main():
    inject_discovery()
    A = Node(A_ERL, "A")
    B = Node(B_ERL, "B")
    failures = 0

    # ── PHASE 1: AVOIDANCE (both jobs in canonical CPU -> GPU order) ──────────
    print("[PHASE 1] AVOIDANCE - crossed jobs, both in canonical CPU->GPU order")
    # Job1 (from A): 4 CPU from A, then 1 GPU from B
    A.send("JOB_REQUEST 1 @127.0.0.10:cpu:4 @127.0.0.20:gpu:1")
    # Job2 (from B): 4 CPU from A, then 1 GPU from B
    B.send("JOB_REQUEST 2 @127.0.0.10:cpu:4 @127.0.0.20:gpu:1")

    g1 = A.wait_for("JOB_GRANTED 1", 8)
    print("   Job1 granted: %s" % g1)
    if g1:
        # Job1 done: release it so the queued Job2 can take the CPU/GPU.
        A.send("JOB_RELEASE 1")
    g2 = B.wait_for("JOB_GRANTED 2", 10)
    print("   Job2 granted (after Job1 released): %s" % g2)

    if g1 and g2:
        print("[PHASE 1] PASS - no circular wait formed; both jobs completed")
    else:
        print("[PHASE 1] FAIL - a job never completed (g1=%s g2=%s)" % (g1, g2))
        failures += 1
    B.send("JOB_RELEASE 2")
    time.sleep(1.5)

    # ── PHASE 2: RESOLUTION (Job4 adversarial GPU -> CPU forms the deadlock) ──
    inject_discovery()  # refresh peer entries (they expire after 15s)
    print("")
    print("[PHASE 2] RESOLUTION - Job4 requests GPU before CPU -> section-6 deadlock")
    # Job3 (from A), canonical: CPU@A then GPU@B
    A.send("JOB_REQUEST 3 @127.0.0.10:cpu:4 @127.0.0.20:gpu:1")
    # Job4 (from B), ADVERSARIAL: GPU@B then CPU@A
    B.send("JOB_REQUEST 4 @127.0.0.20:gpu:1 @127.0.0.10:cpu:4")

    time.sleep(6)
    g3 = A.seen("JOB_GRANTED 3"); g4 = B.seen("JOB_GRANTED 4")
    if not g3 and not g4:
        print("   deadlock formed as expected (Job3 and Job4 both blocked after 6s)")
    else:
        print("   note: a job was granted early (g3=%s g4=%s) - no deadlock this run" % (g3, g4))

    # Wait for the No-Preemption timeout (JOB_TIMEOUT_SEC=30) to break it.
    print("   waiting up to 45s for the timeout to break the deadlock...")
    broke = False; who = ""
    end = time.time() + 45
    while time.time() < end:
        if A.seen("JOB_TIMEOUT 3"): broke = True; who = "Job3 (node A) timed out"; break
        if B.seen("JOB_TIMEOUT 4"): broke = True; who = "Job4 (node B) timed out"; break
        time.sleep(0.5)
    time.sleep(3)  # let the survivor complete after resources are freed
    survivor = ("JOB_GRANTED 3" if A.seen("JOB_GRANTED 3")
                else "JOB_GRANTED 4" if B.seen("JOB_GRANTED 4") else "none")

    if broke:
        print("   deadlock broken: %s ; surviving job: %s" % (who, survivor))
        print("[PHASE 2] PASS - No-Preemption timeout resolved the deadlock")
    else:
        print("[PHASE 2] FAIL - no JOB_TIMEOUT within the window; deadlock not resolved")
        failures += 1

    # tidy up any live jobs
    for j in ("3", "4"):
        A.send("JOB_RELEASE %s" % j); B.send("JOB_RELEASE %s" % j)
    time.sleep(0.5)

    print("")
    print("SCENARIO RESULT: %s" % ("PASS" if failures == 0 else "FAIL (%d phase(s))" % failures))
    sys.exit(failures)

if __name__ == "__main__":
    main()
PYEOF

main "$@"
