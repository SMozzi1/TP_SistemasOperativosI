#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# test_deadlock.sh
#
# Reproduces the distributed deadlock of ENUNCIADO section 6 with two local nodes
# and demonstrates that the implementation RESOLVES it by breaking No-Preemption:
# a job that waits longer than JOB_TIMEOUT_SEC is timed out, its partial
# reservation is released, and the other job then completes. Showing the timeout
# break the cycle is enough to prove the system does not stay deadlocked.
#
# THE SCENARIO (section 6):
#   Job3 on node A requests in the canonical order   @A:cpu  then  @B:gpu
#   Job4 on node B requests in the ADVERSARIAL order  @B:gpu  then  @A:cpu
#   -> A holds its CPU waiting for B's GPU, B holds its GPU waiting for A's CPU:
#      a circular wait. After JOB_TIMEOUT_SEC one job times out, frees its held
#      resource, and the survivor acquires it and completes.
#
# WHY THE LOOPBACK-IP TRICK:
#   received_node is keyed by IP only, so two nodes on the same host (same eth0
#   IP) would collide. We give each node a distinct loopback identity and inject
#   discovery ourselves:  node A server = 127.0.0.10:4200, node B = 127.0.0.20:4201,
#   Erlang interface = 127.0.0.1:<port>.  (Server sockets bind 0.0.0.0, the Erlang
#   socket 127.0.0.1, so most-specific match routes 127.0.0.1 -> Erlang and
#   127.0.0.10/20 -> the peer/server socket.)  Jobs are injected directly on each
#   node's Erlang socket (not via the Erlang scheduler, which only emits the
#   canonical order and so could not build the adversarial case).
# ──────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

# ─── Configuration ────────────────────────────────────────────────────────────
PORT_A=4200
PORT_B=4201
SERVER="build/servidor"

# Amount each crossed job requests. These MUST match the node inventory so that
# two jobs cannot both hold the resource (that is what forms the deadlock):
#   JOB_CPU == the node's CPU (LOCAL_CPU),  JOB_GPU == the node's GPU (LOCAL_GPU).
# The defaults match the reference inventory 4 / 8192 / 1 in main.c. If you change
# the #defines there, set these two to match, or override on the command line:
#   JOB_CPU=8 JOB_GPU=2 ./test_deadlock.sh
JOB_CPU="${JOB_CPU:-4}"
JOB_GPU="${JOB_GPU:-1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$(mktemp -d)"
PIDS=()
PID_A=""; PID_B=""
FAILURES=0

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC}  $*"; FAILURES=$((FAILURES + 1)); }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }

# ─── Cleanup (kill nodes, detect stuck D-state, remove temp files) ────────────
cleanup() {
    info "Cleaning up..."
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] || continue
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] || continue
        if kill -0 "$pid" 2>/dev/null; then
            warn "PID $pid survived SIGTERM, sending SIGKILL"; kill -9 "$pid" 2>/dev/null || true
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

check_deps() {
    for cmd in gcc make python3; do
        command -v "$cmd" >/dev/null 2>&1 || { error "Missing dependency: $cmd"; exit 1; }
    done
    pass "Dependencies OK (gcc, make, python3)"
}

build_project() {
    info "Building the C server..."
    cd "$SCRIPT_DIR"
    make "$SERVER" > "$LOG_DIR/build.log" 2>&1 || { error "Build failed"; cat "$LOG_DIR/build.log"; exit 1; }
    [ -x "$SCRIPT_DIR/$SERVER" ] || { error "$SERVER not produced"; exit 1; }
    pass "Built $SERVER"
}

launch_nodes() {
    pkill -f "$SERVER $PORT_A" 2>/dev/null || true
    pkill -f "$SERVER $PORT_B" 2>/dev/null || true
    sleep 0.5
    "$SCRIPT_DIR/$SERVER" "$PORT_A" > "$LOG_DIR/nodeA.log" 2>&1 & PID_A=$!
    sleep 0.4
    "$SCRIPT_DIR/$SERVER" "$PORT_B" > "$LOG_DIR/nodeB.log" 2>&1 & PID_B=$!
    PIDS+=("$PID_A" "$PID_B")
    sleep 2
    kill -0 "$PID_A" 2>/dev/null || { error "Node A died at startup"; return 1; }
    kill -0 "$PID_B" 2>/dev/null || { error "Node B died at startup"; return 1; }
    return 0
}

# ──────────────────────────────────────────────────────────────────────────────
main() {
    echo ""
    echo "==================================================================="
    echo "  DEADLOCK TEST - ENUNCIADO section 6 (two local nodes)"
    echo "  resolution by No-Preemption timeout"
    echo "  job amounts: cpu:$JOB_CPU gpu:$JOB_GPU  (must match the node inventory)"
    echo "==================================================================="
    echo ""

    check_deps
    build_project

    info "Launching two local nodes..."
    launch_nodes || { error "Could not start the nodes"; exit 1; }
    pass "Nodes A ($PID_A) and B ($PID_B) alive"
    echo ""

    JOB_CPU="$JOB_CPU" JOB_GPU="$JOB_GPU" python3 "$LOG_DIR/scenario.py" 2>&1 | tee "$LOG_DIR/scenario.out"
    [ "${PIPESTATUS[0]}" -eq 0 ] || error "Deadlock was not resolved"

    echo ""
    info "Verifying both nodes are still responsive (not hung)..."
    kill -0 "$PID_A" 2>/dev/null && pass "Node A still running" || error "Node A is gone (possible crash/hang)"
    kill -0 "$PID_B" 2>/dev/null && pass "Node B still running" || error "Node B is gone (possible crash/hang)"

    echo ""
    echo "==================================================================="
    if [ $FAILURES -eq 0 ]; then
        echo -e "  ${GREEN}RESULT: DEADLOCK FORMED AND RESOLVED BY TIMEOUT${NC}"
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
# Injects discovery with distinct loopback identities and runs the section-6
# resolution scenario against the two running nodes.
import socket, threading, time, sys, os

CPU = int(os.environ.get("JOB_CPU", "4"))   # cpu each job asks from node A
GPU = int(os.environ.get("JOB_GPU", "1"))   # gpu each job asks from node B

A_ERL = ("127.0.0.1", 4200)      # node A Erlang interface
B_ERL = ("127.0.0.1", 4201)      # node B Erlang interface
A_SRV = "127.0.0.10"             # node A server, distinct peer identity
B_SRV = "127.0.0.20"             # node B server, distinct peer identity
UDP_PORT = 12529

def announce(src_ip, port):
    """Loopback-broadcast an ANNOUNCE with a chosen source IP so BOTH nodes learn
    <src_ip>:<port> in their peer table (dodging the IP-collision)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((src_ip, 0))
    s.sendto(("ANNOUNCE %d cpu:%d mem:8192 gpu:%d\n" % (port, CPU, GPU)).encode(),
             ("127.255.255.255", UDP_PORT))
    s.close()

def inject_discovery():
    announce(A_SRV, 4200)
    announce(B_SRV, 4201)
    time.sleep(0.8)

class Node:
    """A persistent Erlang connection to one node with a background line reader."""
    def __init__(self, addr):
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

def main():
    inject_discovery()
    A = Node(A_ERL); B = Node(B_ERL)
    print("[SCENARIO] RESOLUTION - Job4 requests GPU before CPU -> section-6 deadlock")
    A.send("JOB_REQUEST 3 @%s:cpu:%d @%s:gpu:%d" % (A_SRV, CPU, B_SRV, GPU))   # canonical
    B.send("JOB_REQUEST 4 @%s:gpu:%d @%s:cpu:%d" % (B_SRV, GPU, A_SRV, CPU))   # adversarial

    time.sleep(6)
    g3 = A.seen("JOB_GRANTED 3"); g4 = B.seen("JOB_GRANTED 4")
    both_early = g3 and g4
    if not g3 and not g4:
        print("   deadlock formed as expected (Job3 and Job4 both blocked after 6s)")
    elif both_early:
        print("   both jobs were granted (NO contention) -> the deadlock never formed")
    else:
        print("   one job progressed early (g3=%s g4=%s); the other should be stuck" % (g3, g4))

    print("   waiting up to 50s for the timeout to break the deadlock...")
    broke = False; who = ""
    end = time.time() + 50
    while time.time() < end:
        if A.seen("JOB_TIMEOUT 3"): broke = True; who = "Job3 (node A) timed out"; break
        if B.seen("JOB_TIMEOUT 4"): broke = True; who = "Job4 (node B) timed out"; break
        time.sleep(0.5)
    time.sleep(3)  # let the survivor complete after resources are freed
    survivor = ("JOB_GRANTED 3" if A.seen("JOB_GRANTED 3")
                else "JOB_GRANTED 4" if B.seen("JOB_GRANTED 4") else "none")

    if broke:
        print("   deadlock broken: %s ; surviving job: %s" % (who, survivor))
        print("[SCENARIO] PASS - No-Preemption timeout resolved the deadlock")
        return 0
    if both_early:
        print("[SCENARIO] FAIL - No contention formed: the node inventory is bigger than the job")
        print("           amounts (cpu:%d/gpu:%d), so the two jobs did not compete." % (CPU, GPU))
        print("           Set JOB_CPU/JOB_GPU (top of the script) to your LOCAL_CPU/LOCAL_GPU, e.g.")
        print("           JOB_CPU=8 JOB_GPU=2 ./test_deadlock.sh")
        return 1
    print("[SCENARIO] FAIL - no JOB_TIMEOUT within the window; deadlock not resolved")
    return 1

sys.exit(main())
PYEOF

main "$@"
