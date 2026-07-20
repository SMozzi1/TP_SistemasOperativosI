#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENT_SCRIPT="$ROOT_DIR/scripts/run_agent.sh"
EBIN_DIR="$ROOT_DIR/erlang_scheduler/ebin"

if [ ! -x "$AGENT_SCRIPT" ]; then
  echo "Error: run_agent.sh not found or not executable"
  exit 1
fi

if [ ! -d "$EBIN_DIR" ]; then
  echo "Error: Erlang ebin directory not found: $EBIN_DIR"
  exit 1
fi

if ! find "$EBIN_DIR" -maxdepth 1 -name '*.beam' | read; then
  echo "Error: No compiled Erlang beam files found in $EBIN_DIR"
  exit 1
fi

AGENT_A_LOG="/tmp/deadlock_agent_a.log"
AGENT_B_LOG="/tmp/deadlock_agent_b.log"
SCHED_A_LOG="/tmp/deadlock_scheduler_a.log"
SCHED_B_LOG="/tmp/deadlock_scheduler_b.log"

cleanup() {
  echo "Cleaning up..."
  kill 2>/dev/null "$AGENT_A_PID" || true
  kill 2>/dev/null "$AGENT_B_PID" || true
  kill 2>/dev/null "$SCHED_A_PID" || true
  kill 2>/dev/null "$SCHED_B_PID" || true
  wait 2>/dev/null "$AGENT_A_PID" || true
  wait 2>/dev/null "$AGENT_B_PID" || true
  wait 2>/dev/null "$SCHED_A_PID" || true
  wait 2>/dev/null "$SCHED_B_PID" || true
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

printf "Building project before running test...\n"
make >/dev/null 2>&1

printf "Starting Agent A on port 8080 (2 CPU, 8GB, 0 GPU)...\n"
"$AGENT_SCRIPT" port:8080 bind:127.0.0.1 cpu:2 mem:8192 gpu:0 > "$AGENT_A_LOG" 2>&1 &
AGENT_A_PID=$!

printf "Starting Agent B on port 8081 (2 CPU, 4GB, 1 GPU)...\n"
"$AGENT_SCRIPT" port:8081 bind:127.0.0.1 cpu:2 mem:4096 gpu:1 > "$AGENT_B_LOG" 2>&1 &
AGENT_B_PID=$!

printf "Waiting for agents to initialize and discover each other...\n"
sleep 8

printf "Starting Scheduler A for Agent A...\n"
erl -noshell -pa "$EBIN_DIR" -eval '
S = spawn(fun() -> scheduler:init([8080],0, "127.0.0.1") end),
 timer:sleep(12000),
 S ! {request_job, 1, [{"cpu",2},{"gpu",1}], self()},
 io:format("[sched_a] Sent JOB_REQUEST 1 ~p~n", [[{"cpu",2},{"gpu",1}]]),
 timer:sleep(45000),
 halt().' > "$SCHED_A_LOG" 2>&1 &
SCHED_A_PID=$!

printf "Starting Scheduler B for Agent B...\n"
erl -noshell -pa "$EBIN_DIR" -eval '
S = spawn(fun() -> scheduler:init([8081],0, "127.0.0.1") end),
 timer:sleep(12050),
 S ! {request_job, 2, [{"cpu",2},{"gpu",1}], self()},
 io:format("[sched_b] Sent JOB_REQUEST 2 ~p~n", [[{"cpu",2},{"gpu",1}]]),
 timer:sleep(45000),
 halt().' > "$SCHED_B_LOG" 2>&1 &
SCHED_B_PID=$!

printf "Waiting for the deadlock scenario to evolve...\n"
sleep 40

printf "\n===== AGENT A LOG =====\n"
tail -n 50 "$AGENT_A_LOG" || true
printf "\n===== AGENT B LOG =====\n"
tail -n 50 "$AGENT_B_LOG" || true
printf "\n===== SCHEDULER A LOG =====\n"
tail -n 50 "$SCHED_A_LOG" || true
printf "\n===== SCHEDULER B LOG =====\n"
tail -n 50 "$SCHED_B_LOG" || true

printf "\nTest finished. Look for GRANTED/PENDING/Denied behavior in the logs.\n"
exit 0
