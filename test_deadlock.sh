#!/usr/bin/env bash
# =============================================================================
# test_deadlock.sh
# Trabajo Práctico – Manejador de recursos distribuidos para HPC
# R-322 Sistemas Operativos I
#
# Levanta dos instancias locales del agente C en puertos distintos y simula
# el escenario de deadlock descripto en §6:
#   Nodo A (puerto 4200): cpu:4 mem:8192 gpu:0
#   Nodo B (puerto 4201): cpu:2 mem:4096 gpu:1
#
#   Job1 (desde A): necesita 2 CPUs de A  +  1 GPU de B
#   Job2 (desde B): necesita 1 GPU de B   +  2 CPUs de A
#
# Si la implementación previene deadlocks, al menos uno de los dos jobs
# debe completarse (JOB_GRANTED) antes de que venza el timeout.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
AGENT_BIN="./servidor"          # Ruta al binario compilado del agente C
PORT_A=4200
PORT_B=4201
TIMEOUT_SEC=30                # Tiempo máximo esperado para resolución
LOG_DIR="/tmp/deadlock_test"
PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Colores para la salida
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # Sin color

log()  { echo -e "${YELLOW}[TEST]${NC} $*"; }
ok()   { echo -e "${GREEN}[PASS]${NC} $*"; ((PASS++)); }
fail() { echo -e "${RED}[FAIL]${NC} $*"; ((FAIL++)); }

# ---------------------------------------------------------------------------
# Limpieza al salir
# ---------------------------------------------------------------------------
cleanup() {
    log "Limpiando procesos e IPC..."
    kill "$PID_A" "$PID_B" 2>/dev/null || true
    wait "$PID_A" "$PID_B" 2>/dev/null || true
    rm -f "$LOG_DIR"/node_{a,b}.log \
          "$LOG_DIR"/erlang_{a,b}.fifo \
          "$LOG_DIR"/erlang_{a,b}_resp.log
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Verificar que el binario existe
# ---------------------------------------------------------------------------
if [[ ! -x "$AGENT_BIN" ]]; then
    echo -e "${RED}[ERROR]${NC} No se encontró el binario '$AGENT_BIN'."
    echo "        Compilá primero con:  make"
    exit 1
fi

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Función auxiliar: enviar un comando al agente vía TCP (simula Erlang)
# Usa netcat en modo client; si el agente implementa el puerto local de Erlang
# en 127.0.0.1:PORT, este envío lo alcanza.
# ---------------------------------------------------------------------------
send_erlang_cmd() {
    local port="$1"
    local cmd="$2"
    local resp_file="$3"
    # nc sale solo cuando el servidor cierra la conexión o tras el timeout de lectura
    echo -e "$cmd" | nc -q 2 127.0.0.1 "$port" >> "$resp_file" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Función auxiliar: esperar una cadena en un archivo de log
# ---------------------------------------------------------------------------
wait_for_string() {
    local file="$1"
    local pattern="$2"
    local secs="$3"
    local elapsed=0
    while (( elapsed < secs )); do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 1
        (( elapsed++ ))
    done
    return 1
}

# ===========================================================================
# INICIO DEL TEST
# ===========================================================================
log "=== Test de Deadlock – HPC Resource Manager ========================="
log "Escenario: Job1 (A→B) y Job2 (B→A) pedidos casi simultáneamente."

# ---------------------------------------------------------------------------
# 1. Levantar Nodo A
# ---------------------------------------------------------------------------
log "Levantando Nodo A en puerto $PORT_A..."
# Pasamos las variables de recursos y puerto como variables de entorno;
# el agente debe leerlas o usar los valores por defecto del código.
AGENT_PORT="$PORT_A" CPU=4 MEM=8192 GPU=0 \
    "$AGENT_BIN" > "$LOG_DIR/node_a.log" 2>&1 &
PID_A=$!
log "  PID Nodo A: $PID_A"

# ---------------------------------------------------------------------------
# 2. Levantar Nodo B
# ---------------------------------------------------------------------------
log "Levantando Nodo B en puerto $PORT_B..."
AGENT_PORT="$PORT_B" CPU=2 MEM=4096 GPU=1 \
    "$AGENT_BIN" > "$LOG_DIR/node_b.log" 2>&1 &
PID_B=$!
log "  PID Nodo B: $PID_B"

# Dar tiempo a que levanten y se descubran vía UDP broadcast
log "Esperando 3 s para que los nodos se descubran entre sí..."
sleep 3

# Verificar que ambos procesos siguen vivos
if ! kill -0 "$PID_A" 2>/dev/null; then
    fail "Nodo A terminó inesperadamente. Revisar $LOG_DIR/node_a.log"
    exit 1
fi
if ! kill -0 "$PID_B" 2>/dev/null; then
    fail "Nodo B terminó inesperadamente. Revisar $LOG_DIR/node_b.log"
    exit 1
fi
ok "Ambos nodos están corriendo."

# ---------------------------------------------------------------------------
# 3. Preparar archivos de respuesta
# ---------------------------------------------------------------------------
RESP_A="$LOG_DIR/erlang_a_resp.log"
RESP_B="$LOG_DIR/erlang_b_resp.log"
> "$RESP_A"
> "$RESP_B"

# ---------------------------------------------------------------------------
# 4. Enviar JOB_REQUEST simultáneamente desde ambos nodos
#    Job1 (id=1001): A pide 2 CPUs de A (local) + 1 GPU de B
#    Job2 (id=2001): B pide 1 GPU de B (local) + 2 CPUs de A
#
#    Nota: los recursos locales se indican con la IP del propio nodo;
#    aquí usamos 127.0.0.1 porque ambos corren en localhost.
#    En un entorno real serían IPs distintas.
# ---------------------------------------------------------------------------
log "Enviando JOB_REQUEST 1001 desde Nodo A  (cpu@A + gpu@B)..."
send_erlang_cmd "$PORT_A" \
    "JOB_REQUEST 1001 @127.0.0.1:cpu:2 @127.0.0.1:gpu:1" \
    "$RESP_A" &

log "Enviando JOB_REQUEST 2001 desde Nodo B  (gpu@B + cpu@A)..."
send_erlang_cmd "$PORT_B" \
    "JOB_REQUEST 2001 @127.0.0.1:gpu:1 @127.0.0.1:cpu:2" \
    "$RESP_B" &

log "Esperando hasta ${TIMEOUT_SEC}s la resolución..."

# ---------------------------------------------------------------------------
# 5. Esperar resultado: al menos uno debe recibir JOB_GRANTED
# ---------------------------------------------------------------------------
GRANTED_A=false
GRANTED_B=false
DENIED_A=false
DENIED_B=false

deadline=$(( SECONDS + TIMEOUT_SEC ))

while (( SECONDS < deadline )); do
    grep -q "JOB_GRANTED 1001" "$RESP_A" 2>/dev/null && GRANTED_A=true
    grep -q "JOB_GRANTED 2001" "$RESP_B" 2>/dev/null && GRANTED_B=true
    grep -q "JOB_DENIED  1001\|JOB_TIMEOUT 1001" "$RESP_A" 2>/dev/null && DENIED_A=true
    grep -q "JOB_DENIED  2001\|JOB_TIMEOUT 2001" "$RESP_B" 2>/dev/null && DENIED_B=true

    # Si al menos uno fue GRANTED y el otro resuelto (GRANTED o DENIED), terminamos
    if { $GRANTED_A || $GRANTED_B; } && { $GRANTED_A || $DENIED_A; } && { $GRANTED_B || $DENIED_B; }; then
        break
    fi

    # Si ambos están en deadlock total (sin respuesta) esperamos más
    sleep 1
done

# ---------------------------------------------------------------------------
# 6. Evaluar resultados
# ---------------------------------------------------------------------------
log "--- Resultados ---"

if $GRANTED_A; then
    ok "Job 1001 (Nodo A) → JOB_GRANTED"
elif $DENIED_A; then
    ok "Job 1001 (Nodo A) → JOB_DENIED/TIMEOUT (deadlock resuelto por rechazo)"
else
    fail "Job 1001 (Nodo A) → sin respuesta después de ${TIMEOUT_SEC}s  ← posible DEADLOCK"
fi

if $GRANTED_B; then
    ok "Job 2001 (Nodo B) → JOB_GRANTED"
elif $DENIED_B; then
    ok "Job 2001 (Nodo B) → JOB_DENIED/TIMEOUT (deadlock resuelto por rechazo)"
else
    fail "Job 2001 (Nodo B) → sin respuesta después de ${TIMEOUT_SEC}s  ← posible DEADLOCK"
fi

# Al menos un job debe haber sido GRANTED (sistema útil, no solo rechazos)
if $GRANTED_A || $GRANTED_B; then
    ok "Al menos un job completado exitosamente – el sistema progresa."
else
    fail "Ningún job fue GRANTED – el sistema no logró asignar recursos."
fi

# Verificar que ningún nodo crasheó durante el test
if kill -0 "$PID_A" 2>/dev/null; then
    ok "Nodo A sigue activo al final del test."
else
    fail "Nodo A terminó durante el test (crash)."
fi

if kill -0 "$PID_B" 2>/dev/null; then
    ok "Nodo B sigue activo al final del test."
else
    fail "Nodo B terminó durante el test (crash)."
fi

# ---------------------------------------------------------------------------
# 7. Resumen final
# ---------------------------------------------------------------------------
echo ""
log "=== Resumen ========================================================="
echo -e "  ${GREEN}PASS: $PASS${NC}   ${RED}FAIL: $FAIL${NC}"

if (( FAIL == 0 )); then
    echo -e "${GREEN}[OK] La implementación supera el escenario de deadlock.${NC}"
    exit 0
else
    echo -e "${RED}[KO] Hay fallas – revisar logs en $LOG_DIR/${NC}"
    echo "     node_a.log  →  $LOG_DIR/node_a.log"
    echo "     node_b.log  →  $LOG_DIR/node_b.log"
    exit 1
fi
