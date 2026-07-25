# Gestor de Recursos Distribuido

Trabajo Práctico final de **Sistemas Operativos I** (Licenciatura en Ciencias de la Computación).

Sistema distribuido de reserva de recursos finitos (**CPU**, **MEM**, **GPU**) sobre un clúster de
máquinas ("nodos"), que demuestra una estrategia concreta de **evasión y recuperación de deadlock**.
Está compuesto por dos mitades que cooperan:

- **servidor** (agente en C): un proceso por máquina. Es el **mecanismo**: administra el pool local
  de recursos, descubre a sus pares por UDP, atiende pedidos de reserva de otros nodos por TCP y
  ejecuta la máquina de estados de reserva. Reactor basado en `epoll` con un pool fijo de hilos.
- **scheduler** (planificador en Erlang/OTP): la **política**. Inventa *jobs*, decide qué nodo
  aporta cada recurso y maneja al agente C local por un socket TCP de loopback.

---

## Tabla de contenidos

1. [Estructura del proyecto](#estructura-del-proyecto)
2. [Requisitos](#requisitos)
3. [Compilación](#compilación)
4. [Ejecución](#ejecución)
   - [Nodo agente (C)](#nodo-agente-c)
   - [Scheduler (Erlang)](#scheduler-erlang)
5. [Test de deadlock](#test-de-deadlock)
6. [Limpieza](#limpieza)
7. [Targets de Make disponibles](#targets-de-make-disponibles)
8. [Documentación técnica](#documentación-técnica)

---

## Estructura del proyecto

```text
TP_SistemasOperativosI
├── main.c                      # Entry point: arma tablas/colas y llama a setup_epoll(port)
├── Makefile                    # Build de C (build/servidor) + Erlang (.beam)
├── README.md
├── test_deadlock.sh            # Demo de 2 nodos: forma y resuelve el deadlock por timeout
│
├── agent/                      # ── AGENTE C ──
│   ├── agent.c / .h            # Setup de sockets + epoll, pool de workers, event_loop
│   ├── communications.c / .h   # Protocolo: erlang_to_C, client_to_myserver, ask_for_next_resource
│   ├── loopfunc.c / .h         # Los 8 handlers de eventos (accept, udp bcast, send, recv, …)
│   ├── read_instructions.c / .h# Framing por fd (reensamblado por '\n', tokenizado)
│   ├── utils.c / .h            # Contabilidad del pool, drenado de colas, serialización de nodos
│   ├── timer.c / .h            # timerfd + barridos de timeout de jobs/nodos
│   └── globals.h               # Estado compartido (tablas, colas, epollfd, erlangfd)
│
├── ResourceManager/            # ── ESTRUCTURAS DE DATOS ──
│   ├── hashtable.c / .h        # Tabla hash encadenada, genérica y thread-safe
│   ├── hash.c / .h             # Structs de dominio + callbacks y factories por tabla
│   └── resource_queue.c / .h   # Colas FIFO de pedidos por recurso
│
├── Scheduler_Erlang/           # ── SCHEDULER ERLANG ──
│   ├── scheduler.erl           # Coordinador + procesos job/job_generator
│   ├── scheduler_utils.erl     # Helpers de mensajes/log, parseo de nodos, elección de recursos
│   ├── test.erl                # Módulo de pruebas
│   └── Explicacion.md          # Diseño: estrategia de deadlock y tablas de protocolo
│
├── build/                      # (generado por make)
│   ├── servidor                #   ejecutable del agente C
│   ├── obj/                    #   todos los objetos .o juntos
│   └── erlang_beams/           #   módulos Erlang compilados (.beam)
├── ENUNCIADO.pdf               # Consigna
├── Informe.docx                # Informe
└── Diagrama.png                # Diagrama de arquitectura
```

---

## Requisitos

### Agente C

- **GCC** compatible con C11.
- **GNU Make**.
- **Linux**: usa `epoll` y `timerfd` (no funciona en macOS/Windows nativo; en Windows usar **WSL**).

### Scheduler Erlang

- **Erlang/OTP** instalado, con `erlc` disponible en el `PATH`.

### Verificación del entorno

```bash
gcc --version
make --version
erl -version
```

---

## Compilación

Desde el directorio raíz del proyecto:

```bash
make
```

Este comando:

1. Compila el agente C en `build/servidor` (todos los objetos `.o` quedan juntos en `build/obj/`).
2. Compila los módulos Erlang de `Scheduler_Erlang/` a archivos `.beam` en `build/erlang_beams/`.

Se compila sin warnings bajo `-Wall -Wextra`.

---

## Ejecución

Cada máquina corre **un** `servidor` y, opcionalmente, **un** `scheduler` Erlang que lo maneja.
Los nodos se descubren entre sí por *broadcast* UDP (puerto `12529`) y negocian recursos por TCP.

### Nodo agente (C)

```bash
./build/servidor [PUERTO]
```

| Parámetro | Descripción | Valor por defecto |
|---|---|---|
| `PUERTO` | Puerto TCP de escucha (peers + interfaz Erlang) | `4200` |

Ejemplos:

```bash
./build/servidor            # escucha en el puerto 4200
./build/servidor 4201       # escucha en el puerto 4201
```

El inventario local de recursos (`cpu:4 mem:8192 gpu:1`) está definido con `#define` en
[main.c](main.c); para modelar otra máquina, editá `LOCAL_CPU` / `LOCAL_MEM` / `LOCAL_GPU`.

### Scheduler (Erlang)

Con el agente ya corriendo, desde la raíz del proyecto (los `.beam` están en `build/erlang_beams/`):

```bash
erl -pa build/erlang_beams -noshell -s scheduler start
```

El scheduler se conecta al agente local en `127.0.0.1:4200` (constante `?PORT` en
[scheduler.erl](Scheduler_Erlang/scheduler.erl)) y comienza a generar *jobs*.
`make` ya deja los `.beam` compilados; si querés compilarlos a mano:

```bash
erlc -o build/erlang_beams Scheduler_Erlang/scheduler.erl Scheduler_Erlang/scheduler_utils.erl
```

---

## Test de deadlock

`test_deadlock.sh` levanta **dos nodos locales** y reproduce el deadlock distribuido de la
**sección 6** del enunciado: un job pide sus recursos en el orden **adversarial** (GPU antes que
CPU), se forma la espera circular, y tras `JOB_TIMEOUT_SEC` uno de los jobs hace *timeout*, libera
su reserva parcial y el otro completa. Romper el No-Preemption por timeout **resuelve** el deadlock.

```bash
./test_deadlock.sh
```

El script compila, arranca los nodos, inyecta el escenario, espera la resolución por timeout
(~30–40 s) y verifica que ningún nodo quede colgado (chequea estado-D y zombies).

> Los montos que piden los jobs (`JOB_CPU` / `JOB_GPU`, arriba en el script) **deben coincidir** con
> el inventario `LOCAL_CPU` / `LOCAL_GPU` de `main.c` para que se forme la contención. Si cambiás el
> inventario, ajustá esos valores o pasalos por línea de comandos:
>
> ```bash
> JOB_CPU=8 JOB_GPU=2 ./test_deadlock.sh
> ```

---

## Limpieza

```bash
make clean
```

Elimina el árbol `build/`, los `.beam` de Erlang y `erl_crash.dump`.

---

## Targets de Make disponibles

| Comando | Descripción |
|---|---|
| `make` / `make all` | Compila el agente C (`build/servidor`) y los `.beam` de Erlang |
| `make clean` | Elimina los archivos generados |

---

## Documentación técnica

- [Scheduler_Erlang/Explicacion.md](Scheduler_Erlang/Explicacion.md) — diseño del scheduler,
  estrategia de deadlock y tablas del protocolo.
- `ENUNCIADO.pdf` — consigna del trabajo práctico.
- `Informe.docx` — informe del proyecto.
- `Diagrama.png` — diagrama de arquitectura.
