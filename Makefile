# ─── CONFIGURACIÓN DEL COMPILADOR C ───────────────────────────────
CC = gcc
CFLAGS = -Wall -Wextra -g -pthread -D_GNU_SOURCE -I. -IResourceManager
LDFLAGS = -pthread

# Nombre del ejecutable final de C
TARGET = servidor

# ─── ARCHIVOS FUENTE C (.c) ───────────────────────────────────────
SRCS = main.c \
       agenteC/agente.c \
       agenteC/comunicaciones.c \
       agenteC/utils.c \
       ResourceManager/job_table.c \
       ResourceManager/rm_Queue.c

# Convierte automáticamente la lista de .c en archivos objeto .o
OBJS = $(SRCS:.c=.o)

# ─── CONFIGURACIÓN ERLANG ─────────────────────────────────────────
ERLC = erlc
ERL_SRCS = Scheduler_Erlang/scheduler.erl Scheduler_Erlang/scheduler_utils.erl
BEAM_FILES = $(ERL_SRCS:.erl=.beam)

# ─── REGLAS DE COMPILACIÓN ────────────────────────────────────────

# Regla principal (se ejecuta al escribir 'make'). Ahora compila C y Erlang.
all: $(TARGET) $(BEAM_FILES)

# Enlaza los archivos objeto para crear el ejecutable C
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS)

# Regla para compilar cada archivo C a un .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Regla para compilar los archivos Erlang (.erl) a (.beam)
%.beam: %.erl
	$(ERLC) $<

# Limpieza de archivos temporales, el ejecutable y los archivos compilados de Erlang
clean:
	rm -f $(OBJS) $(TARGET) $(BEAM_FILES) erl_crash.dump

.PHONY: all clean