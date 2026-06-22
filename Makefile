# ─── CONFIGURACIÓN DEL COMPILADOR ─────────────────────────────────
CC = gcc
# -I. y -IResourceManager le dicen al compilador dónde buscar los archivos .h
CFLAGS = -Wall -Wextra -g -pthread -D_GNU_SOURCE -I. -IResourceManager
LDFLAGS = -pthread

# Nombre del ejecutable final
TARGET = servidor

# ─── ARCHIVOS FUENTE (.c) ─────────────────────────────────────────
# Dejamos fuera explícitamente los archivos de test (jt_unit_test.c y rm_Queue_test.c)
SRCS = main.c \
       agenteC/agente.c \
       agenteC/comunicaciones.c \
       agenteC/utils.c \
       ResourceManager/job_table.c \
       ResourceManager/rm_Queue.c

# Convierte automáticamente la lista de .c en archivos objeto .o
OBJS = $(SRCS:.c=.o)

# ─── REGLAS DE COMPILACIÓN ────────────────────────────────────────

# Regla principal (se ejecuta al escribir 'make')
all: $(TARGET)

# Enlaza los archivos objeto para crear el ejecutable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS)

# Regla para compilar cada archivo .c a un .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Limpieza de archivos temporales y el ejecutable
clean:
	rm -f $(OBJS) $(TARGET)

# Evita conflictos si existen archivos que se llamen 'all' o 'clean'
.PHONY: all clean