# Usamos Ubuntu como sistema operativo base
FROM ubuntu:latest

# Evitamos que la instalación nos haga preguntas interactivas
ENV DEBIAN_FRONTEND=noninteractive

# Instalamos los compiladores y herramientas de red
RUN apt-get update && apt-get install -y \
    gcc \
    make \
    erlang \
    net-tools \
    iputils-ping

# Le decimos que nuestra carpeta de trabajo va a ser /app
WORKDIR /app

# Copiamos todo el código de tu compu a la carpeta /app del contenedor
COPY . /app

# Compilamos tu proyecto ni bien se crea la imagen
RUN make