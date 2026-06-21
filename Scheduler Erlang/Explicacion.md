# EXPLICACION SCHEDULER ERLANG

> Note : This explanation was made on a team-purpose, thus the language is spanish :p.

## 1) Objetivo y arquitectura principal

La idea de esta parte del trabajo se divide en dos partes importantes:

1. En base a los recursos de los nodos de la red distribuida, crear trabajos que usen esos recursos, y
2. Manejar la comunicacion entre estos trabajos y nuestro agente de C, a traves de un coordinador general.

Notar que toda la base de estas dos implementaciones se encuentra en **scheduler.erl**, con sus respectivas
funciones auxiliares en **scheduler_utils.erl**.

Empezaremos explicando el segundo inciso, ya que es la parte por donde se inicia este trabajo.

### 1.1) Coordinador()

Como vemos en nuestra funcion start(), iniciamos la funcion coordinador() conectandonos al host local
(127.0.0.1) y el puerto establecido previamente con nuestro agente de C (definido en la macro ?PORT).

Aclarar que gracias a la opcion {active, true} a la hora de conectarnos, podemos manejar los mensajes
recibidos a traves de TCP como hariamos con cualquier mensaje entre agentes de Erlang, lo que nos permite
no hacer llamadas a gen_tcp:recv(), las cuales son bloqueantes y ralentizarian todo el codigo.

Una vez conectados, lanzamos nuestro job_generator() (se explica luego) y de ahi, llamamos a coordinator_loop().
Este loop sera la base de nuestra comunicacion entre el agente de C y los trabajos creados por job_generator.
Pasamos a explicar que espera y que manda.

### 1.2) Coordinator_loop()

Recibe tres argumentos:

- **Socket**: para comunicarse con el agente de C
- **Jobs_Map**: donde se almacenan los jobs actuales
- **GenPid**: Pid del agente que pidio GET_NODES

Nota: Los dos ultimos items se explicaran luego.

Y lo unico que hacemos es recibir mensajes y actuar al respecto.
Podemos dividir esto en 4 bloques:

#### 1.2.1) Mensajes de Jobs a Agente C

- {register_and_request, JobId, PidJob, Message}: Recibimos que un job ha sido creado y solicita los recursos del mensaje, el cual ya esta con el formato adecuado (ver parte de formatos). Se lo enviamos al agente de C a traves del Socket, agregamos el job al Jobs_Map, con su JobId como key y su PidJob como value.

- {finished, JobId}: Un trabajo acaba de terminar su ejecucion, le avisamos a C que ya terminamos de usar esos recursos y lo eliminamos del map.

#### 1.2.2) Mensajes del Agente acerca del estado de un job

En cualquier caso de que sea JOB_GRANTED/DENIED/TIMEOUT, el trabajo es el mismo: lo escribimos en el log y le mandamos el msg al job a traves de msg_to_job().

#### 1.2.3) Mensajes de GET_NODES

- {get_nodes, Pid}: job_generator() quiere ver los recursos y sus nodos correspondientes, se lo pide al agente de C. Guardamos el pid del generador.
- {tcp, Socket, "NODES " ++ Rest}: Llega la info de los nodos con sus recursos, se la pasamos al generator.

#### 1.2.4) Mensajes de error/cierre del Agente C

En cualquiera de los dos casos, avisamos a cada job que cancele cualquier operacion que estaba haciendo. A diferencia del resto de los casos, esta rama **no vuelve a llamar a coordinator_loop**, por lo que el coordinador tambien termina aca.

### 1.3) Job_generator()

Comienza con un JobId, el cual es un entero que sera asignado al job como su Id, comenzando desde 1001, y haciendo que el proximo job tenga el siguiente al anterior.

Lo primero que hace es pedir GET_NODES, esto se hace ya que en distintos momentos de la ejecucion puede haber varios cambios grandes en la red de nodos: nuevos nodos, recursos liberados/ocupados, nodos eliminados. Por ende, siempre que creamos un job, vemos que recursos estan disponibles y en que nodos.

Luego de que hayan llegado, vamos a parsear esa cadena que recibimos (ver formato) a una gran lista, donde cada elemento de la lista es del tipo {IP, Port, Resources}, con

    Resources = [{cpu, CpuSize},{mem, MemSize},{gpu, GpuSize}]

Creamos luego los recursos a pedir con make_resources_for_job(), y le pasamos ese resultado, con su Id y el Id del Coord, a un nuevo agente con la funcion job().

Luego, esperamos un tiempo definido por la macro ?GENERATOR_LOOP_TIME, y volvemos a llamar a job_generator().

### 1.4) Job()

Job solo tiene un comportamiento sencillo.

Parsea los recursos a una cadena valida para enviarle al agente de C. Le envia {register_and_request, JobId, self(), Message} al coordinator, y espera:

- Si recibe granted, **simula** un tiempo entre 1 a 20 segundos, luego de eso, le avisa al coordinador que termino, y muere
- Si recibe denied, muere
- Si recibe timeout, espera un cierto tiempo y vuelve a ejecutarse
- En caso de recibir cancelled, tambien muere

## 2) Decisiones de estilo generales

Antes de entrar en el manejo de deadlocks y la traza de ejemplo, vale la pena justificar algunas decisiones que se repiten a lo largo de todo el modulo.

### 2.1) Estructuras de datos elegidas

**Jobs_Map como map (JobId => Pid):** se eligio un map en vez de una lista porque las operaciones que mas se repiten en coordinator_loop son agregar, buscar y eliminar un job por su JobId. Un map resuelve estas tres operaciones de forma directa (maps:put, maps:get, maps:remove), sin necesitar recorrer nada.

**Recursos como lista de tuplas, no map:** un job siempre pide exactamente tres recursos (cpu, mem, gpu), nunca mas ni menos. Por eso se opto por una lista de tuplas de tamano fijo ([{cpu,N},{mem,N},{gpu,N}]) en vez de un map. Esto permite hacer pattern matching directo ([Cpu, Mem, Gpu] = Recursos) en lugar de tener que buscar cada clave por separado.

**Nodos como lista de tuplas {IP, Port, Resources}, no map indexado por IP:** aca la operacion principal no es buscar el nodo X, sino elegir un nodo al azar. Para eso una lista es mas natural, ya que lists:nth(rand:uniform(N), Lista) resuelve la eleccion directamente; un map hubiera necesitado primero extraer las keys y recien ahi elegir una al azar, un paso de mas sin beneficio.

### 2.2) El coordinador como unico punto de contacto con C

Ningun job abre su propio socket ni habla directamente con el agente C. Todo pasa por mensajes Erlang hacia el coordinador, que es quien arma los strings del protocolo y los envia por TCP. Esto evita condiciones de carrera sobre un mismo socket compartido entre multiples procesos, y mantiene toda la logica del protocolo concentrada en un solo lugar en vez de repartida entre N jobs.

### 2.3) Recursos aleatorios, tiempo aleatorio

Como no esta definido un parametro de cuanto tiempo esperar entre cada trabajo ni cuanto tomar de cada recurso, se opto por la aleatoriedad (acotada por el tiempo maximo o los recursos maximos de un nodo).

## 3) Manejo de deadlocks

La idea es bastante basica y requiere que todos los mensajes enviados por otro agente de Erlang sigan la misma ejecucion, y se basa en romper el circular wait.

Con el orden de **Cpu > Mem > Gpu**, nunca se podra generar un deadlock ya que la espera circular no es posible.

En caso de que otro agente no implemente este orden, se requiere otra estrategia adicional (wait-die es posible).

## 4) Traza de ejemplo :)

Nota: Obviamos el tipeo y la transformacion de un tipo de dato a otro, solo vemos la base de la traza.

- Llamamos a start(), el cual llama a coordinator()
- Este se conecta al socket, lanza el job generator y entra a su loop
- job_generator() pide los nodos con sus datos, coordinator lo recibe y se lo pide al agente. Luego de un tiempo, recibe los nodos y se los pasa al job generator.
- El generator mira de un nodo X una cpu de cant i, de un nodo Y una mem de cant j y de un nodo Z una gpu de cant k. Crea un agente con la funcion job, le pasa esos recursos para que job se los pida al coordinador.
- El job inicia, se lo pide al coord y espera a que coord se comunique con el.
- Coordinator guarda el job en su map para comunicarse luego, le pasa un mensaje al agente de C diciendo que ese job esta pidiendo esos recursos a esos nodos, y espera el mensaje del agente de C.
- Una vez que recibe la respuesta respectiva a ese jobId, se la pasa al job. Supongamos que fue "granted", en ese caso, job() simula trabajo durmiendo una cantidad de tiempo random.
- Luego de su trabajo, le avisa al coordinator que termino, y se muere.
- El coordinator recibe el mensaje de que el job termino, lo borra del map y le avisa al agente de C que ya puede liberar esos recursos.

## 5) Formato de mensajes inusuales

### 5.1) GET_NODES (respuesta NODES)

NODES ip:puerto:cpu:cantidad:mem:cantidad:gpu:cantidad;ip:puerto:cpu:cantidad:mem:cantidad:gpu:cantidad;...

Cada nodo va separado por ";". Dentro de un mismo nodo, los campos van separados por ":", siempre en el mismo orden: ip, puerto, cpu, cantidad, mem, cantidad, gpu, cantidad.

Ejemplo con dos nodos:

    NODES 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096:gpu:1

parse_node/1 splitea primero por ";" para separar los nodos, y despues cada nodo por ":" para sacar sus 8 campos.

### 5.2) JOB_REQUEST

JOB_REQUEST <job_id> @ip:tipo:cantidad @ip:tipo:cantidad @ip:tipo:cantidad

A diferencia de NODES, aca cada recurso puede venir de un nodo distinto (notar el @ al principio de cada bloque). El job no esta atado a pedir todo de un solo nodo.

Ejemplo, donde el cpu y el gpu se piden al mismo nodo pero la mem a otro:

    JOB_REQUEST 1001 @192.168.1.10:cpu:2 @192.168.1.11:mem:4096 @192.168.1.10:gpu:1

El orden de los tres bloques (cpu, mem, gpu) siempre es el mismo: es la base de la estrategia anti-deadlock explicada en la seccion 3.