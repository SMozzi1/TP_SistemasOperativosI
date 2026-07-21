#include "tablahash.h"
#include <assert.h>
#include <stdlib.h>
#include <pthread.h>

/**
 * Casillas en la que almacenaremos los datos de la tabla hash.
 */

typedef struct _NodoLista {
    void *dato;
    struct _NodoLista *next;
} NodoLista;

typedef struct {
  NodoLista* lista;
} CasillaHash;

/**
 * Estructura principal que representa la tabla hash.
 */
struct _TablaHash {
  CasillaHash *elems;
  unsigned numElems;
  unsigned capacidad;
  FuncionCopiadora copia;
  FuncionComparadora comp;
  FuncionDestructora destr;
  FuncionHash hash;
  pthread_mutex_t mutex;
};

/**
 * Crea una nueva tabla hash vacia, con la capacidad dada.
 */
TablaHash tablahash_crear(unsigned capacidad, FuncionCopiadora copia,
                          FuncionComparadora comp, FuncionDestructora destr,
                          FuncionHash hash) {

  // Pedimos memoria para la estructura principal y las casillas.
  TablaHash tabla = malloc(sizeof(struct _TablaHash));
  assert(tabla != NULL);
  tabla->elems = malloc(sizeof(CasillaHash) * capacidad);
  assert(tabla->elems != NULL);
  tabla->numElems = 0;
  tabla->capacidad = capacidad;
  tabla->copia = copia;
  tabla->comp = comp;
  tabla->destr = destr;
  tabla->hash = hash;

  pthread_mutex_init(tabla->mutex, NULL);

  // Inicializamos las casillas con datos nulos.
  for (unsigned idx = 0; idx < capacidad; ++idx) {
    tabla->elems[idx].dato = NULL;
  }

  return tabla;
}

/**
 * Retorna el numero de elementos de la tabla.
 */
int tablahash_nelems(TablaHash tabla) { 
  pthread_mutex_lock(tabla->mutex);
  int i = tabla->numElems; 
  pthread_mutex_unlock(tabla->mutex);
  return i;
}

/**
 * Retorna la capacidad de la tabla.
 */
int tablahash_capacidad(TablaHash tabla) { return tabla->capacidad; }

/**
 * Destruye la tabla.
 */
void tablahash_destruir(TablaHash tabla) {

  // Destruir cada uno de los datos.
  for (unsigned idx = 0; idx < tabla->capacidad; ++idx)
    if (tabla->elems[idx].dato != NULL)
      tabla->destr(tabla->elems[idx].dato);

  // Liberar el arreglo de casillas y la tabla.
  pthread_mutex_destroy(&tabla->mutex);
  free(tabla->elems);
  free(tabla);
  return;
}

// funciones aux para implementar la hash con listas enlazadas

//Tomi dice que no es thead_safe. para mi si ya que si me lo borran mientras itero no lo va a encontrar
// debido al return dentro del while, si usara una bandera si habria que tomar el lock
static NodoLista *buscar_en_lista(TablaHash tabla, NodoLista *lista, void *dato) {
  NodoLista *actual = lista;
  while (actual != NULL) {
    if (tabla->comp(actual->dato, dato) == 0)
      return actual;
    actual = actual->next;
  }
  return NULL;
}

/**
 * Inserta un dato en la tabla, o lo reemplaza si ya se encontraba.
 * IMPORTANTE: La implementacion no maneja colisiones.
 */
void tablahash_insertar(TablaHash tabla, void *dato) {

  pthread_mutex_lock(tabla->mutex);
  // Calculamos la posicion del dato dado, de acuerdo a la funcion hash.
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* inList = buscar_en_lista(tabla, tabla->elems[idx], dato);

  // Insertar el dato si la casilla estaba libre.
  if (inList != NULL) {
    tabla->destr(existente->dato);
    existente->dato = tabla->copia(dato);
  } else { // it wasnt in the list (inserto adelante)

    NodoLista *nuevo = malloc(sizeof(NodoLista));
    assert(nuevo != NULL);
    nuevo->dato = tabla->copia(dato);
    nuevo->next = tabla->elems[idx].lista;
    tabla->elems[idx].lista = nuevo;
    tabla->numElems++;

  }
  pthread_mutex_unlock(tabla->mutex);
}

/**
 * Retorna el dato de la tabla que coincida con el dato dado, o NULL si el dato
 * buscado no se encuentra en la tabla.
 */
void *tablahash_buscar(TablaHash tabla, void *dato) {

  pthread_mutex_lock(tabla->mutex);
  // Calculamos la posicion del dato dado, de acuerdo a la funcion hash.
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* encontrado = buscar_en_lista(tabla, tabla->elems, dato);

  void* data = (encontrado != NULL) ? encontrado->dato : NULL

  pthread_mutex_lock(tabla->mutex);
  return data;
}

/**
 * Elimina el dato de la tabla que coincida con el dato dado.
 */
void tablahash_eliminar(TablaHash tabla, void *dato) {

  pthread_mutex_lock(tabla->mutex);
  // Calculamos la posicion del dato dado, de acuerdo a la funcion hash.
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* actual = tabla->elems[idx].lista;
  NodoLista* prev = NULL;

  while(actual != NULL && tabla->comp(actual->dato, dato) != 0){
    prev = actual;
    actual = actual->next;
  }

  if (actual == NULL){
    pthread_mutex_unlock(tabla->mutex);
    return;
  }

  if (prev == NULL)
    tabla->elems[idx].lista = actual->next;
  else
    prev->next = actual->next;

  tabla->destr(actual->dato);
  free(actual);
  tabla->numElems--;

  pthread_mutex_unlock(&tabla->mutex);

}
