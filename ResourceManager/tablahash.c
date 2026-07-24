#include "tablahash.h"
#include <assert.h>

/**
 * Crea una nueva tabla hash vacia, con la capacidad dada.
 */
TablaHash tablahash_crear(FuncionCopiadora copia,
                          FuncionComparadora comp, FuncionDestructora destr,
                          FuncionHash hash) {

  TablaHash tabla = malloc(sizeof(struct _TablaHash));
  assert(tabla != NULL);
  tabla->elems = malloc(sizeof(CasillaHash) * HASH_SIZE);
  assert(tabla->elems != NULL);
  tabla->numElems = 0;
  tabla->capacidad = HASH_SIZE;
  tabla->copia = copia;
  tabla->comp = comp;
  tabla->destr = destr;
  tabla->hash = hash;

  pthread_mutex_init(&tabla->table_mutex, NULL);

  for (unsigned idx = 0; idx < HASH_SIZE; ++idx) {
    tabla->elems[idx].lista = NULL;
  }

  return tabla;
}




int tablahash_nelems(TablaHash tabla) {
  pthread_mutex_lock(&tabla->table_mutex);
  int i = tabla->numElems;
  pthread_mutex_unlock(&tabla->table_mutex);
  return i;
}



int tablahash_capacidad(TablaHash tabla) { return tabla->capacidad; }



void tablahash_destruir(TablaHash tabla) {
  for (unsigned idx = 0; idx < tabla->capacidad; ++idx) {
    NodoLista *actual = tabla->elems[idx].lista;
    while (actual != NULL) {
      NodoLista *siguiente = actual->next;
      tabla->destr(actual->dato);
      free(actual);
      actual = siguiente;
    }
  }

  pthread_mutex_destroy(&tabla->table_mutex);
  free(tabla->elems);
  free(tabla);
}

static NodoLista *buscar_en_lista(TablaHash tabla, NodoLista *lista, void *dato) {
  NodoLista *actual = lista;
  while (actual != NULL) {
    if (tabla->comp(actual->dato, dato) == 0)
      return actual;
    actual = actual->next;
  }
  return NULL;
}

void tablahash_insertar(TablaHash tabla, void *dato) {

  pthread_mutex_lock(&tabla->table_mutex);
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* existente = buscar_en_lista(tabla, tabla->elems[idx].lista, dato);

  if (existente != NULL) {
    tabla->destr(existente->dato);
    existente->dato = tabla->copia(dato);
  } else {
    NodoLista *nuevo = malloc(sizeof(NodoLista));
    assert(nuevo != NULL);
    nuevo->dato = tabla->copia(dato);
    nuevo->next = tabla->elems[idx].lista;
    tabla->elems[idx].lista = nuevo;
    tabla->numElems++;
  }
  pthread_mutex_unlock(&tabla->table_mutex);
}

void *tablahash_buscar(TablaHash tabla, void *dato) {

  pthread_mutex_lock(&tabla->table_mutex);
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* encontrado = buscar_en_lista(tabla, tabla->elems[idx].lista, dato);

  void* data = (encontrado != NULL) ? encontrado->dato : NULL;

  pthread_mutex_unlock(&tabla->table_mutex);
  return data;
}

void tablahash_eliminar(TablaHash tabla, void *dato) {

  
  unsigned idx = tabla->hash(dato) % tabla->capacidad;
  NodoLista* actual = tabla->elems[idx].lista;
  NodoLista* prev = NULL;

  while (actual != NULL && tabla->comp(actual->dato, dato) != 0) {
    prev = actual;
    actual = actual->next;
  }

  /* Not found: nothing to do. Do NOT unlock here — this function assumes the
   * caller (tablahash_eliminar_lock) holds the mutex and will release it. */
  if (actual == NULL) {
    return;
  }

  if (prev == NULL)
    tabla->elems[idx].lista = actual->next;
  else
    prev->next = actual->next;

  tabla->destr(actual->dato);
  free(actual);
  tabla->numElems--;


}



void tablahash_eliminar_lock(TablaHash tabla, void *dato) {

  pthread_mutex_lock(&tabla->table_mutex);
    tablahash_eliminar(tabla, dato);
  pthread_mutex_unlock(&tabla->table_mutex);
}