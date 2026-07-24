#ifndef __TABLAHASH_H__
#define __TABLAHASH_H__

#include <stdlib.h>
#include <pthread.h>

#define HASH_SIZE 256

/* 1) Typedefs de punteros a función, definidos ANTES de usarlos en el struct */
typedef void *(*FuncionCopiadora)(void *dato);
/** Retorna una copia fisica del dato */
typedef int (*FuncionComparadora)(void *dato1, void *dato2);
/** Retorna un entero negativo si dato1 < dato2, 0 si son iguales y un entero
 * positivo si dato1 > dato2  */
typedef void (*FuncionDestructora)(void *dato);
/** Libera la memoria alocada para el dato */
typedef unsigned (*FuncionHash)(void *dato);
/** Retorna un entero sin signo para el dato */

/* 2) Casillas de la tabla hash (encadenamiento para colisiones) */
typedef struct _NodoLista {
    void *dato;
    struct _NodoLista *next;
} NodoLista;

typedef struct {
  NodoLista* lista;
} CasillaHash;

/* 3) El struct principal, ahora expuesto acá en el .h */
struct _TablaHash {
  CasillaHash *elems;
  unsigned numElems;
  unsigned capacidad;
  FuncionCopiadora copia;
  FuncionComparadora comp;
  FuncionDestructora destr;
  FuncionHash hash;
  pthread_mutex_t table_mutex;
};

/* 4) TablaHash sigue siendo un puntero al struct (coincide con el uso "->" en el .c) */
typedef struct _TablaHash *TablaHash;

/**
 * Crea una nueva tabla hash vacia, con la capacidad dada.
 */
TablaHash tablahash_create(FuncionCopiadora copia,
                          FuncionComparadora comp, FuncionDestructora destr,
                          FuncionHash hash);

/**
 * Retorna el numero de elementos de la tabla.
 */
int tablahash_nelems(TablaHash tabla);

/**
 * Retorna la capacidad de la tabla.
 */
int tablahash_capacity(TablaHash tabla);

/**
 * Destruye la tabla.
 */
void tablahash_destroy(TablaHash tabla);

/**
 * Inserta un dato en la tabla, o lo reemplaza si ya se encontraba.
 */
void tablahash_insert(TablaHash tabla, void *dato);

/**
 * Retorna el dato de la tabla que coincida con el dato dado, o NULL si el dato
 * buscado no se encuentra en la tabla.
 */
void *tablahash_find(TablaHash tabla, void *dato);

/**
 * Elimina el dato de la tabla que coincida con el dato dado.
 */
void tablahash_remove(TablaHash tabla, void *dato);

void tablahash_remove_lock(TablaHash tabla, void *dato);

#endif /* __TABLAHASH_H__ */