#ifndef __TABLAHASH_H__
#define __TABLAHASH_H__

#include <stdlib.h>
#include <pthread.h>

#define HASH_SIZE 256

/* 1) Function-pointer typedefs, defined BEFORE they are used in the struct */
typedef void *(*CopyFunc)(void *data);
/** Returns a physical copy of the data */
typedef int (*CompareFunc)(void *data1, void *data2);
/** Returns a negative int if data1 < data2, 0 if equal and a positive int if
 * data1 > data2 */
typedef void (*DestroyFunc)(void *data);
/** Frees the memory allocated for the data */
typedef unsigned (*HashFunc)(void *data);
/** Returns an unsigned int for the data */

/* 2) Hash table buckets (chaining for collisions) */
typedef struct _ListNode {
    void *data;
    struct _ListNode *next;
} ListNode;

typedef struct {
  ListNode* list;
} HashBucket;

/* 3) The main struct, now exposed here in the .h */
struct _HashTable {
  HashBucket *elems;
  unsigned numElems;
  unsigned capacity;
  CopyFunc copy;
  CompareFunc comp;
  DestroyFunc destr;
  HashFunc hash;
  pthread_mutex_t table_mutex;
};

/* 4) HashTable is still a pointer to the struct (matches the "->" use in the .c) */
typedef struct _HashTable *HashTable;

/**
 * Creates a new empty hash table, with the given capacity.
 */
HashTable tablahash_create(CopyFunc copy,
                          CompareFunc comp, DestroyFunc destr,
                          HashFunc hash);

/**
 * Returns the number of elements in the table.
 */
int tablahash_nelems(HashTable table);

/**
 * Returns the capacity of the table.
 */
int tablahash_capacity(HashTable table);

/**
 * Destroys the table.
 */
void tablahash_destroy(HashTable table);

/**
 * Inserts a datum into the table, or replaces it if already present.
 */
void tablahash_insert(HashTable table, void *data);

/**
 * Returns the table datum matching the given one, or NULL if the searched
 * datum is not found in the table.
 */
void *tablahash_find(HashTable table, void *data);

/**
 * Removes the table datum matching the given one.
 */
void tablahash_remove(HashTable table, void *data);

void tablahash_remove_lock(HashTable table, void *data);

#endif /* __TABLAHASH_H__ */
