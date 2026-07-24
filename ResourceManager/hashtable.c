#include "hashtable.h"
#include <assert.h>

/**
 * Creates a new empty hash table, with the given capacity.
 */
HashTable tablahash_create(CopyFunc copy,
                          CompareFunc comp, DestroyFunc destr,
                          HashFunc hash) {

  HashTable table = malloc(sizeof(struct _HashTable));
  assert(table != NULL);
  table->elems = malloc(sizeof(HashBucket) * HASH_SIZE);
  assert(table->elems != NULL);
  table->numElems = 0;
  table->capacity = HASH_SIZE;
  table->copy = copy;
  table->comp = comp;
  table->destr = destr;
  table->hash = hash;

  pthread_mutex_init(&table->table_mutex, NULL);

  for (unsigned idx = 0; idx < HASH_SIZE; ++idx) {
    table->elems[idx].list = NULL;
  }

  return table;
}




int tablahash_nelems(HashTable table) {
  pthread_mutex_lock(&table->table_mutex);
  int i = table->numElems;
  pthread_mutex_unlock(&table->table_mutex);
  return i;
}



int tablahash_capacity(HashTable table) { return table->capacity; }



void tablahash_destroy(HashTable table) {
  for (unsigned idx = 0; idx < table->capacity; ++idx) {
    ListNode *current = table->elems[idx].list;
    while (current != NULL) {
      ListNode *next_node = current->next;
      table->destr(current->data);
      free(current);
      current = next_node;
    }
  }

  pthread_mutex_destroy(&table->table_mutex);
  free(table->elems);
  free(table);
}

static ListNode *find_in_list(HashTable table, ListNode *list, void *data) {
  ListNode *current = list;
  while (current != NULL) {
    if (table->comp(current->data, data) == 0)
      return current;
    current = current->next;
  }
  return NULL;
}

void tablahash_insert(HashTable table, void *data) {

  pthread_mutex_lock(&table->table_mutex);
  unsigned idx = table->hash(data) % table->capacity;
  ListNode* existing = find_in_list(table, table->elems[idx].list, data);

  if (existing != NULL) {
    table->destr(existing->data);
    existing->data = table->copy(data);
  } else {
    ListNode *new_node = malloc(sizeof(ListNode));
    assert(new_node != NULL);
    new_node->data = table->copy(data);
    new_node->next = table->elems[idx].list;
    table->elems[idx].list = new_node;
    table->numElems++;
  }
  pthread_mutex_unlock(&table->table_mutex);
}

void *tablahash_find(HashTable table, void *data) {

  pthread_mutex_lock(&table->table_mutex);
  unsigned idx = table->hash(data) % table->capacity;
  ListNode* found = find_in_list(table, table->elems[idx].list, data);

  void* result = (found != NULL) ? found->data : NULL;

  pthread_mutex_unlock(&table->table_mutex);
  return result;
}

void tablahash_remove(HashTable table, void *data) {


  unsigned idx = table->hash(data) % table->capacity;
  ListNode* current = table->elems[idx].list;
  ListNode* prev = NULL;

  while (current != NULL && table->comp(current->data, data) != 0) {
    prev = current;
    current = current->next;
  }

  /* Not found: nothing to do. Do NOT unlock here — this function assumes the
   * caller (tablahash_remove_lock) holds the mutex and will release it. */
  if (current == NULL) {
    return;
  }

  if (prev == NULL)
    table->elems[idx].list = current->next;
  else
    prev->next = current->next;

  table->destr(current->data);
  free(current);
  table->numElems--;


}



void tablahash_remove_lock(HashTable table, void *data) {

  pthread_mutex_lock(&table->table_mutex);
    tablahash_remove(table, data);
  pthread_mutex_unlock(&table->table_mutex);
}
