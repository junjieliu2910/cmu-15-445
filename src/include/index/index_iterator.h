/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t page_id, int index, BufferPoolManager* manager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  // add your own private member variables here
  page_id_t page_id_;
  int index_;
  BufferPoolManager *buffer_pool_manager_;
  B_PLUS_TREE_LEAF_PAGE_TYPE * leaf_;
};

} // namespace cmudb
