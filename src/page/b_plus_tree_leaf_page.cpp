/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetNextPageId(INVALID_PAGE_ID);
    int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage))/
      (sizeof(KeyType) + sizeof(ValueType));
    SetMaxSize(size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
    int len = GetSize();
    int begin = 0, end = len;
    while(begin < end){
        int mid = (begin + end) /2 ;
        if(comparator(array[mid].first, key) == -1){
            begin = mid + 1;
        }else{
            end = mid;
        }
    }
    return begin;
    // for(int i = 0; i < GetSize(); ++i){
    //     if(comparator(array[i].first, key) >= 0){
    //         return i;
    //     }
    // }
    // return GetSize() - 1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
    assert(index >=0 && index < GetSize());
    return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    // Do not need to split here
    int size = GetSize();
    assert(size <= GetMaxSize());
    // bigger than the last one
    if(size == 0 || comparator(key, array[size-1].first) > 0){
        array[size] = std::make_pair(key, value);
        IncreaseSize(1);
        return size + 1;
    }else if(comparator(key, array[0].first) < 0){
        memmove(array + 1, array, static_cast<size_t>(GetSize()*sizeof(MappingType)));
        array[0] = std::make_pair(key, value);
        IncreaseSize(1);
        return size+1;
    }else{ 
        // in the middle of array
        int proper_index = KeyIndex(key, comparator);
        memmove(array + proper_index + 1, array + proper_index, static_cast<size_t>((GetSize() - proper_index)*sizeof(MappingType)));
        array[proper_index] = std::make_pair(key, value);
        IncreaseSize(1);
        return size + 1;
    }
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    // add here
    int size = (GetSize()+1)/2;
    // Move the right half to the recipient
    MappingType *src = array + GetSize() - size;
    recipient->CopyHalfFrom(src, size);
    IncreaseSize(-1*size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    assert(GetSize() == 0);
    memcpy(array, items, size*sizeof(MappingType));
    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    int size = GetSize();
    if(size == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(size-1)) > 0){
        return false;
    }
    int key_index = KeyIndex(key, comparator);
    if(comparator(array[key_index].first, key)==0){
        value = array[key_index].second;
        //LOG_INFO("Leaf page look up,  index: %d", key_index);
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
    // Ke does exist
    if(GetPageId()==700){
        LOG_INFO("Remove element in page 700");
    }

    int size = GetSize();
    if(size == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(size-1)) > 0){
        return size;
    }
    int key_index = KeyIndex(key, comparator);
    if(comparator(key, KeyAt(key_index)) == 0){
        memmove(array+key_index, array+key_index+1, (GetSize() - key_index - 1)*sizeof(MappingType));
        IncreaseSize(-1);
        size --;
    }
    return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
    //Move to left
    recipient->CopyAllFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    //Copy from right 
    int current_size = GetSize();
    assert(current_size + size <= GetMaxSize());
    memcpy(array + current_size, items, size*sizeof(MappingType));
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    //
    recipient->CopyLastFrom(array[0]);
    memmove(array, array + 1, (GetSize()-1)*sizeof(MappingType));
    IncreaseSize(-1);

    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while MoveFirstToEndOf");
    }
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    // Doubt here
    parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array[0].first);

    buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    int size = GetSize();
    assert(size < GetMaxSize());
    array[size] = item;
    IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
    //
    int size = GetSize();
    assert(size > GetMinSize());
    recipient->CopyFirstFrom(array[size-1], parentIndex, buffer_pool_manager);
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    assert(size < GetMaxSize());
    memmove(array + 1, array, size*sizeof(MappingType));
    array[0] = item;
    IncreaseSize(1);
    //Update parent page 
    auto page = buffer_pool_manager->FetchPage(GetParentPageId());
    if(page==nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX, "parent page not found when CopyFirstFrom");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    parent->SetKeyAt(parentIndex, array[0].first);
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
