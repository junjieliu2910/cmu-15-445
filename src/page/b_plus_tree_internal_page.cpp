/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(1);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage))/
      (sizeof(KeyType) + sizeof(ValueType));
    SetMaxSize(size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    assert(0 <= index && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(0 <= index && index < GetSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    // The value is note sorted, can only use sequential scan
    for(int i = 0; i < GetSize(); ++i){
        if(array[i].second == value){
            return i;
        }
    }
    return GetSize()-1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(0 <= index && index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    //assert(GetSize() > 1);
    if(comparator(key, array[1].first) < 0){
        return array[0].second;
    }else if(comparator(key, array[GetSize()-1].first) > 0){
        return array[GetSize()-1].second;
    }

    for(int i = 1; i < GetSize(); ++i){
        if(comparator(key, array[i].first) < 0){
            return array[i-1].second;
        }
    }
    return array[GetSize()-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    array[0].second = old_value;
    array[1] = std::make_pair(new_key, new_value);
    IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    // Add code here
    int size = GetSize();
    assert(size < GetMaxSize());
    int value_index = ValueIndex(old_value);
    memmove(array+value_index+2, array+value_index+1, (size-value_index-1)*sizeof(MappingType));
    array[value_index+1] = std::make_pair(new_key, new_value);
    IncreaseSize(1);
    return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    // Move the right half part to the recipient
    int half = (GetSize()+1)/2;
    recipient->CopyHalfFrom(array+GetSize()-half, half, buffer_pool_manager);
    //Update the parent page id for right child
    for(int i = GetSize() - half; i < GetSize(); ++i){
        auto child_page = buffer_pool_manager->FetchPage(array[i].second);
        assert(child_page != nullptr);
        auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);
    }
    IncreaseSize(-1*half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    //
    assert(!IsLeafPage() && GetSize() == 1 && size > 0);
    memcpy(array, items, size*sizeof(MappingType));
    IncreaseSize(size-1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    int size = GetSize();
    assert(size > 1);
    memmove(array+index, array+index+1, (size-index-1)*sizeof(MappingType));
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    int size = GetSize();
    assert(size == 2);
    return array[1].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
    //
    assert(recipient->GetParentPageId() == GetParentPageId());
    int size = GetSize();
    // Update the data in parent page
    auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
    //LOG_INFO("MoveAllto, parent page pin count: %d", parent_page->GetPinCount());
    //assert(parent_page != nullptr && parent_page->GetPinCount() == 2);
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
    // Need to change the related key value in parent page
    SetKeyAt(0, parent_node->KeyAt(index_in_parent));
    buffer_pool_manager->UnpinPage(parent_node->GetPageId(), false);
    recipient->CopyAllFrom(array, size, buffer_pool_manager);
    // Update the parent page id of the child
    for(int i = 0; i < size; ++i){
        auto page = buffer_pool_manager->FetchPage(array[i].second);
        assert(page != nullptr);
        auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        node->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    }
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    //
    int current_size = GetSize();
    assert(current_size + size <= GetMaxSize());
    memcpy(array+current_size, items, (size_t)size*sizeof(MappingType));
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    //
    int size = GetSize();
    assert(size > GetMinSize());
    //Construct the pair
    auto page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    int parent_index = parent->ValueIndex(GetPageId());
    recipient->CopyLastFrom(std::make_pair(parent->KeyAt(parent_index), array[0].second), buffer_pool_manager);
    //Change parent page
    parent->SetKeyAt(parent_index, array[1].first);
    Remove(0); // Size already reduce by 1 here
    //Unpin pages
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    //
    int size = GetSize();
    array[size] = pair;
    IncreaseSize(1);
    // Change state in child page
    auto child_page = buffer_pool_manager->FetchPage(pair.second);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    //
    int size = GetSize();
    assert(size > GetMinSize());
    recipient->CopyFirstFrom(array[size-1], parent_index, buffer_pool_manager);
    IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    //
    int size = GetSize();
    assert(size < GetMaxSize());
    //Get parent page
    auto page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    array[0].first = parent->KeyAt(parent_index);
    //Insert into the first position
    memmove(array+1, array, size*sizeof(MappingType));
    array[0] = pair;
    //Change parent page
    parent->SetKeyAt(parent_index, pair.first);

    //Change child page
    page = buffer_pool_manager->FetchPage(pair.second);
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(GetPageId());

    //unpin pages
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
