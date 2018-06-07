/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager* manager) : 
page_id_(page_id), index_(index), buffer_pool_manager_(manager) {
    auto page = buffer_pool_manager_->FetchPage(page_id_);
    if(page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX, "Index iterator: cannot get page");
    }
    leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(page_id_, false);
}


INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd(){
    auto next_id = leaf_->GetNextPageId();
    return (index_ >= leaf_->GetSize() && next_id == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*(){
    if(isEnd()){
        throw std::out_of_range("IndexIterator: out of range");
    }
    return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
IndexIterator<KeyType, ValueType, KeyComparator> &INDEXITERATOR_TYPE::operator++(){
    index_ ++;
    if(index_ >= leaf_->GetSize()){
        //Fetch next page 
        page_id_t next = leaf_->GetNextPageId();
        if(next != INVALID_PAGE_ID){
            buffer_pool_manager_->UnpinPage(page_id_, false);
            auto page = buffer_pool_manager_->FetchPage(next);
            if(page == nullptr){
                throw Exception(EXCEPTION_TYPE_INDEX, "Operator ++: cannot get page");
            }
            leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
            index_ = 0;
            page_id_ = next;
        }
    }
    return *this;
}


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
