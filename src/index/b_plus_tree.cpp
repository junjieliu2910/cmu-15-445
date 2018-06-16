/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"


namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), split_count_(0) {
    //   
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    //
    if(IsEmpty()) return false;
    LOG_INFO("GetValue");
    if(transaction == nullptr){
        LOG_INFO("nullptr txn");
    }
    auto leaf_page = FindLeafPage(key, false, transaction, Operation::SEARCH);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    ValueType tmp;
    if(leaf->Lookup(key, tmp, comparator_)){
        result.push_back(tmp);
        leaf_page->RUnlatch();
        if(transaction != nullptr)
            transaction->GetPageSet()->pop_front();
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return true;
    }else{
        leaf_page->RUnlatch();
        if(transaction != nullptr)
            transaction->GetPageSet()->pop_front();
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return false;
    }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    //
    LOG_INFO("Insert");
    // Tried to use call_flag and call_one but fail here.
    { 
        std::lock_guard<std::mutex> locker(root_mutex_);
        if(IsEmpty()){
            LOG_INFO("Start new tree");
            StartNewTree(key, value, transaction);
            return true;
        }
    }
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction* txn) {

    assert(IsEmpty());
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if(page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX, "Out of memory");
    }
    page->WLatch();
    LOG_INFO("Write lock page:%d", page->GetPageId());
    txn->GetPageSet()->push_back(page);
    auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    UpdateRootPageId(true);
    root->Init(root_page_id_, INVALID_PAGE_ID);
    assert(!IsEmpty());
    root->Insert(key, value, comparator_);
    page->WUnlatch();
    LOG_INFO("Write unlock page:%d", page->GetPageId());
    txn->GetPageSet()->pop_back();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    //Do not need to check empty, since this is only called inside Insert function
    auto leaf_page = FindLeafPage(key, false, transaction, Operation::INSERT);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    ValueType tmp;
    if(leaf->Lookup(key, tmp, comparator_)){
        //find
        //UnlockParentPage(leaf_page, transaction, Operation::INSERT);
        leaf_page->WUnlatch();
        if(leaf_page->GetPageId() == root_page_id_){
            UnlockRoot();
        }
        LOG_INFO("Write unlock page:%d", leaf_page->GetPageId());
        transaction->GetPageSet()->pop_front();
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return false;
    }else{
        // Not find
        if(leaf->GetSize() < leaf->GetMaxSize()){
            // leaf is not full
            //UnlockParentPage(leaf_page, transaction, Operation::INSERT);
            leaf->Insert(key, value, comparator_);
            leaf_page->WUnlatch();
            if(leaf_page->GetPageId() == root_page_id_){
                UnlockRoot();
            }
            LOG_INFO("Write unlock page:%d", leaf_page->GetPageId());
            transaction->GetPageSet()->pop_front();
            buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
        }else{
            auto new_leaf_node = Split(leaf);
            assert(new_leaf_node->IsLeafPage()); 
            KeyType middle_one = new_leaf_node->KeyAt(0);
            // Split the right half to new node
            if(comparator_(key, middle_one) < 0){
                leaf->Insert(key, value, comparator_);
            }else{
                new_leaf_node->Insert(key, value, comparator_);
            }
            new_leaf_node->SetNextPageId(leaf->GetNextPageId());
            leaf->SetNextPageId(new_leaf_node->GetPageId());
            InsertIntoParent(leaf, middle_one, new_leaf_node, transaction);
            // Insert finish
            UnlockParentPage(leaf_page, transaction, Operation::INSERT);
            leaf_page->WUnlatch();
            LOG_INFO("Write unlock page:%d", leaf_page->GetPageId());
            transaction->GetPageSet()->pop_front();
        }
        return true;
    }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(new_page_id);
    if(new_page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX, "Split: Out of memory");
    }
    
    N* new_node = reinterpret_cast<N *>(new_page->GetData());
    new_node->Init(new_page_id, INVALID_PAGE_ID);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    split_count_ ++;

    assert(new_page->GetPinCount() == 1 && new_page->GetPageId() == new_node->GetPageId()); 
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    //
    if(old_node->IsRootPage()){
        //If the old node is the root node
        auto page = buffer_pool_manager_->NewPage(root_page_id_);
        if(page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX, "InsertIntoParent: Out of memory");
        }
        page->WLatch();
        LOG_INFO("Write lock root page:%d", page->GetPageId());
        transaction->GetPageSet()->push_back(page);
        auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
        // generate new root
        new_root->Init(root_page_id_, INVALID_PAGE_ID);
        assert(new_root->GetPageId() == root_page_id_);
        new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        // Update the children page 
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        // Update root page id instead of inserting new root 
        UpdateRootPageId(false);
        split_count_++;
        // Unpin pages 
        page->WUnlatch();
        UnlockRoot();
        LOG_INFO("Write unlock root page:%d", page->GetPageId());
        transaction->GetPageSet()->pop_back();
        buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    }else{
        //The node is not the root node 
        auto page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
        if(page==nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX, "Out of memory");
        }

        auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
        
        if(parent->GetSize() < parent->GetMaxSize()){
            parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(parent->GetPageId());
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true); 
            buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
        }else{
            //Parent page is full, split parent page 
            auto new_internal = Split(parent);
            new_internal->SetParentPageId(parent->GetParentPageId());
            KeyType mid_one = new_internal->KeyAt(0);
            if(comparator_(key, mid_one) < 0){
                parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
                new_node->SetParentPageId(parent->GetPageId());
            }else{
                new_internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
                new_node->SetParentPageId(new_internal->GetPageId());
            }

            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
            assert(page->GetPinCount() == 1);
            InsertIntoParent(parent, mid_one, new_internal);
        }
    }
    
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {

    auto leaf_page = FindLeafPage(key, false, transaction, Operation::DELETE);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    if(leaf==nullptr){
        // The tree is empty 
        return;
    }
    int size = leaf->GetSize();
    int size_after_deletion = leaf->RemoveAndDeleteRecord(key, comparator_);
    if(size==size_after_deletion){
        //Key not found
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
        return;
    }
    //Key found and delete 
    if(size_after_deletion < leaf->GetMinSize()){
        CoalesceOrRedistribute(leaf, transaction);
    }else{
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    if(node->IsRootPage()){
        // node is root page 
        return AdjustRoot(node);
    }else{
        // node is not root node, get the parent and sibling node 
        auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
        if(page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX, "CoalesceOrRedistribute: out of memory");
        }
        auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
        /* Always get the right neighborhood, if current one is the right more one, 
         * then get the left neighborhood. 
         * Alway merge to left
         */
        int index_in_parent = parent->ValueIndex(node->GetPageId());
        if(index_in_parent == 0){
            // The left most node 
            auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_in_parent+1));
            assert(neighbor_page != nullptr);
            N* neighbor_node = reinterpret_cast<N *>(neighbor_page->GetData());
            if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
                Coalesce(node, neighbor_node, parent, index_in_parent+1, transaction);
                return false;
            }else{
                Redistribute(neighbor_node, node, index_in_parent);
                //assert(page->GetPinCount() == 1);
                buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
                return false;
            }
        }else{
            auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_in_parent-1));
            assert(neighbor_page != nullptr);
            N* neighbor_node = reinterpret_cast<N *>(neighbor_page->GetData());
            if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
                Coalesce(neighbor_node, node, parent, index_in_parent, transaction);
                return true;
            }else{
                Redistribute(neighbor_node, node, index_in_parent);
                //assert(page->GetPinCount() == 1);
                buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
                return false;
            }
        }
    }
    
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
    //   
    int parent_index = index;
    node->MoveAllTo(neighbor_node, parent_index, buffer_pool_manager_);
    parent->Remove(parent_index);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(node->GetPageId());
    if(parent->GetSize() < parent->GetMinSize()){
        return CoalesceOrRedistribute(parent, transaction);
    }else {
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return false;
    }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    // Redistribute does not need to deal with recursively coalesce or distribution
    if(index == 0){
        // Move sibling's first to node' last 
        // The function MoveFisrtToEndOf already update the related parent and child page 
        if(node->GetSize() < neighbor_node->GetSize())
            neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
        else
            node->MoveLastToFrontOf(neighbor_node, index+1, buffer_pool_manager_);
    }else{
        if(node->GetSize() < neighbor_node->GetSize())
            neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
        else
            node->MoveFirstToEndOf(neighbor_node, buffer_pool_manager_);
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    if(!old_root_node->IsLeafPage()){
        // Root is not leaf page and have only one child
        auto root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
        //
        if(root_node->GetSize() == 1){
            root_page_id_ = root_node->ValueAt(0);
            UpdateRootPageId(false);
            auto page = buffer_pool_manager_->FetchPage(root_page_id_);
            auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
            if(node->IsLeafPage()){
                auto new_root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
                new_root->SetParentPageId(INVALID_PAGE_ID);
            }else{
                auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(node);
                new_root->SetParentPageId(INVALID_PAGE_ID);
            }
            buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(root_page_id_, true);
            buffer_pool_manager_->DeletePage(root_node->GetPageId());
            return true;
        }
        return false;
    }else{
        //Root is leaf page, delete when size == 0
        B_PLUS_TREE_LEAF_PAGE_TYPE *root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(old_root_node);
        if(root_node->GetSize() == 0){
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
            buffer_pool_manager_->DeletePage(root_node->GetPageId());
            return true;
        }
        return false;
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { 
    KeyType tmp;
    auto leaf_page = FindLeafPage(tmp, true, nullptr, Operation::SEARCH);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    auto page_id = leaf->GetPageId();
    leaf_page->RUnlatch();
    LOG_INFO("Read unlock page:%d", leaf_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    auto leaf_page = FindLeafPage(key, false, nullptr, Operation::SEARCH);
    auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    int index = leaf->KeyIndex(key, comparator_);
    auto page_id = leaf->GetPageId();
    leaf_page->RUnlatch();
    LOG_INFO("Read unlock page:%d", leaf_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page* page, Transaction* txn, Operation op){
    if(op == Operation::SEARCH){
        page->RLatch();
        LOG_INFO("Read lock page: %d", page->GetPageId());
    }else{
        page->WLatch();
        LOG_INFO("Write lock page: %d", page->GetPageId()); 
    }
    if(txn != nullptr)
        txn->GetPageSet()->push_back(page);
}

/*
 If current node is safe, all the lock held by parent can be released
 Safe condition:
    Current size < Max Size
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockParentPage(Page* page, Transaction* txn, Operation op){
    if(txn==nullptr){
        LOG_INFO("here, nullptr");
    }
    if(txn->GetPageSet()->empty()) return;
    while(txn->GetPageSet()->front() != page){
        auto front = txn->GetPageSet()->front();
        if(op == Operation::SEARCH){
            front->RUnlatch();
            LOG_INFO("Read unlock page:%d", front->GetPageId());
        }else{
            front->WUnlatch(); 
            if(front->GetPageId() == root_page_id_){
                UnlockRoot();
            }
            LOG_INFO("Write unlock page:%d", front->GetPageId());
        }
        txn->GetPageSet()->pop_front();
    }
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost,
                                                         Transaction* txn,
                                                         Operation op) {
    // Check empty
    if(IsEmpty()) return nullptr;  
    /* 
     When root page is write latched, cannot access
     root page_id
     */
    if(op != Operation::SEARCH){
        // All write latch will lock
        LockRoot();
    }
    auto page = buffer_pool_manager_->FetchPage(root_page_id_);
    if(page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX, "Out of memory");
    }
    LockPage(page, txn, op);
    BPlusTreePage* node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while(!node->IsLeafPage()){
        auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(node);
        page_id_t child_page_id;
        if(leftMost){
            child_page_id = internal->ValueAt(0);
        }else{
            child_page_id = internal->Lookup(key, comparator_);
        }
        
        if(txn==nullptr) {
            page->RUnlatch();
            LOG_INFO("Read unlock page: %d", page->GetPageId());
        }

        page = buffer_pool_manager_->FetchPage(child_page_id);
        if(page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX, "Out of memory");
        }
        LockPage(page, txn, op);
        node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        
        if(txn != nullptr){
            if(op==Operation::SEARCH || 
            (op==Operation::INSERT && internal->GetSize() < internal->GetMaxSize())|| 
            (op==Operation::DELETE && internal->GetSize() > internal->GetMinSize())){
                // Search, or current page is safe
                UnlockParentPage(page, txn, op);
            }
        }
        buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
    }
    // Now node is leaf page
    //auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { 
     if (IsEmpty()) {
    return "Empty tree";
  }
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty()) {
    node = todo.front();
    if (first) {
      first = false;
      tree << "| ";
    }
    // leaf page, print all key-value pairs
    if (node->IsLeafPage()) {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
    } else {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty()) {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }
    // unpin node when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
