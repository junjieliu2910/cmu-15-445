#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager *disk_manager,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new std::unordered_map<page_id_t, Page *>;
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);

  Page *page = nullptr;
  if (page_id == INVALID_PAGE_ID) { return page; }
  auto found = page_table_->find(page_id);
  if (found != page_table_->end()) {
    page = found->second;
    replacer_->Erase(page);
    pin_page(page);
    return page;
  }

  if (!free_list_->empty()) {
    page = *free_list_->begin();
    free_list_->pop_front();
    assert(page->pin_count_ == 0);
    assert(page->page_id_ == INVALID_PAGE_ID);
    assert(!page->is_dirty_);
  } else {
    if (!replacer_->Victim(page)) {
      LOG_INFO("All page are pinned");
      return nullptr;
    }
    assert(page->pin_count_ == 0);
    if (page->is_dirty_) {
      if(ENABLE_LOGGING && page->GetLSN() > log_manager_->GetPersistentLSN()){
        // force the log manager to flush
      }

      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    page_table_->erase(page->GetPageId());
  }

  disk_manager_->ReadPage(page_id, page->GetData());
  page_table_->insert(std::make_pair(page_id, page));
  page->page_id_ = page_id;
  pin_page(page);
  assert(page->pin_count_ == 1);
  assert(!page->is_dirty_);
  return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  Page *page = nullptr;
  auto found = page_table_->find(page_id);
  if (found == page_table_->end()) {
    return false;
  }
  page = found->second;
  if(page->pin_count_ == 0){
    LOG_INFO("Illeagal unpin page id: %d", page_id);
    assert(page->pin_count_ > 0);
  }
  page->pin_count_--;
  //LOG_INFO("Unpin,  page: %d, pin count:%d", page->GetPageId(), page->GetPinCount());
  if (page->pin_count_ == 0) {
    replacer_->Insert(page);
  }

  if (is_dirty) {
    page->is_dirty_ = true;
  }
  return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  Page *page = nullptr;
  auto found = page_table_->find(page_id);
  if (found == page_table_->end()) {
    return false;
  }
  page = found->second;

  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be responsible for removing this entry out
 * of page table, resetting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  Page *page = nullptr;
  //auto ret = page_table_->Find(page_id, page);
  auto found = page_table_->find(page_id);
  if (found!=page_table_->end()) {
    page = found->second;
    if (page->GetPinCount() != 0) {
      return false;
    }

    auto erase = replacer_->Erase(page);
    assert(erase);
    free_list_->insert(free_list_->end(), page);
    page_table_->erase(page_id);
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->ResetMemory();
  }

  disk_manager_->DeallocatePage(page_id);
  return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  std::lock_guard<std::mutex> guard(latch_);

  Page *page = nullptr;
  if (!free_list_->empty()) {
    page = *free_list_->begin();
    free_list_->pop_front();
    assert(page->pin_count_ == 0);
    assert(page->page_id_ == INVALID_PAGE_ID);
    assert(!page->is_dirty_);
  } else {
    if (!replacer_->Victim(page)) {
      return nullptr;
    }
    assert(page->pin_count_ == 0);
    if (page->is_dirty_) {
      if(ENABLE_LOGGING && page->GetLSN() > log_manager_->GetPersistentLSN()){
        // force the log manager to flush, and wait untill finish
        log_manager_->ForceFlush();
      }

      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    page_table_->erase(page->GetPageId());
  }
  page_id = disk_manager_->AllocatePage();

  page_table_->insert(std::make_pair(page_id, page));
  page->ResetMemory();
  page->page_id_ = page_id;
  page->is_dirty_ = true;
  pin_page(page);
  assert(page->pin_count_ == 1);


  return page;
}
} // namespace cmudb