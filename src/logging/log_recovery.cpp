/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {
  // Get the log header
  int32_t size_ = *reinterpret_cast<const int *>(data);
  lsn_t lsn_ = *reinterpret_cast<const lsn_t *>(data + 4);;
  txn_id_t txn_id_ = *reinterpret_cast<const lsn_t *>(data + 8);
  lsn_t prev_lsn_ = *reinterpret_cast<const lsn_t *>(data + 12);
  LogRecordType log_record_type_ = *reinterpret_cast<const LogRecordType *>(data + 16);

  if (size_ < 0 || lsn_ == INVALID_LSN || txn_id_ == INVALID_TXN_ID ||
      log_record_type_ == LogRecordType::INVALID) {
    return false;
  }

  log_record.size_ = size_;
  log_record.lsn_ = lsn_;
  log_record.txn_id_ = txn_id_;
  log_record.prev_lsn_ = prev_lsn_;
  log_record.log_record_type_ = log_record_type_;

  switch (log_record_type_) {
  case LogRecordType::INSERT: {
    log_record.insert_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.insert_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;
  }
  case LogRecordType::MARKDELETE:
  case LogRecordType::ROLLBACKDELETE:
  case LogRecordType::APPLYDELETE: {
    log_record.delete_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.delete_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;
  }
  case LogRecordType::UPDATE: {
    log_record.update_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.old_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    log_record.new_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID) +
        log_record.old_tuple_.GetLength());
    break;
  }
  case LogRecordType::NEWPAGE: {
    log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(
        data + LogRecord::HEADER_SIZE);
    break;
  }
  default:break;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  
  while(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)){

    LogRecord record; 
    int buffer_offset = 0;
    while(DeserializeLogRecord(log_buffer_ + buffer_offset, record)){
      lsn_mapping_[record.GetLSN()] = offset_ + buffer_offset;
      buffer_offset += record.GetSize();


      switch(record.GetLogRecordType()){
        case LogRecordType::BEGIN :
          active_txn_[record.GetTxnId()] = record.GetLSN();
          break;
        case LogRecordType::COMMIT :
        case LogRecordType::ABORT :
          active_txn_.erase(record.GetTxnId());
          break;
        case LogRecordType::INSERT :{
          auto rid = record.GetInsertRID();
          auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          assert(page != nullptr);
          if(page->GetLSN() >= record.GetLSN()){
            break;
          }
          TablePage *tablePage = reinterpret_cast<TablePage *>(page);
          tablePage->InsertTuple(record.insert_tuple_, rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break; 
        }
        case LogRecordType::UPDATE : {
          auto rid = record.update_rid_;
          auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          assert(page != nullptr);
          if(page->GetLSN() >= record.GetLSN()){
            break;
          }
          TablePage *tablePage = reinterpret_cast<TablePage *>(page);
          tablePage->UpdateTuple(record.new_tuple_, record.old_tuple_, rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break; 
        }
        case LogRecordType::APPLYDELETE : {
          auto rid = record.GetDeleteRID();
          auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          assert(page != nullptr);
          if(page->GetLSN() >= record.GetLSN()){
            break;
          }
          TablePage *tablePage = reinterpret_cast<TablePage *>(page);
          tablePage->ApplyDelete(rid, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        }
        case LogRecordType::MARKDELETE : {
          auto rid = record.GetDeleteRID();
          auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          assert(page != nullptr);
          if(page->GetLSN() >= record.GetLSN()){
            break;
          }
          TablePage *tablePage = reinterpret_cast<TablePage *>(page);
          tablePage->MarkDelete(rid, nullptr, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        }
        case LogRecordType::ROLLBACKDELETE : {
          auto rid = record.GetDeleteRID();
          auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          assert(page != nullptr);
          if(page->GetLSN() >= record.GetLSN()){
            break;
          }
          TablePage *tablePage = reinterpret_cast<TablePage *>(page);
          tablePage->RollbackDelete(rid, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        }
        case LogRecordType::NEWPAGE : {
          auto page_id = record.prev_page_id_;
          TablePage* tablePage = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
          tablePage->Init(page_id, PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
          buffer_pool_manager_->UnpinPage(page_id, true);
          break;
        }
        default: break;
      }
    }
    offset_ += buffer_offset;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  if(!ENABLE_LOGGING){
    return; 
  }
  
  for(auto txn = active_txn_.begin(); txn != active_txn_.end(); ++txn){
    auto last_lsn = txn->second;
    int last_lsn_offset = lsn_mapping_[last_lsn];
    LogRecord record;
    while(true){
      //undo untill the begin
      assert(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, last_lsn_offset));
      assert(DeserializeLogRecord(log_buffer_, record));

      LOG_INFO("Undo: %s", record.ToString().c_str());

      auto log_type = record.GetLogRecordType();
      if(log_type == LogRecordType::BEGIN){
        break;
      }else if(log_type == LogRecordType::INSERT){
        // Insert
        auto rid = record.GetInsertRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        assert(page != nullptr);
        if(page->GetLSN() < record.GetLSN()){
          last_lsn = record.GetPrevLSN();
          last_lsn_offset = lsn_mapping_[last_lsn];
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->ApplyDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if(log_type == LogRecordType::APPLYDELETE){
        // Delete 
        auto rid = record.GetDeleteRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        assert(page != nullptr);
        if(page->GetLSN() < record.GetLSN()){
          last_lsn = record.GetPrevLSN();
          last_lsn_offset = lsn_mapping_[last_lsn];
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->InsertTuple(record.delete_tuple_ ,rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if(log_type == LogRecordType::MARKDELETE){
        auto rid = record.GetDeleteRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        assert(page != nullptr);
        if(page->GetLSN() < record.GetLSN()){
          last_lsn = record.GetPrevLSN();
          last_lsn_offset = lsn_mapping_[last_lsn];
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->RollbackDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (log_type == LogRecordType::ROLLBACKDELETE){
        auto rid = record.GetDeleteRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        assert(page != nullptr);
        if(page->GetLSN() < record.GetLSN()){
          last_lsn = record.GetPrevLSN();
          last_lsn_offset = lsn_mapping_[last_lsn];
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (log_type == LogRecordType::UPDATE) {
        auto rid = record.GetDeleteRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        assert(page != nullptr);
        if(page->GetLSN() < record.GetLSN()){
          last_lsn = record.GetPrevLSN();
          last_lsn_offset = lsn_mapping_[last_lsn];
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->UpdateTuple(record.old_tuple_, record.new_tuple_, rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      last_lsn = record.GetPrevLSN();
      last_lsn_offset = lsn_mapping_[last_lsn];
    }
  }
}

} // namespace cmudb
