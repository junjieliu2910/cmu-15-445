/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */

void LogManager::ForceFlush(){
  std::unique_lock<std::mutex> locker(latch_);
  allow_to_flush_ = true;
  locker.unlock();
  cv_.notify_one();
  while(!persistent_lsn_changed_); // waiting 
  persistent_lsn_changed_ = false;
}

void LogManager::RunFlushThread() {
  // If ENABLE_LOGGING already set, no need to set up background thread again
  if(!ENABLE_LOGGING){
    ENABLE_LOGGING = true;
    flush_thread_ = new std::thread([&](){
      while(ENABLE_LOGGING){
        int flush_log_size = 0, flush_lsn = 0;
        std::unique_lock<std::mutex> locker(latch_);
        if(cv_.wait_for(locker, LOG_TIMEOUT) == std::cv_status::timeout 
           || allow_to_flush_){
          //Start condition, timeout or nitified
          LOG_INFO("Start flush");
          flush_log_size = offset_;
          SwapBuffer();
        }
        allow_to_flush_ = false;
        flush_lsn = next_lsn_ - 1;
        locker.unlock();

        if(ENABLE_LOGGING){
          disk_manager_->WriteLog(flush_buffer_, flush_log_size);
          SetPersistentLSN(flush_lsn);
          LOG_INFO("End flush, current flush lsn: %d", flush_lsn);
        }
      }
      LOG_INFO("Flush thread end");
    });
  }
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  if(ENABLE_LOGGING){
    ENABLE_LOGGING = false;
    std::unique_lock<std::mutex> locker(latch_);
    allow_to_flush_ = true;
    locker.unlock();
    cv_.notify_one();
   
    if(flush_thread_->joinable()){
      flush_thread_->join();
    }
  }
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  std::unique_lock<std::mutex> locker(latch_);
  // Current log buffer is full, wake flush thread 
  if(log_record.GetSize() > LOG_BUFFER_SIZE - offset_){
    allow_to_flush_ = true;
    locker.unlock();
    cv_.notify_all(); // notify the flusing thread 
    locker.lock();
  }

  // Current log buffer can held this record
  log_record.lsn_ = next_lsn_++;
  memcpy(log_buffer_ + offset_, &log_record, 20);
  int pos = offset_ + 20;

  if(log_record.log_record_type_ == LogRecordType::INSERT){
    // Insert log 
    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE ||
      log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE ||
      log_record.log_record_type_ == LogRecordType::APPLYDELETE){
    // Delete log 
    memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  
  } else if(log_record.log_record_type_ == LogRecordType::UPDATE){
    // Update log
    memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record.old_tuple_.GetLength();
    log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if(log_record.log_record_type_ == LogRecordType::NEWPAGE) {
    // New page log 
    memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(page_id_t));
  }

  offset_ += log_record.size_;
  return log_record.lsn_;
}

} // namespace cmudb
