/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <condition_variable>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockMode { SHARED = 0, EXCLUSIVE }; // lock mode for list item 

class LockManager {

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){
    //lock_table_ = new std::unordered_map<RID, std::list<Transaction *>>;
    // This is not efficient, better to intialize when first lock
  };

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/
  bool IsValidToLock(Transaction *txn);

  class LockList {
    public:
      LockList(Transaction* txn, LockMode mode){
        list_.push_back(std::make_pair(txn, mode));
      }

      inline bool IsEmpty(){return list_.empty();}

      inline void Add(Transaction* txn, LockMode mode){
        list_.push_back(std::make_pair(txn, mode));
      }
      
      inline void push_front(Transaction* txn, LockMode mode){
        list_.push_back(std::make_pair(txn, mode));
      }

      void MoveToFront(){
        // move an element to the begin of the list
      }

      bool CanAddShardLock(){
        auto first = list_.begin();
        if(first->second == LockMode::SHARED){
          return true;
        }
        return false;
      }

    private:
      std::list<std::pair<Transaction*, LockMode>> list_;
  };

private:
  bool strict_2PL_;
  std::unordered_map<RID, std::shared_ptr<LockList>> lock_table_;
  std::mutex mutex_;
  std::condition_variable cond;
};

} // namespace cmudb
