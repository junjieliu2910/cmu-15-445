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
#include "common/logger.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockMode { SHARED = 0, EXCLUSIVE }; // lock mode for list item 

struct LockItem {
    
  explicit LockItem(txn_id_t id, LockMode mode, bool status):
  tid_(id), mode_(mode), held_(status){}
  
  txn_id_t tid_;
  LockMode mode_;
  bool held_; // For strict 2PL
};

class LockList {
  public:
    LockList(txn_id_t id, LockMode mode, bool status){
      list_.push_back(LockItem(id, mode, status));
      oldest_ = id;
    }

    inline bool IsEmpty(){return list_.empty();}
    inline txn_id_t GetOldest(){return oldest_;}
    inline std::list<LockItem>::iterator Begin(){return list_.begin();}

    void Add(txn_id_t id, LockMode mode, bool status){
      bool flag = true; 
      for(auto i = list_.begin(); i != list_.end(); ++i){
        if(i->held_) continue;
        if(i->tid_ > id){
          //Find first larger than
          list_.insert(i, LockItem(id, mode, status));
          flag = false;
          break; 
        }
      }
      // Not insert in middle, then push back
      if(flag)
        list_.push_back(LockItem(id, mode, status));
    }
    
    void Push_front(txn_id_t id, LockMode mode, bool status){
      list_.push_front(LockItem(id, mode, status));
      if(id < oldest_) oldest_ = id; // Update oldest txn
    }

    LockItem Find(txn_id_t id){
      for(auto i = list_.begin(); i != list_.end(); ++i){
        if(i->tid_ == id){
          return *i;
        }
      } 
      return LockItem(-1, LockMode::SHARED, false);
    }

    bool IsFirst(txn_id_t id){return list_.begin()->tid_ == id;}

    void Remove(txn_id_t id){
      // need to update the oldest id
      for(auto i = list_.begin(); i != list_.end(); ++i){
        if(i->tid_ == id){
          list_.erase(i);
          break;
        }
      }
      if(id == oldest_){
        // Update oldest_
        oldest_ = 10000000;
        if(!list_.empty()){
          for(auto i = list_.begin(); i != list_.end(); ++i){
            if(!i->held_) break;
            if(oldest_ > i->tid_) oldest_ = i->tid_;
          }
        }
      }
    }

    void Hold(txn_id_t id){
      for(auto i = list_.begin() ; i!=list_.end(); ++i){
        if(i->tid_ == id && !i->held_){
          i->held_ = true;
          return;
        }
      }
    }

    // void Upgrade(txn_id_t id){
    //   assert(!IsEmpty());
    //   for(auto i )
    // }

    // void MoveToFront(txn_id_t id){
    //   // move an element to the begin of the list
    //   assert(!IsEmpty())
    //   if(list_[0].tid_ == id) return;
    //   for(auto i = list_.begin(); i != list.end(); ++i){
    //     if(*i.tid_ == id){
    //       list_.Push_front(LockItem(id, *i.mode_, true));
    //     }
    //   }
    // }

    bool CanAddShardLock(){
      if(list_.empty()) return true;
      return list_.begin()->mode_ == LockMode::SHARED;
    }

  private:
    std::list<LockItem> list_;
    txn_id_t oldest_; // Wait-die, keep the oldest tid of multiple shared locks
};

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

private:
  bool strict_2PL_;
  std::unordered_map<RID, std::shared_ptr<LockList>> lock_table_;
  std::mutex mutex_;
  std::condition_variable cond;
};

} // namespace cmudb
