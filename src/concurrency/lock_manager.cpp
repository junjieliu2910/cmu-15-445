/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

//Chech whether current transaction is valid to lock
bool LockManager::IsValidToLock(Transaction* txn){
  if(txn->GetState() == TransactionState::ABORTED) return false;
  if(txn->GetState() == TransactionState::COMMITTED) return false;
  if(txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return true;
}

/*
  Question for this part:
  1. If there are several txns that are waiting, and all are older than current one.
  When the lock release, should I grant the lock to the first required txn or the 
  oldest txn in the waiting list ?

  2. Is upgraded lock has highest priority ?

  In this implementation, I grant to the oldest txn in the waiting list.
  Since otherwise I have to abort all the other incompatiable waiting txn.
  */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  
  std::unique_lock<std::mutex> locker(mutex_);

  if(!IsValidToLock(txn)) return false;
  // No lock is granted 
  auto found_list = lock_table_.find(rid);
  if(found_list == lock_table_.end()){
    lock_table_.insert({rid, std::make_shared<LockList>(txn->GetTransactionId(), LockMode::SHARED, true)});
    locker.unlock();
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }

  //Chech whether current granted lock is shared 
  auto locklist = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();
  if(locklist->CanAddShardLock()){
    // First lock is shared 
    locklist->Push_front(tid, LockMode::SHARED, true);
    locker.unlock();
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }else{
    // First lock is exclusive, wait die 
    if(tid > locklist->GetOldest()){
      // younger than held txn, abort 
      txn->SetState(TransactionState::ABORTED);
      return false;
    }else{
      locklist->Add(tid, LockMode::SHARED, false);
      cond.wait(locker, [&](){
        return locklist->Begin()->tid_ == tid;
      });
      locklist->Hold(tid);
      locker.unlock();
      txn->GetSharedLockSet()->insert(rid);
      return true;
    }
  }
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(mutex_);
  if(!IsValidToLock(txn)) return false;

  // Rid not locked yet
  auto found_list = lock_table_.find(rid);
  if(found_list == lock_table_.end()){
    lock_table_.insert({rid, std::make_shared<LockList>(txn->GetTransactionId(), LockMode::EXCLUSIVE, true)});
    locker.unlock();
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
  }

  // Wait-die
  auto lock_list = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();
  if(tid > lock_list->GetOldest()){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }else{
    lock_list->Add(tid, LockMode::EXCLUSIVE, false);
    cond.wait(locker, [&](){
      return lock_list->Begin()->tid_ == tid;
    });
    lock_list->Hold(tid);
    locker.unlock();
    txn->GetExclusiveLockSet()->insert(rid);
    return true;
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> locker(mutex_);
  if(!IsValidToLock(txn)) return false;

  auto found_list = lock_table_.find(rid);
  if(found_list == lock_table_.end()){return false;}
  
  auto lock_list = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();
  /*
    Not sure whether upgrade should follow wait-die
   */
  if(tid > lock_list->GetOldest()){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }else{
    lock_list->Remove(tid);
    lock_list->Add(tid, LockMode::EXCLUSIVE, false);
    return true;
  }
}

/*
  Strict 2PL
  Unlock until committed or aborted 
 */
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  
  std::unique_lock<std::mutex> locker(mutex_);
  if(strict_2PL_){
   if(txn->GetState()!=TransactionState::ABORTED && 
      txn->GetState()!=TransactionState::COMMITTED){
      // Can not unlock in growing or shrinking state 
      txn->SetState(TransactionState::ABORTED);
   }
   return false;
  }else{
   if(txn->GetState() == TransactionState::GROWING){
     txn->SetState(TransactionState::SHRINKING);
   }
  }

  /*
    Exclusive lock must be the first one
   */
  txn_id_t tid = txn->GetTransactionId();
  auto lock_list = lock_table_.find(rid)->second;
  auto item = lock_list->Find(tid);
  item.held_ = false;
  if(item.mode_ == LockMode::SHARED){
    txn->GetSharedLockSet()->erase(rid);
  }else{
    txn->GetExclusiveLockSet()->erase(rid);
  }
  if(item.mode_ == LockMode::EXCLUSIVE || lock_list->IsFirst(tid)){
    cond.notify_all(); // Notify all waiting thread
  }
  lock_list->Remove(tid);
  return true;
}

} // namespace cmudb
