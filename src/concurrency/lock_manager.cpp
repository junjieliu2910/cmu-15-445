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
  For transacton in shrinking phase:
  1. RID is not held by other transaction or the required lock is 
  compatiable with current held lock. -> Grant the lock, add to 
  corresponding lock set in transaction 
  2. The lock require is not compatiable with current held lock, then
  wait.

  Abort condition:
  1. Require lock in shrinking/abort phase

  After abort:
  1. Unlock all the held lock
  2. Delete all the waiting requiest of this transaction 
  */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  
  std::unique_lock<std::mutex> locker(mutex_);

  if(!IsValidToLock(txn)) return false;
  // No lock is granted 
  auto found_list = lock_table_.find(rid);
  if(found_list == lock_table_.end()){
    lock_table_.push_back(std::make_shared<LockList>(txn, LockMode::SHARED));
    locker.unlock();
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }

  //Chech whether current granted lock is shared 
  auto locklist = found_list->second;
  if(locklist->CanAddShardLock()){
    // First lock is shared 
    locklist->push_front(txn, LockMode::SHARED);
    locker.unlock();
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }else{
    // First lock is exclusive 
    locklist->Add(txn, LockMode::SHARED);
    cond.wait(locker, locklist->CanAddShardLock);
    locklist->MoveToFront()
    locker.unlock();
    txn->GetSharedLockSet()->insert(rid);
    return true;
  }
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  
  return false;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return false;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  return false;
}

} // namespace cmudb
