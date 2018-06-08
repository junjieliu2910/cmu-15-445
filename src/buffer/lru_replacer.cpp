/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template<typename T>
LRUReplacer<T>::LRUReplacer() {}

template<typename T>
LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template<typename T>
void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(mtx);
  if (hash.find(value) != hash.end()) {
    auto iter = hash[value];
    lst.erase(iter);
  }

  lst.insert(lst.begin(), value);
  hash[value] = lst.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template<typename T>
bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> guard(mtx);
  if (lst.empty()) {
    return false;
  }
  value = *lst.rbegin();
  lst.pop_back();
  hash.erase(value);
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template<typename T>
bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> guard(mtx);
  if (hash.find(value) == hash.end()) {
    return false;
  }
  auto iter = hash[value];
  lst.erase(iter);
  hash.erase(value);
  return true;
}

template<typename T>
size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> guard(mtx);
  return hash.size();
}

template
class LRUReplacer<Page *>;
// test only
template
class LRUReplacer<int>;

} // namespace cmudb