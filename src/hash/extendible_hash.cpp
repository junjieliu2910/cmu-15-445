#include <list>
#include <cassert>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size): globalDepth(0), bucketSizeLimit(size) {
        bucketDirectory.push_back(std::make_shared<Bucket>(0));
    }

/*
 * helper function to calculate the hashing address of input key
 */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        return std::hash<K>{}(key);
    }

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        return globalDepth;
    }

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        return bucketDirectory[bucket_id]->localDepth;
    }

/*
 * helper function to return current number of bucket in hash table
 */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return static_cast<int>(bucketDirectory.size());
    }

/*
 * lookup function to find value associate with input key
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        std::lock_guard<std::mutex> guard(mtx);
        std::shared_ptr<Bucket> bucket = getBucket(key);
        if (bucket == nullptr || bucket->contents.find(key) == bucket->contents.end()) {
            return false;
        }
        value = bucket->contents[key];
        return true;
    }

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        std::lock_guard<std::mutex> guard(mtx);

        std::shared_ptr<Bucket> bucket = getBucket(key);
        if (bucket->contents.find(key) == bucket->contents.end()) {
            return false;
        }

        bucket->contents.erase(key);
        return true;
    }

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        std::lock_guard<std::mutex> guard(mtx);

        std::shared_ptr<Bucket> target = getBucket(key);
        int index = getBucketIndex(HashKey(key));
        while (target->contents.size() == bucketSizeLimit) {
            if (target->localDepth == globalDepth) {
                size_t length = bucketDirectory.size();
                for(size_t i = 0; i < length; i++){
                    bucketDirectory.push_back(bucketDirectory[i]);
                }
                globalDepth++;
            }
            int mask = (1 << target->localDepth );

            auto a = std::make_shared<Bucket>(target->localDepth + 1);
            auto b = std::make_shared<Bucket>(target->localDepth + 1);
            for(auto item : target->contents){
                size_t newKey = HashKey(item.first);
                if(newKey & mask){
                    b->contents.insert(item);
                }else{
                    a->contents.insert(item);
                }
            }
            for(size_t i = 0; i < bucketDirectory.size(); i++){
                if(bucketDirectory[i] == target){
                    if(i & mask){
                        bucketDirectory[i] = b;
                    }else{
                        bucketDirectory[i] = a;
                    }
                }
            }
            target = getBucket(key);
            index = getBucketIndex(HashKey(key));
        }

        bucketDirectory[index]->contents[key] = value;
    }

    template<typename K, typename V>
    int ExtendibleHash<K, V>::getBucketIndex(size_t hashKey) const {
        return static_cast<int>(hashKey & ((1 << globalDepth) - 1));
    }

    template<typename K, typename V>
    std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::getBucket(const K &key) {
        auto ret = bucketDirectory[getBucketIndex(HashKey(key))];
        return ret;
    };

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

// test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb