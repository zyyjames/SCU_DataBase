#include <list>
#include <iostream>
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :GlobalDepth(1),BucketSize(size) 
{
	Buckets.push_back(new map<K,V>);
	BucketsDepth.push_back(1);
	Buckets.push_back(new map<K,V>);
	BucketsDepth.push_back(1);
}
template <typename K, typename V>
ExtendibleHash<K, V>::~ExtendibleHash() 
{
    set<map<K, V>*> Unique;
    for (auto Elem : Buckets)Unique.emplace(Elem);
    for (auto Elem : Unique)delete Elem;
}
/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
	return ((long long int)key) & ((1 << GlobalDepth) - 1);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
	return GlobalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
	return BucketsDepth[bucket_id];
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
	return Buckets.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    map<K, V>* Bucket = Buckets[HashKey(key)];
    auto Iter = Bucket->find(key);
    if (Iter == Bucket->end())return false;
    else
    {
        value = Iter->second;
        return true;
    }
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    map<K, V>* Bucket = Buckets[HashKey(key)];
    auto Iter = Bucket->find(key);
    if (Iter == Bucket->end())return false;
    else
    {
        Bucket->erase(key);
        return true;
    }
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) 
{
    int BucketId = HashKey(key);
    if ((int)Buckets[BucketId]->size() == BucketSize)
    {
        int LocalDepth = ++BucketsDepth[BucketId];
        int PairIndex = BucketId ^ (1 << (LocalDepth - 1));
        int Inc = 1 << LocalDepth;
        int Num = 1 << GlobalDepth;
        if (LocalDepth > GlobalDepth)
        {
            for (int i = 0; i < Num; i++)
            {
				
                Buckets.push_back(Buckets[i]);
                BucketsDepth.push_back(BucketsDepth[i]);
			}
			GlobalDepth++;
		}
        Buckets[PairIndex] = new map<K, V>;
        BucketsDepth[PairIndex]=LocalDepth;
        map<K, V> Temp = *Buckets[BucketId];
        Buckets[BucketId]->clear();
        for (int i = PairIndex - Inc; i >= 0; i -= Inc)
        {
			Buckets[i] = Buckets[PairIndex];
			BucketsDepth[i]=LocalDepth;
		}
        for (int i = PairIndex + Inc; i < Num; i += Inc)
        {
			Buckets[i] = Buckets[PairIndex];
			BucketsDepth[i]=LocalDepth;
		}
        for (auto& Elem : Temp)Insert(Elem.first, Elem.second);
        Insert(key, value);
    }
    else Buckets[BucketId]->emplace(key, value);
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
