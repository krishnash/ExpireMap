#ifndef EXPIRE_MAP_H
#define EXPIRE_MAP_H

#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <cassert>
using namespace std;
#include <chrono>
using namespace std::chrono;
#include <pthread.h>

//
// ExpireMap
// ==============================================================================
// 
// This class provides a map that stores KV pairs with an associated expiry time.
// Entries become invalid after the expiry given while adding an element.
//
// Constructor does not take any arguments.
//
// void put(K key, V value, long timeoutMs)
// - Adds a the key value pair to the map. The KV pair is valid from the
//   time of put call to timeoutMs milliseconds in the future.
// - Time is tracked internally in microseconds.
// - The value is valid till time current time + timeoutMs (Inclusive.
//  End time is included).
// - When a put is called for an existing key, the value and timeout are
//  overwritten with the new input.
// - Put calls with zero or negative timeout are ignored
//
// V get(K key)
// - Returns an unexpired value for the key if it exists in the map
// - Returns NULL otherwise
//
// void remove(Key key)
// - Removes an entry from the ExpireMap
//
template <class Key, class Value>
class ExpireMap {
    public: // Types
        // Types for hash table to store and lookup KVs
        typedef struct TimedValue {
            Value value;
            long long expiry;
            TimedValue() : value(), expiry(0) { }
            TimedValue(Value p_value, long long p_expiry)
                : value(p_value), expiry(p_expiry) { }
        } TimedValue;
        typedef unordered_map<Key, TimedValue> KVStore;
        // Type to track expired KVs
        // This is an ordered map keyed by expiry times in order from earliest to latest.
        // The value for each expiry time is a unique set of keys expiring at that time.
        typedef map<long long, set<Key> > ExpiryQueue;
        typedef typename ExpiryQueue::iterator ExpiryIterator;
        typedef typename set<Key>::iterator KeyIterator;

    private: // Data
        KVStore _data_table;        // Hash table to store and lookup KVs
        ExpiryQueue _expiry_queue;  // Queue to track KVs in order of expiry
        bool _shutdown;             // Tracks if shutdown has been initiated. Set in the
                                    // destructor. ExpireMap stops serving set/get requests once
                                    // this has been set.
        pthread_t eviction_thread;  // Thread that evicts invalid entries from the data table
    private: // Data protection
        pthread_rwlock_t data_tbl_lock;
        pthread_mutex_t expiry_q_lock;

    public: // Constructor/Desctructor
    ExpireMap();
    ~ExpireMap();

    public: // Accessors
        // If there is no entry with the key in the map, add the key/value pair as a new
        // entry.
        // If there is an existing entry with the key, the current entry will be replaced
        // with the new key/value pair.
        // If the newly added entry is not removed after timeoutMs since it's added to
        // the map, remove it.
        void put(Key key, Value value, long timeoutMs);
        // Get the value associated with the key if present; otherwise, return null.
        Value get(Key key);
        // Remove the entry associated with key, if any.
        void remove(Key key);

    private: // Helpers
        // Evicts all expired entries from data table in order of earliest expired to latest.
        // Clears expired entries from expiry queue.
        // Returns time to sleep in microseconds.
        long long _evict();
        // Erase an entry from the expiry queue
        // No op if the entry does not exist in the expiry queue. Eviction could have discarded
        // the entry by the time this function got to the removal. So asserting that they key
        // exists in the expiry queue is invalid
        void _byol_erase_from_expiry_queue(long long expiry, Key key);

    public: // Garbage collection
        // Function for eviction thread
        static void* eviction(void* arg);

    public: // APIs for test
        // Returns size of the data table
        int debug_size() {
            pthread_rwlock_rdlock(&data_tbl_lock);
            int sz = _data_table.size();
            pthread_rwlock_unlock(&data_tbl_lock);
            return sz;
        }
}; // ExpireMap

#include "expire_map.hh"

#endif // EXPIRE_MAP_H
