#ifndef EXPIRE_MAP_HH
#define EXPIRE_MAP_HH

template <class Key, class Value>
ExpireMap<Key, Value>::
ExpireMap() : _data_table(), _expiry_queue(), _shutdown(false) {
    // In case the platform default for rwlocks does not prefer writes, it will lead to 
    // starvation of writers. Default attribute can be changed to prefer writers and block
    // further readers if a writer is waiting using attribute such as
    // PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
    int rwlock_ret = pthread_rwlock_init(&data_tbl_lock, NULL /* attr */);
    assert(rwlock_ret == 0);
    int mutex_ret = pthread_mutex_init(&expiry_q_lock, NULL /* attr */);
    assert(mutex_ret == 0);
    // Spawn eviction thread
    pthread_create(&eviction_thread, NULL /* attr */, eviction, this);
}

template <class Key, class Value>
ExpireMap<Key, Value>::
~ExpireMap() {
    _shutdown = true;
    // Wait for eviction thread to finish
    pthread_join(eviction_thread, NULL /* ret */);
    // Clear data structures
    _data_table.clear();
    _expiry_queue.clear();
    int rwlock_ret = pthread_rwlock_destroy(&data_tbl_lock);
    assert(rwlock_ret == 0);
    int mutex_ret = pthread_mutex_destroy(&expiry_q_lock);
    assert(mutex_ret == 0);
}

template <class Key, class Value>
void
ExpireMap<Key, Value>::
put(Key key, Value value, long timeoutMs) {
    // Do not insert values for which validity is less than or equal to zero
    if (_shutdown || timeoutMs <= 0) return;
    // Get current time in microseconds
    long long expiry =
        duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
    // Calculate expiry time
    expiry += (timeoutMs * 1000);

    bool overwrite = false;
    long long ow_expiry = 0;
    // Write lock data table
    pthread_rwlock_wrlock(&data_tbl_lock);
    // Record old value to update the expiry queue if this is an overwrite
    typename KVStore::iterator tbl_iter = _data_table.find(key);
    if (tbl_iter != _data_table.end()) {
        overwrite = true;
        ow_expiry = tbl_iter->second.expiry;
    }
    // Insert into data table
    _data_table[key] = TimedValue(value, expiry);
    // Unlock data table
    pthread_rwlock_unlock(&data_tbl_lock);

    // Lock expiry queue
    pthread_mutex_lock(&expiry_q_lock);
    // If this is an overwrite, remove older entry from the expiry queue
    if (overwrite) {
        _byol_erase_from_expiry_queue(key, ow_expiry);
    }
    ExpiryIterator insert_iter = _expiry_queue.find(expiry);
    // Insert a key set into expiry queue if this is the first KV expiring at 'expiry'
    if (insert_iter == _expiry_queue.end()) {
        // Function returns iterator to the newly inserted pair of (expiry:keyset)
        insert_iter = _expiry_queue.insert(make_pair(expiry, set<Key>())).first;
    }
    insert_iter->second.insert(key);
    // Unlock expiry queue
    pthread_mutex_unlock(&expiry_q_lock);
}

template <class Key, class Value>
Value
ExpireMap<Key, Value>::
get(Key key) {
    if  (_shutdown) {
        return (Value)NULL;
    }
    // Get current time in microseconds
    long long curtime =
        duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
    // Read lock data table
    pthread_rwlock_rdlock(&data_tbl_lock);
    typename KVStore::iterator iter = _data_table.find(key);
    // If value was not found or the value has expired and waiting to be evicted, return NULL
    if (iter == _data_table.end() || iter->second.expiry < curtime) {
        pthread_rwlock_unlock(&data_tbl_lock);
        return (Value) NULL;
    }
    // Key has a valid value, return the value
    Value value = iter->second.value;
    pthread_rwlock_unlock(&data_tbl_lock);
    // Unlock data table
    return value;
}

template <class Key, class Value>
void
ExpireMap<Key, Value>::
remove(Key key) {
    bool exists = false;
    long long expiry = 0;
    // Write lock data table
    pthread_rwlock_wrlock(&data_tbl_lock);
    typename KVStore::iterator tbl_iter = _data_table.find(key);
    // Remove the entry irrespective of the expiry time
    if (tbl_iter != _data_table.end()) {
        exists = true;
        expiry = tbl_iter->second.expiry;
        _data_table.erase(key);
    }
    // Unlock data table
    pthread_rwlock_unlock(&data_tbl_lock);

    // Remove entry from expiry queue.
    // Skip this step to make removal constant time. The down side is that the entry in expiry
    // queue will linger till the actual expiry of the removed entry.
    if (exists) {
        // Lock expiry queue
        pthread_mutex_lock(&expiry_q_lock);
        _byol_erase_from_expiry_queue(key, expiry);
        // Unlock expiry queue
        pthread_mutex_unlock(&expiry_q_lock);
    }
}

template <class Key, class Value>
long long
ExpireMap<Key, Value>::
_evict() {
    long long curtime =
        duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
    // Lock expiry queue
    pthread_mutex_lock(&expiry_q_lock);
    while (!_shutdown && !_expiry_queue.empty() && _expiry_queue.begin()->first < curtime) {
        long long remove_expiry = _expiry_queue.begin()->first;
        set<Key> remove_keys = _expiry_queue.begin()->second;
        _expiry_queue.erase(_expiry_queue.begin());
        // Unlock expiry queue
        pthread_mutex_unlock(&expiry_q_lock);
        // Write lock data table
        pthread_rwlock_wrlock(&data_tbl_lock);
        for (KeyIterator it = remove_keys.begin(); it != remove_keys.end(); ++it) {
            Key remove_key = *it;
            typename KVStore::iterator iter = _data_table.find(remove_key);
            if (iter != _data_table.end()) {
                // Expiry may not match if the removal of an entry is racing with an insertion of a
                // KV with the same key. Ignore removal in this case, overwrite would have removed
                // the value. Removal of the newly inserted value will be taken care of by the new
                // entry being added into the expiry queue.
                if (iter->second.expiry == remove_expiry) {
                    _data_table.erase(remove_key);
                }
            }
        }
        // unlock data table
        pthread_rwlock_unlock(&data_tbl_lock);
        // Lock expiry queue
        pthread_mutex_lock(&expiry_q_lock);
        curtime =
            duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
    }
    // Sleep for the minimum of either
    //  - 4 milliseconds or
    //  - if the queue has more elements, time to next element's expiry.
    // Sleeping is capped at 10 microseconds to evict entries that might be inserted in
    // the future and set to expire in the next 4 milliseconds.
    // Do not sleep if shutdown has been triggered.
    long long sleeptime = _shutdown ? 0 : 4000;
    if (!_shutdown && !_expiry_queue.empty()) {
        // Sleeptime is guaranteed to be positive here because of the condition in while loop
        // above.
        sleeptime = min(sleeptime, _expiry_queue.begin()->first - curtime + 1);
        assert(sleeptime > 0);
    }
    // Unlock expiry queue
    pthread_mutex_unlock(&expiry_q_lock);
    return sleeptime;
}

template <class Key, class Value>
void
ExpireMap<Key, Value>::
_byol_erase_from_expiry_queue(long long expiry, Key key) {
    // Verify that the expiry queue is locked
    assert(pthread_mutex_trylock(&expiry_q_lock) == EBUSY);
    ExpiryIterator iter = _expiry_queue.find(expiry);
    // Remove entry from expiry queue if it exists
    if (iter != _expiry_queue.end()) {
        iter->second.erase(key);
        if (iter->second.empty()) {
            // If the keyset is empty, remove from expiry queue
            _expiry_queue.erase(expiry);
        }
    }
}

template <class Key, class Value>
void*
ExpireMap<Key, Value>::
eviction(void* arg) {
    ExpireMap<Key, Value>* exp_map = (ExpireMap<Key, Value>*)arg;
    while(!exp_map->_shutdown) {
        long long sleeptime = exp_map->_evict();
        usleep(sleeptime);
    }
    pthread_exit(NULL /* ret */);
}

#endif // EXPIRE_MAP_HH
