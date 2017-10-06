#include <iostream>
#include <vector>
using namespace std;
#include <unistd.h>
#include "expire_map.h"

// #define VERBOSE 1

typedef struct ShadowVal {
    int value;
    long expiry;
    ShadowVal() : value(0), expiry(0) { }
} ShadowVal;

void single_threaded_test(int num_keys, int num_ops, long max_timeout_ms) {
    cout << "====Test of correctness of ExpireMap with a single threaded user====" << endl;
    vector<ShadowVal> shadowlist(num_keys);
    ExpireMap<int, int> exp_map;
    int inserted = 0;
    int deleted = 0;
    int expired = 0;
    int overwritten = 0;
    for (int i = 0; i < num_ops; ++i) {
        int idx = rand() % num_keys;
#ifdef VERBOSE
        cout << "Iteration : "<< i << " Key : " << idx<< endl;
#endif
        long long curtime =
            duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
        // If expired, mark shadow entry as deleted
        if (shadowlist[idx].expiry != 0 && shadowlist[idx].expiry < curtime) {
            ++expired;
#ifdef VERBOSE
            cout << "\tEntry expired" << endl;
#endif
            shadowlist[idx].expiry = 0;
            // Verify that value is not valid in expire map
            assert(exp_map.get(idx) == 0);
        }
        if (shadowlist[idx].expiry == 0) {
            ++inserted;
#ifdef VERBOSE
            cout << "\tEntry inserted" << endl;
#endif
            // Entry not in map, insert it into map with random value and 
            // random expiry time between 1 and max_timeout_ms
            int timeout_ms = (rand() % max_timeout_ms) + 1;
            // Set a random value
            shadowlist[idx].value = rand();
            // Retake time to calculate expiry
            curtime =
                duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
            exp_map.put(idx, shadowlist[idx].value, timeout_ms);
            shadowlist[idx].expiry = curtime + timeout_ms * 1000;
            // Verify that value is present in the map
            assert(exp_map.get(idx) == shadowlist[idx].value);
        } else {
#ifdef VERBOSE
            cout << "\tEntry verified" << endl;
#endif
            // Verify that value is present in map
            assert(exp_map.get(idx) == shadowlist[idx].value);
            // Delete entry with 25% probability
            if (rand() % 4 == 0) {
                ++deleted;
#ifdef VERBOSE
                cout << "\tEntry deleted" << endl;
#endif
                shadowlist[idx].expiry = 0;
                exp_map.remove(idx);
                // Verify that value is not present in map
                assert(exp_map.get(idx) == 0);
            } else {
                // If not deleting, overwrite entry with 50% probability
                if (rand() % 2 == 0) {
                    ++overwritten;
#ifdef VERBOSE
                    cout << "\tEntry overwritten" << endl;
#endif
                    int timeout_ms = (rand() % max_timeout_ms) + 1;
                    // Set a random value
                    shadowlist[idx].value = rand();
                    // Retake time to calculate expiry
                    curtime =
                        duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
                    exp_map.put(idx, shadowlist[idx].value, timeout_ms);
                    shadowlist[idx].expiry = curtime + timeout_ms * 1000;
                    // Verify that value is present in the map
                    assert(exp_map.get(idx) == shadowlist[idx].value);
                }
            }
        }
        // Sleep for 4ms
        int sleeptime = (rand() % 4000);
        usleep(sleeptime);
    }
    cout << "Entries inserted : " << inserted << ", expired : " << expired;
    cout << ", overwritten : " << overwritten << ", deleted : " << deleted << endl;
    cout << "====Test successful====" << endl;
}

typedef struct ConcurrentShadowVal {
    int value;
    long expiry;
    pthread_mutex_t mutex;
    ConcurrentShadowVal() : value(0), expiry(0) { }
} ConcurrentShadowVal;

typedef struct ThreadInput {
    ExpireMap<int, int>* exp_map;
    int num_keys;
    int num_ops_per_thread;
    int max_timeout_ms;
    vector<ConcurrentShadowVal>* shadowlist;
    ThreadInput(ExpireMap<int, int>* p_m, int p_k, int p_o, int p_t,
                vector<ConcurrentShadowVal>* p_l)
        : exp_map(p_m), num_keys(p_k), num_ops_per_thread(p_o), max_timeout_ms(p_t),
          shadowlist(p_l) { }
} ThreadInput;

typedef struct ThreadOutput {
    int inserted;
    int expired;
    int deleted;
    int overwritten;
    ThreadOutput() : inserted(0), expired(0), deleted(0), overwritten(0) { }
} ThreadOutput;

void* test_thread(void* arg) {
    ThreadInput* params = (ThreadInput*) arg;
    ThreadOutput* counts = new ThreadOutput();
    for (int i = 0; i < params->num_ops_per_thread; ++i) {
        // Pick an index to work on and lock the index.
        // Expected value will be non deterministic if the index is not locked.
        int idx = rand() % params->num_keys;
        ConcurrentShadowVal* sd_val = &((*params->shadowlist)[idx]);
        pthread_mutex_lock(&(sd_val->mutex));
        long long curtime =
            duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
        if (sd_val->expiry != 0 && sd_val->expiry < curtime) {
            ++counts->expired;
            sd_val->expiry = 0;
            // Verify that value is not valid in expire map
            assert(params->exp_map->get(idx) == 0);
        }
        if (sd_val->expiry == 0) {
            ++counts->inserted;
            // Entry not in map, insert it into map with random value and 
            // random expiry time between 1 and max_timeout_ms
            int timeout_ms = (rand() % params->max_timeout_ms) + 1;
            // Set a random value
            sd_val->value = rand();
            // Retake time to calculate expiry
            curtime =
                duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
            params->exp_map->put(idx, sd_val->value, timeout_ms);
            sd_val->expiry = curtime + timeout_ms * 1000;
            // Verify that value is present in the map
            assert(params->exp_map->get(idx) == sd_val->value);
        } else {
            // Verify that value is present in map
            assert(params->exp_map->get(idx) == sd_val->value);
            // Delete entry with 25% probability
            if (rand() % 4 == 0) {
                ++counts->deleted;
                sd_val->expiry = 0;
                params->exp_map->remove(idx);
                // Verify that value is not present in map
                assert(params->exp_map->get(idx) == 0);
            } else {
                // If not deleting, overwrite entry with 50% probability
                if (rand() % 2 == 0) {
                    ++counts->overwritten;
                    int timeout_ms = (rand() % params->max_timeout_ms) + 1;
                    // Set a random value
                    sd_val->value = rand();
                    // Retake time to calculate expiry
                    curtime =
                        duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
                    params->exp_map->put(idx, sd_val->value, timeout_ms);
                    sd_val->expiry = curtime + timeout_ms * 1000;
                    // Verify that value is present in the map
                    assert(params->exp_map->get(idx) == sd_val->value);
                }
            }
        }
        pthread_mutex_unlock(&(sd_val->mutex));
        // Sleep for 4ms
        int sleeptime = (rand() % 4000);
        usleep(sleeptime);
    }

    pthread_exit((void*) counts);
}

void multi_threaded_test(int num_threads, int num_keys, int num_ops_per_thread,
                         long max_timeout_ms) {
    cout << "====Test of correctness of ExpireMap with a multi threaded user====" << endl;
    vector<ConcurrentShadowVal> shadowlist(num_keys);
    for (int i = 0; i < num_keys; ++i) {
        int ret = pthread_mutex_init(&shadowlist[i].mutex, NULL /* attr */);
        assert(ret == 0);
    }
    ExpireMap<int, int> exp_map;
    vector<pthread_t> threads(num_threads);
    ThreadInput thread_input(&exp_map, num_keys, num_ops_per_thread, max_timeout_ms,
                             &shadowlist);
    // Spawn threads to operate on ExpireMap
    for (int i = 0; i < num_threads; ++i) {
        int ret =
            pthread_create(&threads[i], NULL /* attr */, test_thread, (void*)&thread_input);
        assert(ret == 0);
    }
    // Wait for threads to complete
    for (int i = 0; i < num_threads; ++i) {
        ThreadOutput* thread_output;
        int ret = pthread_join(threads[i], (void**)&thread_output);
        cout << "Thread " << i << " - Entries inserted : " << thread_output->inserted;
        cout << ", expired : " << thread_output->expired;
        cout << ", overwritten : " << thread_output->overwritten;
        cout << ", deleted : " << thread_output->deleted << endl;
        delete thread_output;
    }
    for (int i = 0; i < num_keys; ++i) {
        int ret = pthread_mutex_destroy(&shadowlist[i].mutex);
        assert(ret == 0);
    }
    cout << "====Test successful====" << endl;
}

void expiration_test(int num_keys, int max_timeout_ms) {
    cout << "====Test of expiration functionality====" << endl;
    ExpireMap<int, int> exp_map;
    for (int i = 0; i < num_keys; ++i) {
        int val = rand();
        exp_map.put(i, val, (rand() % max_timeout_ms) + 1);
        // Verify that the entry is present in the ExpireMap
        assert(exp_map.get(i) == val);
    }
    // Sleep for maximum timeout + 4ms to allow for eviction to catch up
    usleep(max_timeout_ms * 1000 + 4000);
    assert(exp_map.debug_size() == 0);
    cout << num_keys << " entries added and expired within " << max_timeout_ms << " ms" << endl;
    cout << "====Test successful====" << endl;
}

int main(int argc, char *argv[]) {
    single_threaded_test(512 /* num uniq keys */, 10000 /* num ops */, 1024 /* max timeout */);
    multi_threaded_test(16 /* num threads */, 1024 /* num uni keys */, 10000 /* num ops */,
                        1024 /* max timeout */);
    expiration_test(1 << 18 /* Num keys */, 128 /* max timeout */);
    return 0;
}
