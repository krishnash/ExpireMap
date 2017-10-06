                        ExpireMap
                        ---------
Summary
-------
ExpireMap is a map that stores key value pairs with an associated
timeout. KV pairs are discarded after the timeout and are no longer
accessible.

Class
-----
ExpireMap<Key, Value>

Usage:
Templatized class. Instantiate an object with appropriate template
parameters to use.

Example:
#include "expire_map.h"
ExpireMap<int, int> exp_map;

Destruction of object will block the caller till eviction exits (Details below).

Interface
---------

void put(K key, V value, long timeoutMs)
- Adds a the key value pair to the map. The KV pair is valid from the
  time of put call to timeoutMs milliseconds in the future.
- Time is tracked internally in microseconds.
- The value is valid till time current time + timeoutMs (Inclusive.
  End time is included).
- When a put is called for an existing key, the value and timeout are
  overwritten with the new input.
- Put calls with zero or negative timeout are ignored

V get(K key)
- Returns an unexpired value for the key if it exists in the map
- Returns NULL otherwise

void remove(Key key)
- Removes an entry from the ExpireMap

Implementation
--------------
ExpireMap primarily needs to track two aspects:
- The actual KV store
- Expiration and clean up of KV pairs

KV store:
The KV store is an std::unordered_map (Hash table) protected by a write
preferred read write lock. It maps
    Key -> (Value, Expiration time in microseconds)
This is referred to in code as _data_table.

Garbage collection:
The expiration of key value pairs are tracked using an std::map. This is an
ordered map sorted by expiration times. Keys are unique. This is
internally a tree structure and ideally suits this purpose. This maps
    Expiration time in microseconds -> std::set of keys.
The set of keys are all the unique keys that will expire at the time.
This is referred to in code as _expiry_queue.

On instantiation of an ExpireMap object, an eviction thread is
spawned. This thread walks the expiry queue till the current time is
greater than expiry time of the next object. For each expired entry in the
expiry queue, remove the entry from the expiry queue. Thread gets all keys
from the keyset and removes them from the data table. The removal from
data table is only done if the expiry in data table matches the expiry queue
entry being processed. This should typically be the case unless the eviction
is racing with a fresh insertion og hte same key. Once the thread has exhausted
all entries that can be evicted at the moment, it sleeps for a minimum of either
    - 4 milliseconds or
    - if the expiry queue is not empty, time to expiration of next
      object.
Then it wakes up and redoes the same operations as above trying to
evict as many KVs as possible from they data table. The 4 millisecond
wait is so that entries that are added to the table and are set to
expire before the expiration of the next existing KV do not have to
wait for more than 4 milliseconds to clear up.

Destruction of ExpireMap signals the eviction thread (using _shutdown flag) and
waits for the eviction thread to exit. So destruction of ExpireMap will block
calling thread. Eviction thread stops processing as soon as it sees the shutdown flag set.
Expire map stops serving Put and get requests as soon as it detects
shutdown (Initiation of destruction of object). Once eviction thread
has exit, all internal data structures are cleared and locks
destroyed.

Operation
---------
Put:
When a key is inserted into the ExpireMap, aside from the entry into
the data table, an entry is added into the expiry queue.
Remove or overwrite of a key removes the older entry from both the
data table and the expiry queue.

Get:
Look up data table. Compare current time to the expiry time in the
data table. Return the value if current time is less than or equal to
the expiry time for the KV pair.

Remove:
Remove the KV pair from both the data table and expiry queue
irrespective of expiry time.

Time Complexity
---------------
Insertion(put):
 - Constant time insertion into hash table.
 - Logarithmic time for insertion into/deletion from the expiry queue.
 - Set insertion/deletion is logarithmic in general and amortized constant.
   This size of each set is limited by the fact that time is tracked in
   microsecond granularity.
Overall complexity will be dictated by the expiry queue which is O(log n)
where n is the number of entries in expiry queue (~Number of valid KV
pairs).

Read (get):
  - Constant time lookup from the hash table.
  - Constant time comparision of expiry to current time.
Overall complexity will be O(1).

Removal(remove):
  - Remove from hash table in constant time.
  - Remove from map if applicable in logarithmic time but
    amortized constant (std::map)
  - Keyset is cleared in constant time per key.
Overall complexity will be dictated by expiry queue removal which will
be O(log n) but amortized contant where n is the number of entries in
expiry queue (~Number of valid KV pairs).

Eviction:
    - Reading next entry to be evicted is constant time.
    - Removal of beginning of the expiry queue(std::map) is logarithmic but
      amortized constant time.
    - Removal from data table is contant time.
Eviction of each entry will be performed in constant time O(log n) but
amortized constant(1) where n is number of entries in expiry_queue
(~Number of valid KV pairs)

Space Complexity
----------------
At most there will be one entry in the data table and one in the
expiry queue. So space complexity is O(n) where n is the number of
KV pairs.

Unit tests
----------
General approach I have taken for unit testing is to do randomized
operations on the ExpireMap with a shadow copy maintained in the unit
test itself. Each operation is followed by verification of data in
ExpireMap with the expected results as per ShadowMap. There are two
tests:
    - Single threaded test: This verifies the
      insert/delete/update/expiration functionality of the ExpireMap
      with a single threaded user. Each operation is performed based
      on some probability and the ExpireMap is queried to verify results.
      Number of unique keys, total number of operations and maximum
      timeout for each key can be passed as arguments to this test.
      Timeout for each inserted key is chosen at random between 1 and
      maximum timeout specified.
    - Multi threaded test: Similar test as above. But many threads
      perform all the operations. Number of threads is accepted as an
      argument apart from the ones accepted in single threaded test.
    - Expiration test: Add a few entries into the table. Wait for all
      items to expire. Verify that the size is 0.

Uncomment the following line to see more details printed as the test
runs.
// #define VERBOSE 1

Tests will take a few seconds to run. No information may be printed in
the console in this time without VERBOSE flag set.

Compilation:
make (From the ExpireMap directory)

Execution:
./test_expire_map
