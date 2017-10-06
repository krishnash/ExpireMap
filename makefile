CC=g++
CFLAGS=-pthread
test_expire_map: src/test_expire_map.cpp src/expire_map.h src/expire_map.hh
	$(CC) src/test_expire_map.cpp -o test_expire_map $(CFLAGS)
