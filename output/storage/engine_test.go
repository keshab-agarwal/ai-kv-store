package storage

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
)

func TestEnginePutGet(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	key := []byte("testKey")
	value := []byte("testValue")

	err := engine.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrievedValue, err := engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(value, retrievedValue) {
		t.Errorf("Get returned wrong value. Expected %s, got %s", value, retrievedValue)
	}

	// Test updating a key
	newValue := []byte("newTestValue")
	err = engine.Put(key, newValue)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrievedValue, err = engine.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(newValue, retrievedValue) {
		t.Errorf("Get returned wrong value. Expected %s, got %s", newValue, retrievedValue)
	}
}

func TestEngineConcurrentAccess(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	numKeys := 1000

	// Insert keys
	for i := 0; i < numKeys; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))
		err := engine.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed for key %s: %v", key, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numKeys)

	// Concurrently get keys
	for i := 0; i < numKeys; i++ {
		go func(keyID int) {
			defer wg.Done()
			key := []byte("key_" + strconv.Itoa(keyID))
			_, err := engine.Get(key)
			if err != nil {
				t.Errorf("Concurrent Get failed for key %s: %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	wg.Add(numKeys)

	// Concurrently delete keys
	for i := 0; i < numKeys; i++ {
		go func(keyID int) {
			defer wg.Done()
			key := []byte("key_" + strconv.Itoa(keyID))
			err := engine.Delete(key)
			if err != nil {
				t.Errorf("Concurrent Delete failed for key %s: %v", key, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys are deleted
	for i := 0; i < numKeys; i++ {
		key := []byte("key_" + strconv.Itoa(i))
		_, err := engine.Get(key)
		if err == nil {
			t.Errorf("Key %s should have been deleted but was found", key)
		}
	}
}
