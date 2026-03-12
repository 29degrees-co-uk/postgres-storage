package certmagicsql

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
	_ "github.com/lib/pq"
)

func setup(t *testing.T) *PostgresStorage {
	return setupWithTimeout(t, time.Minute)
}

func setupWithTimeout(t *testing.T, lockTimeout time.Duration) *PostgresStorage {
	connStr := os.Getenv("CONN_STR")
	if connStr == "" {
		t.Skipf("must set CONN_STR")
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	s := &PostgresStorage{
		Database:     db,
		QueryTimeout: 3 * time.Second,
		LockTimeout:  lockTimeout,
	}
	if err := s.ensureTableSetup(); err != nil {
		t.Fatal(err)
	}
	return s
}

func dropTable() {
	connStr := os.Getenv("CONN_STR")
	if connStr == "" {
		log.Println("must set CONN_STR")
		return
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("drop table if exists certmagic_data")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("drop table if exists certmagic_locks")
	if err != nil {
		log.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	dropTable()
	os.Exit(m.Run())
}

func TestExists(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testkey"
	defer storage.Delete(ctx, key)
	exists := storage.Exists(ctx, key)
	if exists {
		t.Fatalf("key should not exist")
	}
	err := storage.Store(ctx, key, []byte("testvalue"))
	if err != nil {
		t.Fatal(err)
	}
	exists = storage.Exists(ctx, key)
	if !exists {
		t.Fatalf("key should exist")
	}
}

func TestStoreUpdatesModified(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testkey"
	defer storage.Delete(ctx, key)

	err := storage.Store(ctx, key, []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	infoBefore, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.Store(ctx, key, []byte("00"))
	if err != nil {
		t.Fatal(err)
	}
	infoAfter, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !infoBefore.Modified.Before(infoAfter.Modified) {
		t.Fatalf("modified not updated")
	}
	if int64(2) != infoAfter.Size {
		t.Fatalf("size not updated")
	}
}

func TestStoreExistsLoadDelete(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testkey"
	val := []byte("testval")
	defer storage.Delete(ctx, key)

	if storage.Exists(ctx, key) {
		t.Fatalf("key should not exist")
	}

	err := storage.Store(ctx, key, val)
	if err != nil {
		t.Fatal(err)
	}

	if !storage.Exists(ctx, key) {
		t.Fatalf("key should exist")
	}

	load, err := storage.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, load) {
		t.Fatalf("got: %s", load)
	}

	err = storage.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	load, err = storage.Load(ctx, key)
	if load != nil {
		t.Fatalf("load should be nil")
	}
	if _, ok := err.(certmagic.ErrNotExist); !ok {
		t.Fatalf("err not certmagic.ErrNotExist")
	}
}

func TestStat(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testkey"
	val := []byte("testval")
	defer storage.Delete(ctx, key)
	if err := storage.Store(ctx, key, val); err != nil {
		t.Fatal(err)
	}
	stat, err := storage.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Modified.IsZero() {
		t.Fatalf("modified should not be zero")
	}
	if !reflect.DeepEqual(stat, certmagic.KeyInfo{
		Key:        key,
		Modified:   stat.Modified,
		Size:       int64(len(val)),
		IsTerminal: true,
	}) {
		t.Fatalf("got: %v", stat)
	}
}

func TestList(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	keys := []string{
		"testnohit1",
		"testnohit2",
		"testhit1",
		"testhit2",
		"testhit3",
	}
	for _, key := range keys {
		if err := storage.Store(ctx, key, []byte("hit")); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, key := range keys {
			if err := storage.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
	}()
	list, err := storage.List(ctx, "testhit", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("got: %d", len(list))
	}
	sort.Strings(list)
	if !reflect.DeepEqual([]string{"testhit1", "testhit2", "testhit3"}, list) {
		t.Fatalf("got: %v", list)
	}
}

func TestLockAndUnlock(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testlock"
	defer storage.Unlock(ctx, key)

	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Verify the lock row exists in the database
	var exists bool
	row := storage.Database.QueryRow(`SELECT EXISTS(SELECT 1 FROM certmagic_locks WHERE key = $1 AND expires > CURRENT_TIMESTAMP)`, key)
	if err := row.Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("lock row should exist after Lock()")
	}

	if err := storage.Unlock(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Verify the lock row is gone
	row = storage.Database.QueryRow(`SELECT EXISTS(SELECT 1 FROM certmagic_locks WHERE key = $1)`, key)
	if err := row.Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("lock row should not exist after Unlock()")
	}
}

func TestLockBlocksUntilUnlocked(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testblockinglock"
	defer storage.Unlock(ctx, key)

	// Instance A acquires the lock
	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Instance B tries to acquire — should block
	acquired := make(chan error, 1)
	go func() {
		acquired <- storage.Lock(ctx, key)
	}()

	// Give instance B time to attempt and fail at least once
	time.Sleep(500 * time.Millisecond)

	select {
	case <-acquired:
		t.Fatal("Lock() should have blocked while lock is held")
	default:
		// Good — still blocking
	}

	// Instance A releases the lock
	if err := storage.Unlock(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Instance B should now acquire the lock
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("Lock() should have succeeded after Unlock(), got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Lock() did not unblock after Unlock()")
	}

	// Clean up instance B's lock
	storage.Unlock(ctx, key)
}

func TestLockClaimsExpiredLock(t *testing.T) {
	// Use a very short lock timeout so the lock expires quickly.
	// The refresh goroutine ticks every 5s, so a 200ms lock will expire
	// before the first refresh.
	storage := setupWithTimeout(t, 200*time.Millisecond)
	ctx := context.Background()
	key := "testexpiredlock"
	defer storage.Unlock(ctx, key)

	// Acquire the lock, then immediately stop the refresh goroutine
	// (simulating a crashed instance that can't refresh)
	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}
	storage.locksMu.Lock()
	if cancel, ok := storage.locks[key]; ok {
		cancel()
		delete(storage.locks, key)
	}
	storage.locksMu.Unlock()

	// Wait for the lock to expire
	time.Sleep(300 * time.Millisecond)

	// Another caller should be able to claim the stale lock
	lockCtx, lockCancel := context.WithTimeout(ctx, 3*time.Second)
	defer lockCancel()
	if err := storage.Lock(lockCtx, key); err != nil {
		t.Fatalf("should have claimed expired lock, got: %v", err)
	}

	storage.Unlock(ctx, key)
}

func TestLockRespectsContextCancellation(t *testing.T) {
	storage := setup(t)
	ctx := context.Background()
	key := "testcancellock"
	defer storage.Unlock(ctx, key)

	// Acquire the lock
	if err := storage.Lock(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Try to lock with a short-lived context — should fail with context error
	lockCtx, lockCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer lockCancel()

	err := storage.Lock(lockCtx, key)
	if err == nil {
		t.Fatal("Lock() should have returned an error when context was canceled")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	storage.Unlock(ctx, key)
}
