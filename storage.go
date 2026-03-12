package certmagicsql

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

var (
	_ certmagic.Storage = (*PostgresStorage)(nil)
)

const (
	lockPollInterval    = 1 * time.Second
	lockRefreshInterval = 5 * time.Second
)

type PostgresStorage struct {
	logger *zap.Logger

	QueryTimeout     time.Duration `json:"query_timeout,omitempty"`
	LockTimeout      time.Duration `json:"lock_timeout,omitempty"`
	Database         *sql.DB       `json:"-"`
	Host             string        `json:"host,omitempty"`
	Port             string        `json:"port,omitempty"`
	User             string        `json:"user,omitempty"`
	Password         string        `json:"password,omitempty"`
	DBname           string        `json:"dbname,omitempty"`
	SSLmode          string        `json:"sslmode,omitempty"` // Valid values for sslmode are: disable, require, verify-ca, verify-full
	ConnectionString string        `json:"connection_string,omitempty"`
	DisableDDL       bool          `json:"disable_ddl,omitempty"`

	locks   map[string]context.CancelFunc // active lock keepalives, keyed by lock key
	locksMu sync.Mutex
}

func init() {
	caddy.RegisterModule(PostgresStorage{})
}

func (c *PostgresStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "host":
			c.Host = value
		case "port":
			c.Port = value
		case "user":
			c.User = value
		case "password":
			c.Password = value
		case "dbname":
			c.DBname = value
		case "sslmode":
			c.SSLmode = value
		case "connection_string":
			c.ConnectionString = value
		case "disable_ddl":
			var err error
			c.DisableDDL, err = strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf(`parsing disable_ddl value %+v: %w`, value, err)
			}
		}
	}

	return nil
}

func (c *PostgresStorage) Provision(ctx caddy.Context) error {
	c.logger = ctx.Logger(c)

	// Load Environment
	if c.Host == "" {
		c.Host = os.Getenv("POSTGRES_HOST")
	}
	if c.Port == "" {
		c.Port = os.Getenv("POSTGRES_PORT")
	}
	if c.User == "" {
		c.User = os.Getenv("POSTGRES_USER")
	}
	if c.Password == "" {
		c.Password = os.Getenv("POSTGRES_PASSWORD")
	}
	if !c.DisableDDL {
		disableDDLString := os.Getenv("POSTGRES_DISABLE_DDL")
		if disableDDLString != "" {
			var err error
			c.DisableDDL, err = strconv.ParseBool(disableDDLString)
			if err != nil {
				return fmt.Errorf(`parsing POSTGRES_DISABLE_DDL value %+v: %w`, disableDDLString, err)
			}
		}
	}
	if c.DBname == "" {
		c.DBname = os.Getenv("POSTGRES_DBNAME")
	}
	if c.SSLmode == "" {
		c.SSLmode = os.Getenv("POSTGRES_SSLMODE")
	}
	if c.ConnectionString == "" {
		c.ConnectionString = os.Getenv("POSTGRES_CONN_STRING")
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = time.Second * 3
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = time.Minute
	}

	return nil
}

func (PostgresStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.postgres",
		New: func() caddy.Module {
			return new(PostgresStorage)
		},
	}
}

func NewStorage(c PostgresStorage) (certmagic.Storage, error) {
	var connStr string
	if len(c.ConnectionString) > 0 {
		connStr = c.ConnectionString
	} else {
		connStr_fmt := "host=%s port=%s user=%s password=%s dbname=%s sslmode=%s"
		// Set each value dynamically w/ Sprintf
		connStr = fmt.Sprintf(connStr_fmt, c.Host, c.Port, c.User, c.Password, c.DBname, c.SSLmode)
	}

	database, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	s := &PostgresStorage{
		Database:     database,
		QueryTimeout: c.QueryTimeout,
		LockTimeout:  c.LockTimeout,
		DisableDDL:   c.DisableDDL,
	}
	return s, s.ensureTableSetup()
}

func (c *PostgresStorage) CertMagicStorage() (certmagic.Storage, error) {
	return NewStorage(*c)
}

// DB represents the required database API. You can use a *database/sql.DB.
type DB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// Database RDBs this library supports, currently supports Postgres only.
type Database int

const (
	Postgres Database = iota
)

func (s *PostgresStorage) ensureTableSetup() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.QueryTimeout)
	defer cancel()
	if s.DisableDDL {
		// Only check if tables exist with expected columns; do not attempt to create them
		tx, err := s.Database.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `select key, value, modified from certmagic_data limit 0`)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `select key, expires from certmagic_locks limit 0`)
		if err != nil {
			return err
		}
		return nil
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `create table if not exists certmagic_data (
key text primary key,
value bytea,
modified timestamptz default current_timestamp
)`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `create table if not exists certmagic_locks (
key text primary key,
expires timestamptz default current_timestamp
)`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Lock acquires a distributed lock for the given key, blocking until the lock
// is available or the context is canceled. If the existing lock has expired
// (stale), it is claimed. A background goroutine keeps the lock fresh while
// held. This implements certmagic.Storage.Lock.
func (s *PostgresStorage) Lock(ctx context.Context, key string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		acquired, err := s.tryAcquireLock(ctx, key)
		if err != nil {
			return err
		}
		if acquired {
			s.startLockRefresh(key)
			return nil
		}

		// Lock is held by another instance — wait and retry
		select {
		case <-time.After(lockPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryAcquireLock attempts to insert or claim the lock row. Returns true if the
// lock was acquired. Stale locks (where expires <= now) are claimed atomically.
func (s *PostgresStorage) tryAcquireLock(ctx context.Context, key string) (bool, error) {
	qctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()

	expires := time.Now().Add(s.LockTimeout)

	// Attempt to insert a new lock row, or update an existing one only if it
	// has expired. If a non-expired lock exists, the WHERE clause prevents the
	// update and no row is affected.
	result, err := s.Database.ExecContext(qctx, `
		INSERT INTO certmagic_locks (key, expires)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET expires = $2
		WHERE certmagic_locks.expires <= CURRENT_TIMESTAMP
	`, key, expires)
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock %s: %w", key, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// startLockRefresh launches a background goroutine that periodically extends
// the lock's expiry so long-running operations don't have their lock stolen.
func (s *PostgresStorage) startLockRefresh(key string) {
	ctx, cancel := context.WithCancel(context.Background())

	s.locksMu.Lock()
	if s.locks == nil {
		s.locks = make(map[string]context.CancelFunc)
	}
	s.locks[key] = cancel
	s.locksMu.Unlock()

	go func() {
		ticker := time.NewTicker(lockRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				qctx, qcancel := context.WithTimeout(ctx, s.QueryTimeout)
				expires := time.Now().Add(s.LockTimeout)
				_, err := s.Database.ExecContext(qctx, `UPDATE certmagic_locks SET expires = $2 WHERE key = $1`, key, expires)
				qcancel()
				if err != nil && s.logger != nil {
					s.logger.Error("failed to refresh lock", zap.String("key", key), zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Unlock releases the distributed lock for the given key and stops its
// background refresh goroutine. Implements certmagic.Storage.Unlock.
func (s *PostgresStorage) Unlock(ctx context.Context, key string) error {
	s.locksMu.Lock()
	if cancel, ok := s.locks[key]; ok {
		cancel()
		delete(s.locks, key)
	}
	s.locksMu.Unlock()

	qctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	_, err := s.Database.ExecContext(qctx, `DELETE FROM certmagic_locks WHERE key = $1`, key)
	return err
}

// Store puts value at key.
func (s *PostgresStorage) Store(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	_, err := s.Database.ExecContext(ctx, "insert into certmagic_data (key, value) values ($1, $2) on conflict (key) do update set value = $2, modified = current_timestamp", key, value)
	return err
}

// Load retrieves the value at key.
func (s *PostgresStorage) Load(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	var value []byte
	err := s.Database.QueryRowContext(ctx, "select value from certmagic_data where key = $1", key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, fs.ErrNotExist
	}
	return value, err
}

// Delete deletes key. An error should be
// returned only if the key still exists
// when the method returns.
func (s *PostgresStorage) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	_, err := s.Database.ExecContext(ctx, "delete from certmagic_data where key = $1", key)
	return err
}

// Exists returns true if the key exists
// and there was no error checking.
func (s *PostgresStorage) Exists(ctx context.Context, key string) bool {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	row := s.Database.QueryRowContext(ctx, "select exists(select 1 from certmagic_data where key = $1)", key)
	var exists bool
	err := row.Scan(&exists)
	return err == nil && exists
}

// List returns all keys that match prefix.
// If recursive is true, non-terminal keys
// will be enumerated (i.e. "directories"
// should be walked); otherwise, only keys
// prefixed exactly by prefix will be listed.
func (s *PostgresStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	if recursive {
		return nil, fmt.Errorf("recursive not supported")
	}
	rows, err := s.Database.QueryContext(ctx, `select key from certmagic_data where key like $1`, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Stat returns information about key.
func (s *PostgresStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout)
	defer cancel()
	var modified time.Time
	var size int64
	row := s.Database.QueryRowContext(ctx, "select length(value), modified from certmagic_data where key = $1", key)
	err := row.Scan(&size, &modified)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        key,
		Modified:   modified,
		Size:       size,
		IsTerminal: true,
	}, nil
}
