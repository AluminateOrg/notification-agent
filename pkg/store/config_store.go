package store

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

type ConfigStore struct {
	db *sql.DB
}

func NewConfigStore(path string) (*ConfigStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS fallback_notifications (id TEXT PRIMARY KEY, org_id TEXT, channel TEXT, payload TEXT, attempts INTEGER DEFAULT 0, next_try_ts INTEGER);`)
	if err != nil {
		_ = db.Close()
		return nil, err 
	}

	return &ConfigStore{db: db}, nil
}

func (s *ConfigStore) Get(key string) (string, bool) {
	var v string
	err := s.db.QueryRow("SELECT value FROM config WHERE key = ?", key).Scan(&v)
	if err != nil {
		return "", false
	}
	return v, true
}

func (s *ConfigStore) Set(key, value string) error {
	_, err := s.db.Exec("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", key, value)
	return err
}

func (s *ConfigStore) Close() error {
	return s.db.Close()
}

func (s *ConfigStore) SaveFallback(id, orgId, channel, payload string, attempt int, nextTry int64) error {
	_, err := s.db.Exec("INSERT OR REPLACE INTO fallback_notifications (id, org_id, channel, payload, attempts, next_try_ts) VALUES (?,?,?,?,?,?)", 
		id, orgId, channel, payload, attempt, nextTry)
	return err
}