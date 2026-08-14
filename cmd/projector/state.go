package main

import (
	"database/sql"
	"fmt"
	"time"
)

// stateStore owns projector_state.db — cursors, watermarks, and the config snapshot.
//
// This is a SEPARATE FILE from the replica on purpose. The edge agent auto-enrols every
// non-system table it finds in the database it is pointed at, and silently defaults unlisted
// tables to the `rows` strategy, which installs triggers. Bookkeeping tables sitting in the
// replica would therefore get triggers installed and replicate themselves to the cloud.
type stateStore struct {
	db *sql.DB

	// Watermarks are read on nearly every row write, so they are cached. The database
	// remains the durable record.
	marks map[string]string
}

func openState(path string) (*stateStore, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(stateSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init state schema: %w", err)
	}

	s := &stateStore{db: db, marks: map[string]string{}}
	if err := s.loadWatermarks(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *stateStore) Close() error { return s.db.Close() }

func (s *stateStore) loadWatermarks() error {
	rows, err := s.db.Query(`SELECT table_name, last_event_ts FROM emit_watermark`)
	if err != nil {
		return fmt.Errorf("load watermarks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t, ts string
		if err := rows.Scan(&t, &ts); err != nil {
			return err
		}
		s.marks[t] = ts
	}
	return rows.Err()
}

func (s *stateStore) Watermark(table string) (string, error) {
	return s.marks[table], nil
}

func (s *stateStore) SetWatermark(table, ts string) {
	if cur, ok := s.marks[table]; ok && ts <= cur {
		return
	}
	s.marks[table] = ts
	if _, err := s.db.Exec(
		`INSERT INTO emit_watermark (table_name, last_event_ts, last_id) VALUES (?,?,0)
		 ON CONFLICT(table_name) DO UPDATE SET last_event_ts = excluded.last_event_ts`,
		table, ts); err != nil {
		logf("persist watermark %s: %v", table, err)
	}
}

// Cursor returns the last consumed source id and close-time watermark for a source table.
func (s *stateStore) Cursor(table string) (lastID int64, lastClosedMS int64, err error) {
	var closed sql.NullInt64
	err = s.db.QueryRow(
		`SELECT last_src_id, last_closed_ms FROM source_cursors WHERE source_table = ?`,
		table).Scan(&lastID, &closed)
	if err == sql.ErrNoRows {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read cursor %s: %w", table, err)
	}
	if closed.Valid {
		lastClosedMS = closed.Int64
	}
	return lastID, lastClosedMS, nil
}

func (s *stateStore) SetCursor(table string, lastID, lastClosedMS int64) error {
	_, err := s.db.Exec(
		`INSERT INTO source_cursors (source_table, last_src_id, last_closed_ms, updated_at_ms)
		 VALUES (?,?,?,?)
		 ON CONFLICT(source_table) DO UPDATE SET
		     last_src_id    = excluded.last_src_id,
		     last_closed_ms = excluded.last_closed_ms,
		     updated_at_ms  = excluded.updated_at_ms`,
		table, lastID, lastClosedMS, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("write cursor %s: %w", table, err)
	}
	return nil
}

// ConfigSnapshot returns the last observed config values. The source config table is a
// mutable key/value store with no timestamps whatsoever, so change detection is a diff
// against this snapshot rather than a cursor.
func (s *stateStore) ConfigSnapshot() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT config_key, COALESCE(value,'') FROM config_snapshot`)
	if err != nil {
		return nil, fmt.Errorf("read config snapshot: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *stateStore) SetConfigSnapshot(m map[string]string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for k, v := range m {
		if _, err := tx.Exec(
			`INSERT INTO config_snapshot (config_key, value) VALUES (?,?)
			 ON CONFLICT(config_key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return fmt.Errorf("write config snapshot: %w", err)
		}
	}
	return tx.Commit()
}

// LastAlive is when the projector last completed a tick, so the next start can detect and
// record the stretch it was absent for.
//
// Deliberately NOT derived from source_cursors: those only advance when new rows arrive, so
// a projector that is running fine but has nothing to read would look absent and the next
// start would invent a gap that never happened.
func (s *stateStore) LastAlive() (int64, error) {
	var ms int64
	err := s.db.QueryRow(
		`SELECT value FROM projector_meta WHERE key = 'last_alive_ms'`).Scan(&ms)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read last alive: %w", err)
	}
	return ms, nil
}

// MarkAlive is called on every tick, whether or not it found anything.
func (s *stateStore) MarkAlive(ms int64) {
	if _, err := s.db.Exec(
		`INSERT INTO projector_meta (key, value) VALUES ('last_alive_ms', ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, ms); err != nil {
		logf("persist liveness: %v", err)
	}
}
