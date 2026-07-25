// Package sqlite is the default durable EventStore. It uses modernc.org/sqlite
// so the collector builds with CGO_ENABLED=0.
package sqlite

import (
	"context"
	"database/sql"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
  trace_id      BLOB    NOT NULL,
  emitter_id    BLOB    NOT NULL,
  sequence      INTEGER NOT NULL,
  phase         INTEGER NOT NULL,
  hlc_wall_ms   INTEGER NOT NULL,
  hlc_logical   INTEGER NOT NULL,
  service       TEXT    NOT NULL,
  status        INTEGER NOT NULL,
  body          BLOB    NOT NULL,
  PRIMARY KEY (emitter_id, sequence, phase)
);
CREATE INDEX IF NOT EXISTS events_by_time  ON events (hlc_wall_ms, hlc_logical, emitter_id);
CREATE INDEX IF NOT EXISTS events_by_trace ON events (trace_id);

CREATE TABLE IF NOT EXISTS gaps (
  emitter_id    BLOB    NOT NULL,
  from_wall_ms  INTEGER NOT NULL,
  to_wall_ms    INTEGER NOT NULL,
  body          BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS gaps_by_time ON gaps (from_wall_ms, to_wall_ms);
`

// Store is a SQLite-backed EventStore.
type Store struct{ db *sql.DB }

// Open opens or creates the database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// modernc.org/sqlite serializes access at the driver level; a single
	// connection avoids "database is locked" errors under concurrent use
	// from this process without disabling SQLite's own locking.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) AppendEvents(ctx context.Context, events []*v1.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO events
	  (trace_id, emitter_id, sequence, phase, hlc_wall_ms, hlc_logical, service, status, body)
	  VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		body, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx,
			e.TraceId, e.GetId().GetEmitterId(), e.GetId().GetSequence(), int(e.Phase),
			e.GetHlc().GetWallMs(), e.GetHlc().GetLogical(),
			e.GetService().GetName(), int(e.Status), body,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AppendGaps(ctx context.Context, gaps []*v1.Gap) error {
	for _, g := range gaps {
		body, err := proto.Marshal(g)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO gaps (emitter_id, from_wall_ms, to_wall_ms, body) VALUES (?,?,?,?)`,
			g.EmitterId, g.GetFrom().GetWallMs(), g.GetTo().GetWallMs(), body,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) TraceEvents(ctx context.Context, traceID []byte) ([]*v1.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM events WHERE trace_id = ?
		 ORDER BY hlc_wall_ms, hlc_logical, emitter_id`, traceID)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

func (s *Store) Window(ctx context.Context, q store.WindowQuery) ([]*v1.Event, error) {
	// Service filtering is applied after decoding: the set is small and this
	// avoids building a variadic IN clause for the walking skeleton.
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM events WHERE hlc_wall_ms BETWEEN ? AND ?
		 ORDER BY hlc_wall_ms, hlc_logical, emitter_id`, q.FromWallMS, q.ToWallMS)
	if err != nil {
		return nil, err
	}
	all, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(q.Services) == 0 {
		if q.Limit > 0 && len(all) > q.Limit {
			all = all[:q.Limit]
		}
		return all, nil
	}
	want := map[string]bool{}
	for _, n := range q.Services {
		want[n] = true
	}
	var out []*v1.Event
	for _, e := range all {
		if want[e.GetService().GetName()] {
			out = append(out, e)
		}
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (s *Store) Gaps(ctx context.Context, from, to uint64) ([]*v1.Gap, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM gaps WHERE to_wall_ms >= ? AND from_wall_ms <= ?`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*v1.Gap
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var g v1.Gap
		if err := proto.Unmarshal(body, &g); err != nil {
			return nil, err
		}
		out = append(out, &g)
	}
	return out, rows.Err()
}

func (s *Store) Traces(ctx context.Context, q store.TraceQuery) ([]store.TraceSummary, error) {
	events, err := s.Window(ctx, store.WindowQuery{FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS})
	if err != nil {
		return nil, err
	}
	return store.SummarizeTraces(events, q), nil
}

func (s *Store) Close() error { return s.db.Close() }

func scanEvents(rows *sql.Rows) ([]*v1.Event, error) {
	defer rows.Close()
	var out []*v1.Event
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var e v1.Event
		if err := proto.Unmarshal(body, &e); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}
