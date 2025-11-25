// package repo

// import (
// 	"context"
// 	"time"

// 	"github.com/jackc/pgx/v5/pgxpool"
// )

// type Repo struct {
// 	pg *pgxpool.Pool
// }

// type Notification struct {
// 	Id string
// 	OrgId string
// 	EventType string
// 	Payload []byte
// 	Channel string
// 	Recipient string
// 	Status string
// 	Attempts int
// 	CreateAt time.Time
// 	UpdateAt time.Time
// }

// func NewRepo(dbURL string) (*Repo, error) {
// 	cfg, err := pgxpool.ParseConfig(dbURL)
// 	if err != nil {
// 		return nil, err
// 	}
// 	pg, err := pgxpool.NewWithConfig(context.Background(), cfg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Repo{pg: pg}, nil
// }

// func (r *Repo) Close() {
// 	r.pg.Close()
// }

// func (r *Repo) InsertNotification(ctx context.Context, n *Notification) (string, error) {
// 	var id string
// 	err := r.pg.QueryRow(ctx,
// 	`INSERT INTO notifications(organization_id, event_type, payload, channel, recipient) 
// 		 VALUES($1,$2,$3,$4,$5) RETURNING id`,
// 		 n.OrgId, n.EventType, n.Payload, n.Channel, n.Recipient).Scan(&id)
// 	return id, err
// }

// func (r *Repo) FetchQueued(ctx context.Context, limit int) ([]Notification, error) {
// 	rows, err := r.pg.Query(ctx, `SELECT id, org_id, event_type, payload, channel, recipient, status, attempts, created_at, updated_at 
// 	 FROM notifications WHERE status='queued' ORDER BY created_at LIMIT $1`, limit)
	
// 	if err != nil {
// 		return nil, err
// 	}

// 	defer rows.Close()
// 	var res []Notification
// 	for rows.Next() {
// 		var n Notification
// 		if err := rows.Scan(&n.Id, &n.OrgId, &n.EventType, &n.Payload, &n.Channel, &n.Recipient, &n.Status, &n.Attempts, &n.CreateAt, &n.UpdateAt); err != nil {
// 			return nil, err
// 		}
// 		res = append(res, n)
// 	}
// 	return res, nil
// }

// func (r *Repo) MarkProcessing(ctx context.Context, id string) error {
// 	_, err := r.pg.Exec(ctx, `UPDATE notifications SET status='processing', updated_at=NOW() WHERE id=$1`, id)
// 	return err
// }

// func (r *Repo) MarkSent(ctx context.Context, id string) error {
// 	_, err := r.pg.Exec(ctx, `UPDATE notifications SET status='sent', attempts=attempts+1, sent_at=now(), updated_at=now() WHERE id=$1`, id)
// 	return err
// }

// func (r *Repo) MarkFailed(ctx context.Context, id, lastErr string) error {
// 	_, err := r.pg.Exec(ctx, `UPDATE notifications SET status='failed', attempts=attempts+1, last_error=$2, updated_at=now() WHERE id=$1`, id, lastErr)
// 	return err
// }
package repo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repo struct {
	pg *pgxpool.Pool
}

type Notification struct {
	ID string
	OrgID string
	EventType string
	Payload []byte
	Channel string
	Recipient string
	Status string
	Attempts int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewRepo(dbURL string) (*Repo, error) {
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil { return nil, err }
	pg, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil { return nil, err }
	return &Repo{pg: pg}, nil
}

func (r *Repo) Close() { r.pg.Close() }

// optionally persist audit into global DB
func (r *Repo) InsertAudit(ctx context.Context, n *Notification) error {
	_, err := r.pg.Exec(ctx, `INSERT INTO notifications(org_id, type, payload, channel, recipient, status) VALUES ($1,$2,$3,$4,$5,$6)`,
		n.OrgID, n.EventType, n.Payload, n.Channel, n.Recipient, n.Status)
	return err
}
