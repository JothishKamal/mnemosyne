// Package exporter ships recorded events to a collector.
//
// Two rules govern everything here: the application is never blocked, and
// memory is never unbounded. When either would be violated, events are dropped
// and the drop is described by a Gap record so the recording admits its holes.
package exporter

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"time"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/proto"
)

// Config configures an Exporter.
type Config struct {
	Endpoint      string
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	Client        *http.Client
	EmitterID     [8]byte
}

// Exporter batches events and posts them to a collector.
type Exporter struct {
	cfg    Config
	queue  chan *v1.Event
	client *http.Client

	mu       sync.Mutex
	dropped  uint64
	gapFrom  *v1.HLC
	gapTo    *v1.HLC
	gapCount uint64
	pending  []*v1.Gap

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// New returns a running Exporter.
func New(cfg Config) *Exporter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Second}
	}
	e := &Exporter{
		cfg:    cfg,
		queue:  make(chan *v1.Event, cfg.QueueSize),
		client: cfg.Client,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}

// Emit queues an event. It never blocks; a full queue drops the event and
// records the drop.
func (e *Exporter) Emit(ev *v1.Event) {
	select {
	case e.queue <- ev:
	default:
		e.recordDrop(ev.GetHlc(), v1.GapReason_BACKPRESSURE)
	}
}

// recordDrop accumulates a drop into the open gap window.
func (e *Exporter) recordDrop(at *v1.HLC, reason v1.GapReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropped++
	e.gapCount++
	if at == nil {
		at = &v1.HLC{}
	}
	if e.gapFrom == nil || at.WallMs < e.gapFrom.WallMs {
		e.gapFrom = &v1.HLC{WallMs: at.WallMs, Logical: at.Logical}
	}
	if e.gapTo == nil || at.WallMs > e.gapTo.WallMs {
		e.gapTo = &v1.HLC{WallMs: at.WallMs, Logical: at.Logical}
	}
	_ = reason
}

// takeGaps closes the open gap window and returns it for transmission.
func (e *Exporter) takeGaps() []*v1.Gap {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.pending
	e.pending = nil
	if e.gapCount > 0 {
		id := e.cfg.EmitterID
		out = append(out, &v1.Gap{
			EmitterId:    id[:],
			From:         e.gapFrom,
			To:           e.gapTo,
			DroppedCount: e.gapCount,
			Reason:       v1.GapReason_BACKPRESSURE,
		})
		e.gapCount, e.gapFrom, e.gapTo = 0, nil, nil
	}
	return out
}

func (e *Exporter) run() {
	defer close(e.done)
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*v1.Event, 0, e.cfg.BatchSize)
	send := func() {
		if len(batch) == 0 && e.gapCount == 0 {
			return
		}
		e.post(context.Background(), batch, e.takeGaps())
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-e.queue:
			batch = append(batch, ev)
			if len(batch) >= e.cfg.BatchSize {
				send()
			}
		case <-ticker.C:
			send()
		case <-e.stop:
			// Drain what is already queued, then send a final batch.
			for {
				select {
				case ev := <-e.queue:
					batch = append(batch, ev)
					continue
				default:
				}
				break
			}
			send()
			return
		}
	}
}

// post transmits a batch. Transmission failures become drops, never blocking
// retries: a collector outage must not turn into an application outage.
func (e *Exporter) post(ctx context.Context, events []*v1.Event, gaps []*v1.Gap) {
	body, err := proto.Marshal(&v1.Batch{Events: events, Gaps: gaps})
	if err != nil {
		e.countLoss(len(events), v1.GapReason_BACKPRESSURE)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint+"/v1/batch", bytes.NewReader(body))
	if err != nil {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.client.Do(req)
	if err != nil {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
	}
}

// countLoss records the loss of an entire failed batch.
func (e *Exporter) countLoss(n int, reason v1.GapReason) {
	if n == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropped += uint64(n)
	id := e.cfg.EmitterID
	e.pending = append(e.pending, &v1.Gap{
		EmitterId:    id[:],
		From:         &v1.HLC{},
		To:           &v1.HLC{},
		DroppedCount: uint64(n),
		Reason:       reason,
	})
}

// Flush sends everything currently queued.
func (e *Exporter) Flush(ctx context.Context) error {
	var batch []*v1.Event
	for {
		select {
		case ev := <-e.queue:
			batch = append(batch, ev)
			continue
		default:
		}
		break
	}
	e.post(ctx, batch, e.takeGaps())
	return nil
}

// Close stops the exporter, flushing what it can.
func (e *Exporter) Close() error {
	e.once.Do(func() {
		close(e.stop)
		<-e.done
	})
	return nil
}

// DroppedCount returns the total number of events dropped.
func (e *Exporter) DroppedCount() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// SetEmitterID tells the exporter which emitter it ships for. It must be called
// immediately after the Recorder is constructed and before any event is
// emitted; gap records are attributed with this ID.
func (e *Exporter) SetEmitterID(id [8]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg.EmitterID = id
}
