package types

import "time"

// Stats is a point-in-time snapshot of a cache's counters. Ratios are derived
// so dashboards do not need to recompute them.
type Stats struct {
	Hits          int64   `json:"hits"`
	Misses        int64   `json:"misses"`
	Sets          int64   `json:"sets"`
	Deletes       int64   `json:"deletes"`
	Evictions     int64   `json:"evictions"`
	Errors        int64   `json:"errors"`
	Invalidations int64   `json:"invalidations"`
	HitRatio      float64 `json:"hit_ratio"`
}

// Health classifies the operational state of a component.
type Health string

const (
	HealthUp       Health = "up"
	HealthDegraded Health = "degraded"
	HealthDown     Health = "down"
)

// Item is a decoded cache value together with its metadata. Bulk read APIs
// return maps of Item so callers can distinguish hits from misses and inspect
// remaining TTL without a second round trip.
type Item struct {
	Key      string        `json:"key"`
	Value    string        `json:"value"`
	TTL      time.Duration `json:"ttl"`
	Found    bool          `json:"found"`
	Tags     []string      `json:"tags,omitempty"`
	StoredAt time.Time     `json:"stored_at"`
}

// ConsistencyModel describes the guarantee a replicated datum offers.
type ConsistencyModel string

const (
	// Eventual is the model used for presence and awareness: replicas converge
	// via last-writer-wins over pub/sub without coordination.
	Eventual ConsistencyModel = "eventual"
	// Strong is reserved for lock-protected critical sections.
	Strong ConsistencyModel = "strong"
)
