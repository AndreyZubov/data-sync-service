package api

import (
	"time"

	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// runResponse is the JSON form of a sync run.
type runResponse struct {
	ID            int64      `json:"id"`
	Object        string     `json:"object"`
	Direction     string     `json:"direction"`
	Trigger       string     `json:"trigger"`
	Status        string     `json:"status"`
	FullSync      bool       `json:"full_sync"`
	BulkJobID     string     `json:"bulk_job_id,omitempty"`
	RecordsOK     int64      `json:"records_ok"`
	RecordsFailed int64      `json:"records_failed"`
	Error         string     `json:"error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

func newRunResponse(r store.Run) runResponse {
	return runResponse{
		ID:            r.ID,
		Object:        r.Object,
		Direction:     string(r.Direction),
		Trigger:       string(r.Trigger),
		Status:        string(r.Status),
		FullSync:      r.FullSync,
		BulkJobID:     r.BulkJobID,
		RecordsOK:     r.RecordsOK,
		RecordsFailed: r.RecordsFailed,
		Error:         r.Error,
		CreatedAt:     r.CreatedAt,
		StartedAt:     r.StartedAt,
		FinishedAt:    r.FinishedAt,
	}
}
