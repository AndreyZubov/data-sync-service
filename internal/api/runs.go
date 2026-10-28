package api

import (
	"encoding/json"
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
	Resumes       int        `json:"resumes,omitempty"`
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
		Resumes:       r.Resumes,
		CreatedAt:     r.CreatedAt,
		StartedAt:     r.StartedAt,
		FinishedAt:    r.FinishedAt,
	}
}

// outboxEntryResponse is the JSON form of an outbox entry.
type outboxEntryResponse struct {
	ID              int64           `json:"id"`
	Operation       string          `json:"operation"`
	ExternalIDField string          `json:"external_id_field,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	Status          string          `json:"status"`
	Attempts        int             `json:"attempts"`
	LastError       string          `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

func newOutboxEntryResponse(e store.OutboxEntry) outboxEntryResponse {
	return outboxEntryResponse{
		ID:              e.ID,
		Operation:       string(e.Operation),
		ExternalIDField: e.ExternalIDField,
		Payload:         e.Payload,
		Status:          string(e.Status),
		Attempts:        e.Attempts,
		LastError:       e.LastError,
		CreatedAt:       e.CreatedAt,
		UpdatedAt:       e.UpdatedAt,
	}
}
