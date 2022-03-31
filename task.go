package pipeman

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var (

	// options validation errors
	ErrEmptyNamespace = errors.New("pipeman: empty namespace")
	ErrEmptyQname     = errors.New("pipeman: empty qname")
	ErrAt             = errors.New("pipeman: at should not be zero")
	ErrInvisibleSec   = errors.New("pipeman: invisible sec should be >= 0")
)

// Task it describes information about the task itself.
type Task struct {
	ID         string    `json:"id"`
	Payload    []byte    `json:"payload"`
	Error      string    `json:"error"`
	CreatedAt  time.Time `json:"created_at"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	RetriedAt  time.Time `json:"retried_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// InvalidPayloadError it represents the payload decoding error.
type InvalidPayloadError struct {
	Err error
}

func (e *InvalidPayloadError) Error() string {
	return fmt.Sprintf("pipeman: invalid task payload: %v", e.Err)
}

func (t *Task) unmarshalPayload(v interface{}) error {
	err := jsoniter.Unmarshal(t.Payload, v)
	if err != nil {
		return &InvalidPayloadError{Err: err}
	}
	return nil
}

func (t *Task) marshalPayload(v interface{}) error {
	b, err := jsoniter.Marshal(v)
	if err != nil {
		return err
	}
	t.Payload = b
	return nil
}

func NewTask() *Task {
	taskID := uuid.NewString()
	now := time.Now().Truncate(time.Second)
	return &Task{
		ID:         taskID,
		CreatedAt:  now,
		UpdatedAt:  now,
		EnqueuedAt: now,
	}
}

// Enqueuer enqueues a task
type Enqueuer interface {
	Enqueue(*Task, *EnqueueOptions) error
}

// EnqueueOptions specifies how a task is enqueued.
type EnqueueOptions struct {
	// Namesapce is a data isolation space for each queue
	Namespace string
	// Qname is the name of a queue
	Qname string
}

// Validate to check the validity of the EnqueueOptions.
func (opts *EnqueueOptions) Validate() error {
	if opts.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opts.Qname == "" {
		return ErrEmptyQname
	}
	return nil
}

// Dequeuer dequeues a task.
// If a task is processed successfully, call Ack() to delete the job.
type Dequeuer interface {
	Dequeue(*DequeueOptions) (*Task, error)
	Ack(*Task, *AckOptions) error
}

// DequeueOptions specifies how a task is dequeued.
type DequeueOptions struct {
	// Namespace is the namespace of a queue.
	Namespace string
	// Qname is the name of a queue.
	Qname string
	// At is the current time of the dequeuer.
	// Any task that is scheduled before this can be executed.
	At time.Time
	// After the task is dequeued, no other dequeuer can see this task for a while.
	// InvisibleSec controls how long this period is.
	InvisibleSec int64
}

// Validate to check the validity of the DequeueOptions.
func (opt *DequeueOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.Qname == "" {
		return ErrEmptyQname
	}
	if opt.At.IsZero() {
		return ErrAt
	}
	if opt.InvisibleSec < 0 {
		return ErrInvisibleSec
	}
	return nil
}

// AckOptions specifies how a task is deleted from a queue.
type AckOptions struct {
	// Namesapce is a data isolation space for each queue
	Namespace string
	// Qname is the name of a queue
	Qname string
}

// Validate to check the validity of the AckOptions.
func (opts *AckOptions) Validate() error {
	if opts.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opts.Qname == "" {
		return ErrEmptyQname
	}
	return nil
}

// FindOptions specifies how a job is searched from a queue.
type FindOptions struct {
	Namespace string
}

// Validate to check the validity of the FindOptions.
func (opts *FindOptions) Validate() error {
	if opts.Namespace == "" {
		return ErrEmptyNamespace
	}
	return nil
}

// Queue can enqueue and dequeue jobs.
type Queue interface {
	Enqueuer
	Dequeuer
}
