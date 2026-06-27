package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Define sentinel errors
var (
	ErrNotFound         = errors.New("activity not found")
	ErrActivityNotFound = errors.New("activity execution not found")
	ErrUnavailable      = errors.New("service unavailable")
)

// TemporalServer defines the interface for communicating with the Temporal Server.
type TemporalServer interface {
	RecordActivityHeartbeat(taskToken string, details string) error
	RespondActivityTaskCompleted(taskToken string, result string) error
	RespondActivityTaskFailed(taskToken string, err error) error
}

// Worker manages activity execution and heartbeating.
type Worker struct {
	server            TemporalServer
	heartbeatInterval time.Duration
	retryBackoff      time.Duration
	maxRetries        int
}

// NewWorker creates a new Worker instance.
func NewWorker(server TemporalServer, heartbeatInterval time.Duration) *Worker {
	return &Worker{
		server:            server,
		heartbeatInterval: heartbeatInterval,
		retryBackoff:      50 * time.Millisecond,
		maxRetries:        3,
	}
}

// ExecuteActivity runs the activity function, manages its heartbeat loop, and reports results.
func (w *Worker) ExecuteActivity(ctx context.Context, taskToken string, activityFn func(ctx context.Context) (string, error)) error {
	// Create a cancelable context for the activity
	actCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track if the activity was aborted due to heartbeat timeout
	var aborted bool
	var abortedMu sync.Mutex

	setAborted := func() {
		abortedMu.Lock()
		aborted = true
		abortedMu.Unlock()
	}

	isAborted := func() bool {
		abortedMu.Lock()
		defer abortedMu.Unlock()
		return aborted
	}

	// Start the heartbeat loop
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(w.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-actCtx.Done():
				return
			case <-ticker.C:
				err := w.recordHeartbeatWithRetry(taskToken, "heartbeat details")
				if err != nil {
					if errors.Is(err, ErrNotFound) || errors.Is(err, ErrActivityNotFound) {
						setAborted()
						cancel() // Cancel the activity context immediately
						return
					}
				}
			}
		}
	}()

	// Execute the activity function
	result, err := activityFn(actCtx)

	// Wait for heartbeat loop to finish
	cancel()
	<-heartbeatDone

	// If the activity was aborted due to heartbeat timeout, do not report completion/failure
	if isAborted() {
		return fmt.Errorf("activity aborted: heartbeat timeout on server")
	}

	// Report completion or failure
	if err != nil {
		reportErr := w.server.RespondActivityTaskFailed(taskToken, err)
		if reportErr != nil {
			return fmt.Errorf("failed to report failure: %w", reportErr)
		}
		return err
	}

	reportErr := w.server.RespondActivityTaskCompleted(taskToken, result)
	if reportErr != nil {
		return fmt.Errorf("failed to report completion: %w", reportErr)
	}

	return nil
}

func (w *Worker) recordHeartbeatWithRetry(taskToken string, details string) error {
	var err error
	backoff := w.retryBackoff

	for i := 0; i <= w.maxRetries; i++ {
		err = w.server.RecordActivityHeartbeat(taskToken, details)
		if err == nil {
			return nil
		}

		// If it's a non-transient error (NotFound / ActivityNotFound), return immediately
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrActivityNotFound) {
			return err
		}

		// If it's a transient error (Unavailable), retry with backoff
		if errors.Is(err, ErrUnavailable) {
			if i < w.maxRetries {
				time.Sleep(backoff)
				backoff *= 2
				continue
			}
		}

		// For any other error, return it
		return err
	}
	return err
}

func main() {
	fmt.Println("Hello, Bounty Hunter!")
}
