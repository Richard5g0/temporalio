package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// MockServer implements TemporalServer
type MockServer struct {
	mu                    sync.Mutex
	heartbeatFunc         func(taskToken string, details string) error
	respondCompletedFunc  func(taskToken string, result string) error
	respondFailedFunc     func(taskToken string, err error) error
	heartbeatCalls        int
	respondCompletedCalls int
	respondFailedCalls    int
}

func (m *MockServer) RecordActivityHeartbeat(taskToken string, details string) error {
	m.mu.Lock()
	m.heartbeatCalls++
	m.mu.Unlock()
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(taskToken, details)
	}
	return nil
}

func (m *MockServer) RespondActivityTaskCompleted(taskToken string, result string) error {
	m.mu.Lock()
	m.respondCompletedCalls++
	m.mu.Unlock()
	if m.respondCompletedFunc != nil {
		return m.respondCompletedFunc(taskToken, result)
	}
	return nil
}

func (m *MockServer) RespondActivityTaskFailed(taskToken string, err error) error {
	m.mu.Lock()
	m.respondFailedCalls++
	m.mu.Unlock()
	if m.respondFailedFunc != nil {
		return m.respondFailedFunc(taskToken, err)
	}
	return nil
}

func TestHeartbeatNotFoundCancelsActivityAndDoesNotReport(t *testing.T) {
	server := &MockServer{
		heartbeatFunc: func(taskToken string, details string) error {
			return ErrNotFound
		},
	}

	worker := NewWorker(server, 10*time.Millisecond)
	worker.retryBackoff = 1 * time.Millisecond

	ctx := context.Background()
	activityStarted := make(chan struct{})
	activityDone := make(chan struct{})
	var activityErr error

	activityFn := func(ctx context.Context) (string, error) {
		close(activityStarted)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return "success", nil
		}
	}

	go func() {
		activityErr = worker.ExecuteActivity(ctx, "task-1", activityFn)
		close(activityDone)
	}()

	<-activityStarted
	<-activityDone

	if activityErr == nil {
		t.Error("expected activity to return error, got nil")
	}

	server.mu.Lock()
	completedCalls := server.respondCompletedCalls
	failedCalls := server.respondFailedCalls
	server.mu.Unlock()

	if completedCalls > 0 {
		t.Errorf("expected 0 completed calls, got %d", completedCalls)
	}
	if failedCalls > 0 {
		t.Errorf("expected 0 failed calls, got %d", failedCalls)
	}
}

func TestHeartbeatTransientErrorRetriesAndSucceeds(t *testing.T) {
	var heartbeatCount int
	var mu sync.Mutex

	server := &MockServer{
		heartbeatFunc: func(taskToken string, details string) error {
			mu.Lock()
			defer mu.Unlock()
			heartbeatCount++
			if heartbeatCount == 1 {
				return ErrUnavailable
			}
			return nil
		},
	}

	worker := NewWorker(server, 10*time.Millisecond)
	worker.retryBackoff = 2 * time.Millisecond

	ctx := context.Background()
	activityFn := func(ctx context.Context) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "success", nil
	}

	err := worker.ExecuteActivity(ctx, "task-2", activityFn)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	server.mu.Lock()
	completedCalls := server.respondCompletedCalls
	server.mu.Unlock()

	if completedCalls != 1 {
		t.Errorf("expected 1 completed call, got %d", completedCalls)
	}

	mu.Lock()
	count := heartbeatCount
	mu.Unlock()
	if count < 2 {
		t.Errorf("expected at least 2 heartbeat attempts (1 failed, 1 retried/succeeded), got %d", count)
	}
}

func TestNormalActivityExecution(t *testing.T) {
	server := &MockServer{}
	worker := NewWorker(server, 10*time.Millisecond)
	worker.retryBackoff = 1 * time.Millisecond

	ctx := context.Background()
	activityFn := func(ctx context.Context) (string, error) {
		return "done", nil
	}

	err := worker.ExecuteActivity(ctx, "task-3", activityFn)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	server.mu.Lock()
	completedCalls := server.respondCompletedCalls
	failedCalls := server.respondFailedCalls
	server.mu.Unlock()

	if completedCalls != 1 {
		t.Errorf("expected 1 completed call, got %d", completedCalls)
	}
	if failedCalls != 0 {
		t.Errorf("expected 0 failed calls, got %d", failedCalls)
	}
}
