package worker

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
)

const (
	// Default lease duration if not specified in the request.
	defaultLeaseDuration = 1 * time.Minute
)

type handler struct {
	workerstatepb.UnimplementedWorkerServiceServer
	enableWorkerStateTracking dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func newHandler(dc *dynamicconfig.Collection) *handler {
	return &handler{
		enableWorkerStateTracking: dynamicconfig.EnableWorkerStateTracking.Get(dc),
	}
}

func (h *handler) RecordHeartbeat(ctx context.Context, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
	// Check if worker state tracking is enabled for this namespace
	if !h.enableWorkerStateTracking(req.NamespaceId) {
		// Worker state tracking is disabled, return error
		return nil, fmt.Errorf("worker state tracking is disabled for namespace %s", req.NamespaceId)
	}

	// Get lease duration from request
	leaseDuration := req.LeaseDuration.AsDuration()
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}

	// Try to update existing worker, or create new one if it doesn't exist
	_, _, _, _, err := chasm.UpdateWithNewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.WorkerHeartbeat.WorkerInstanceKey,
		},
		func(ctx chasm.MutableContext, heartbeat *HeartbeatRequest) (*Worker, *workerstatepb.RecordHeartbeatResponse, error) {
			// Create new worker and record heartbeat
			w := NewWorker()
			err := w.RecordHeartbeat(ctx, heartbeat.WorkerHeartbeat, heartbeat.LeaseDuration)
			if err != nil {
				return nil, nil, err
			}
			return w, &workerstatepb.RecordHeartbeatResponse{}, nil
		},
		func(w *Worker, ctx chasm.MutableContext, heartbeat *HeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
			// Update existing worker with new heartbeat
			err := w.RecordHeartbeat(ctx, heartbeat.WorkerHeartbeat, heartbeat.LeaseDuration)
			if err != nil {
				return nil, err
			}
			return &workerstatepb.RecordHeartbeatResponse{}, nil
		},
		&HeartbeatRequest{
			WorkerHeartbeat: req.WorkerHeartbeat,
			LeaseDuration:   leaseDuration,
		},
	)

	if err != nil {
		return nil, err
	}

	return &workerstatepb.RecordHeartbeatResponse{}, nil
}
