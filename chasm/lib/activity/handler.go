package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	response, key, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, request *workflowservice.StartActivityExecutionRequest) (*Activity, *workflowservice.StartActivityExecutionResponse, error) {
			newActivity, err := NewStandaloneActivity(mutableContext, request)
			if err != nil {
				return nil, nil, err
			}

			err = TransitionScheduled.Apply(newActivity, mutableContext, nil)
			if err != nil {
				return nil, nil, err
			}

			return newActivity, &workflowservice.StartActivityExecutionResponse{
				Started: true,
				// EagerTask: TODO when supported, need to call the same code that would handle the RecordActivityTaskStarted API
			}, nil
		},
		req.GetFrontendRequest())

	if err != nil {
		return nil, err
	}

	response.RunId = key.EntityID

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

// PollActivityExecution handles PollActivityExecutionRequest from frontend. This method is used by
// clients to poll for activity info and/or result, optionally as a long-poll.
func (h *handler) PollActivityExecution(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	switch req.GetFrontendRequest().GetWaitPolicy().(type) {
	case nil:
		return pollActivityExecutionNoWait(ctx, req)
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		return pollActivityExecutionWaitAnyStateChange(ctx, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		return pollActivityExecutionWaitCompletion(ctx, req)
	default:
		return nil, fmt.Errorf("unexpected wait policy type: %T", req.GetFrontendRequest().GetWaitPolicy())
	}
}

func pollActivityExecutionNoWait(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	// Returned token representing state of component seen by caller.
	var newStateChangeToken []byte
	// Returned info for the activity
	var activityInfo *activity.ActivityExecutionInfo
	var err error

	key := chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetActivityId(),
		EntityID:    request.GetRunId(),
	}

	response, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(a *Activity, chasmContext chasm.Context, _ any) (*workflowservice.PollActivityExecutionResponse, error) {
			if request.GetIncludeInfo() {
				activityInfo, err = a.buildActivityExecutionInfo(chasmContext, key)
				if err != nil {
					return nil, err
				}
			}
			return &workflowservice.PollActivityExecutionResponse{
				Info:                     activityInfo,
				RunId:                    "",  // TODO
				Outcome:                  nil, // TODO
				Input:                    nil, // TODO
				StateChangeLongPollToken: newStateChangeToken,
			}, nil
		},
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

func pollActivityExecutionWaitAnyStateChange(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	// Two cases:
	//
	// 1. Request does not have a token
	//    Last state seen by caller is unknown, so return immediately.

	// 2. Request has a token
	//    Extract caller's last-seen component state transition count from token and compare
	//    to current component state. If the component has already evolved beyond the
	//    caller's last-seen count then return immediately. Otherwise, use PollComponent to
	//    wait for the component transition count to exceed last-seen.
	request := req.GetFrontendRequest()
	waitPolicy := request.GetWaitPolicy().(*workflowservice.PollActivityExecutionRequest_WaitAnyStateChange)
	refBytesFromToken := waitPolicy.WaitAnyStateChange.GetLongPollToken()

	// serialized ref received in request
	var refFromToken chasm.ComponentRef
	// Returned token representing state of component seen by caller.
	var newStateChangeToken []byte
	// Returned info for the activity
	var activityInfo *activity.ActivityExecutionInfo
	var operationFn func(*Activity, chasm.MutableContext, any, any) (any, error)

	var err error

	if refBytesFromToken != nil {
		refFromToken, err = chasm.DeserializeComponentRef(refBytesFromToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("invalid long poll token")
		}
	} else {
		refFromToken = chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  request.GetActivityId(),
			EntityID:    request.GetRunId(),
		})
	}

	_, _, err = chasm.PollComponent(
		ctx,
		refFromToken,
		func(a *Activity, ctx chasm.Context, _ any) (any, bool, error) {
			refBytes, err := ctx.Ref(a)
			if err != nil {
				return nil, false, err
			}

			ref, err := chasm.DeserializeComponentRef(refBytes)
			if err != nil {
				return nil, false, err
			}

			// TODO: correct logic. Must use failover version.
			prevTransitionCount := refFromToken.GetEntityLastUpdateVersionedTransition().GetTransitionCount()
			newTransitionCount := ref.GetEntityLastUpdateVersionedTransition().GetTransitionCount()

			if newTransitionCount > prevTransitionCount {
				// Prev count unknown or less than new - capture new state and return
				newStateChangeToken = refBytes

				if request.GetIncludeInfo() {
					activityInfo, err = a.buildActivityExecutionInfo(ctx, chasm.EntityKey{
						NamespaceID: req.GetNamespaceId(),
						BusinessID:  request.GetActivityId(),
						EntityID:    request.GetRunId(),
					})
					if err != nil {
						return nil, false, err
					}
				}
				return nil, true, nil
			} else if newTransitionCount < prevTransitionCount {
				// TODO: error code?
				return nil, false, serviceerror.NewFailedPrecondition(
					fmt.Sprintf("invalid activity state: last seen transition count (%d) exceeds current (%d)", prevTransitionCount, newTransitionCount))
			} else {
				// No change yet - keep waiting
				return nil, false, nil
			}
		},
		operationFn,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			Info:                     activityInfo,
			RunId:                    "",  // TODO
			Outcome:                  nil, // TODO
			Input:                    nil, // TODO
			StateChangeLongPollToken: newStateChangeToken,
		},
	}, nil
}

func pollActivityExecutionWaitCompletion(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	// Returned token representing state of component seen by caller.
	var newStateChangeToken []byte
	// Returned info for the activity
	var activityInfo *activity.ActivityExecutionInfo
	// TODO: operationFn is unused; consider removing from API
	var operationFn func(*Activity, chasm.MutableContext, any, any) (any, error)

	_, _, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  request.GetActivityId(),
			EntityID:    request.GetRunId(),
		}),
		func(a *Activity, ctx chasm.Context, _ any) (any, bool, error) {
			completed := a.State() == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED

			if completed {
				refBytes, err := ctx.Ref(a)
				if err != nil {
					return nil, false, err
				}
				newStateChangeToken = refBytes
				if request.GetIncludeInfo() {
					activityInfo, err = a.buildActivityExecutionInfo(ctx, chasm.EntityKey{
						NamespaceID: req.GetNamespaceId(),
						BusinessID:  request.GetActivityId(),
						EntityID:    request.GetRunId(),
					})
					if err != nil {
						return nil, false, err
					}
				}
			}
			return nil, completed, nil
		},
		operationFn,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			Info:                     activityInfo,
			RunId:                    "",  // TODO
			Outcome:                  nil, // TODO
			Input:                    nil, // TODO
			StateChangeLongPollToken: newStateChangeToken,
		},
	}, nil
}
