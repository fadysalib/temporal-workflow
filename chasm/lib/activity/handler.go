package activity

import (
	"context"
	"fmt"

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
		return chasm.ReadComponent(
			ctx,
			chasm.NewComponentRef[*Activity](chasm.EntityKey{
				NamespaceID: req.GetNamespaceId(),
				BusinessID:  req.GetFrontendRequest().GetActivityId(),
				EntityID:    req.GetFrontendRequest().GetRunId(),
			}),
			(*Activity).buildPollActivityExecutionResponse,
			nil,
			nil,
		)
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		return pollActivityExecutionWaitAnyStateChange(ctx, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		return pollActivityExecutionWaitCompletion(ctx, req)
	default:
		return nil, fmt.Errorf("unexpected wait policy type: %T", req.GetFrontendRequest().GetWaitPolicy())
	}
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

	var refFromToken chasm.ComponentRef

	if refBytesFromToken != nil {
		var err error
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

	response, ref, err := chasm.PollComponent(
		ctx,
		refFromToken,
		func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
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
			stateChanged := newTransitionCount > prevTransitionCount

			if stateChanged {
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			} else if newTransitionCount < prevTransitionCount {
				// TODO: error code?
				return nil, false, serviceerror.NewFailedPrecondition(
					fmt.Sprintf("invalid activity state: last seen transition count (%d) exceeds current (%d)", prevTransitionCount, newTransitionCount))
			} else {
				// No change yet - keep waiting
				return nil, false, nil
			}
		},
		req,
	)
	if err != nil {
		return nil, err
	}
	response.GetFrontendResponse().StateChangeLongPollToken = ref
	return response, nil
}

func pollActivityExecutionWaitCompletion(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	response, ref, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
			EntityID:    req.GetFrontendRequest().GetRunId(),
		}),
		func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			completed := a.State() == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED
			if completed {
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			}
			return nil, false, nil
		},
		req,
	)
	if err != nil {
		return nil, err
	}
	response.GetFrontendResponse().StateChangeLongPollToken = ref
	return response, nil
}
