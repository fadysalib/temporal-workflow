package activity

import (
	"context"
	"fmt"
	"os"

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
	request := req.GetFrontendRequest()
	_, key, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  request.GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, _ any) (*Activity, any, error) {
			newActivity := NewActivity(request)
			err := TransitionScheduled.Apply(newActivity, mutableContext, nil)
			if err != nil {
				return nil, nil, err
			}
			mutableContext.AddTask(newActivity, chasm.TaskAttributes{}, &activitypb.ActivityStartExecuteTask{}) // Move to component
			return newActivity, nil, nil
		}, nil)

	if err != nil {
		return nil, err
	}
	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			// TODO: Started
			RunId: key.EntityID,
		},
	}, nil
}

func (h *handler) DescribeActivityExecution(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	key := chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetActivityId(),
		EntityID:    request.GetRunId(),
	}
	act, err := GetActivity(ctx, key)
	if err != nil {
		return nil, err
	}
	return &activitypb.DescribeActivityExecutionResponse{
		FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{
			// TODO: LongPollToken
			Info: act.GetActivityExecutionInfo(key),
		},
	}, nil
}

func (h *handler) GetActivityExecutionResult(ctx context.Context, req *activitypb.GetActivityExecutionResultRequest) (*activitypb.GetActivityExecutionResultResponse, error) {
	request := req.GetFrontendRequest()
	key := chasm.EntityKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  request.GetActivityId(),
		EntityID:    request.GetRunId(),
	}

	act, err := GetActivity(ctx, key)
	if err != nil {
		return nil, err
	}

	if request.GetWait() {
		fmt.Fprintln(os.Stderr, "TODO: GetActivityExecutionResult long-polling is not implemented")
	}

	response := &workflowservice.GetActivityExecutionResultResponse{
		RunId: key.EntityID,
	}

	switch outcome := act.GetOutcome().(type) {
	case *activitypb.ActivityState_Result:
		// Completed
		response.Outcome = &workflowservice.GetActivityExecutionResultResponse_Result{
			Result: outcome.Result,
		}
	case *activitypb.ActivityState_Failure:
		// Failed, Canceled, Terminated
		response.Outcome = &workflowservice.GetActivityExecutionResultResponse_Failure{
			Failure: outcome.Failure,
		}
	}

	return &activitypb.GetActivityExecutionResultResponse{
		FrontendResponse: response,
	}, nil
}
