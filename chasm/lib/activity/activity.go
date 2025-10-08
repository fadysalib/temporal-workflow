package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState
}

func (a Activity) LifecycleState(context chasm.Context) chasm.LifecycleState {
	switch a.ActivityState.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateCompleted
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// GetActivityExecutionInfo constructs an ActivityExecutionInfo from the CHASM activity component.
// namespace, activityID, and runID come from the CHASM component key, not from persisted data.
func (a *Activity) GetActivityExecutionInfo(key chasm.EntityKey) *activity.ActivityExecutionInfo {
	if a.ActivityState == nil {
		return nil
	}

	// Convert internal status to external status and run state
	var status enums.ActivityExecutionStatus
	var runState enums.PendingActivityState

	switch a.ActivityState.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		status = enums.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	// TODO: Add pause support
	// case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
	// 	status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
	// 	runState = enums.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	// case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
	// 	status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
	// 	runState = enums.PENDING_ACTIVITY_STATE_PAUSED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		status = enums.ACTIVITY_EXECUTION_STATUS_COMPLETED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		status = enums.ACTIVITY_EXECUTION_STATUS_FAILED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		status = enums.ACTIVITY_EXECUTION_STATUS_CANCELED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		status = enums.ACTIVITY_EXECUTION_STATUS_TERMINATED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		status = enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	}

	info := &activity.ActivityExecutionInfo{
		// From CHASM component key
		ActivityId: key.BusinessID,
		RunId:      key.EntityID,
		// Note: NamespaceId is not a field in ActivityExecutionInfo

		// From ActivityState
		ActivityType:    a.ActivityState.ActivityType,
		ActivityOptions: a.ActivityState.ActivityOptions,
		Status:          status,
		RunState:        runState,
		ScheduledTime:   a.ActivityState.ScheduledTime,
		Priority:        a.ActivityState.Priority,
		Input:           a.RequestData.Input,
		Header:          a.RequestData.Header,

		// TODO: These fields are left at zero value for now:
		// - StartedTime
		// - LastHeartbeatTime
		// - HeartbeatDetails
		// - RetryInfo
		// - AutoResetPoints
		// - ClockTime
	}

	return info
}

func NewActivity(request *workflowservice.StartActivityExecutionRequest) *Activity {
	return &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:    request.ActivityType,
			ActivityOptions: request.Options,
			// TODO(dan): is this the correct way to compute this timestamp?
			ScheduledTime: timestamppb.New(clock.NewRealTimeSource().Now()),
			RequestData: &activitypb.ActivityRequestData{
				Input:        request.Input,
				Header:       request.Header,
				UserMetadata: request.UserMetadata,
			},
		},
	}
}

func GetActivity(ctx context.Context, key chasm.EntityKey) (*Activity, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(
			a *Activity,
			ctx chasm.Context,
			_ any,
		) (*activitypb.ActivityState, error) {
			fmt.Println("Reading activity state for", key.BusinessID)

			return common.CloneProto(a.ActivityState), nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Activity{
		ActivityState: state,
	}, nil
}

// TODO(dan): Let's collect here the heuristics we use to classify different types of requests as
// being for chasm (i.e. standalone) activities.
func ShouldHandle(req any) bool {
	switch req := req.(type) {
	case *historyservice.RecordActivityTaskStartedRequest:
		return req.GetComponentRef() != nil
	}
	return false
}

// TODO(dan): This one takes token to avoid deserializing it twice.
func ShouldHandleTaskToken(req any, token *tokenspb.Task) bool {
	switch req.(type) {
	case *historyservice.RespondActivityTaskCompletedRequest:
		return token.WorkflowId == ""
	case *historyservice.RespondActivityTaskFailedRequest:
		return token.WorkflowId == ""
	case *historyservice.RespondActivityTaskCanceledRequest:
		return token.WorkflowId == ""
	}
	return false

}

// An alternative design would be to allow the Handle* functions below to return a nil response to
// indicate that something else should handle it.

func HandleRecordActivityTaskStarted(
	ctx context.Context,
	req *historyservice.RecordActivityTaskStartedRequest,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	componentRefProto := req.GetComponentRef()
	if componentRefProto == nil {
		return nil, fmt.Errorf("component ref is required")
	}
	componentRef := chasm.ProtoRefToComponentRef(componentRefProto)

	var activityType *commonpb.ActivityType
	var input *commonpb.Payloads
	var header *commonpb.Header
	var taskQueue *taskqueuepb.TaskQueue

	_, _, err := chasm.UpdateComponent(
		ctx,
		componentRef,
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			err := TransitionStarted.Apply(a, ctx, nil)
			if err != nil {
				return struct{}{}, err
			}
			activityType = a.ActivityType
			taskQueue = a.ActivityOptions.TaskQueue
			input = a.RequestData.Input
			header = a.RequestData.Header
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &historyservice.RecordActivityTaskStartedResponse{
		ScheduledEvent: &history.HistoryEvent{
			EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &history.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &history.ActivityTaskScheduledEventAttributes{
					ActivityId:   componentRef.BusinessID,
					ActivityType: activityType,
					Input:        input,
					Header:       header,
					TaskQueue:    taskQueue,
				},
			},
		},
	}, nil
}

// This is a handler for a workflowservice method (as opposed to a method in the service owned by
// this chasm component).
func HandleRespondActivityTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
	key chasm.EntityKey,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			err := TransitionCompleted.Apply(a, ctx, nil)
			if err != nil {
				return struct{}{}, err
			}
			a.Outcome = &activitypb.ActivityState_Result{
				Result: req.CompleteRequest.Result,
			}
			return struct{}{}, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// This is a handler for a workflowservice method (as opposed to a method in the service owned by
// this chasm component).
func HandleRespondActivityTaskFailed(
	ctx context.Context,
	req *historyservice.RespondActivityTaskFailedRequest,
	key chasm.EntityKey,
) (*historyservice.RespondActivityTaskFailedResponse, error) {
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			err := TransitionFailed.Apply(a, ctx, nil)
			if err != nil {
				return struct{}{}, err
			}
			a.Outcome = &activitypb.ActivityState_Failure{
				Failure: req.FailedRequest.Failure,
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// This is a handler for a workflowservice method (as opposed to a method in the service owned by
// this chasm component).
func HandleRespondActivityTaskCanceled(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCanceledRequest,
	key chasm.EntityKey,
) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			err := TransitionCanceled.Apply(a, ctx, nil)
			if err != nil {
				return struct{}{}, err
			}
			a.Outcome = &activitypb.ActivityState_Failure{
				Failure: &failurepb.Failure{
					Message: "Activity canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{
							Details: req.CancelRequest.Details,
						},
					},
				},
			}
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}
