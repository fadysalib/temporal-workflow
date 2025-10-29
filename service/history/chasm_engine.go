package history

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	ChasmEngine struct {
		entityCache     cache.Cache
		shardController shard.Controller
		registry        *chasm.Registry
		config          *configs.Config
	}

	newEntityParams struct {
		entityRef     chasm.ComponentRef
		entityContext historyi.WorkflowContext
		mutableState  historyi.MutableState
		snapshot      *persistence.WorkflowSnapshot
		events        []*persistence.WorkflowEvents
	}

	currentRunInfo struct {
		createRequestID string
		*persistence.CurrentWorkflowConditionFailedError
	}
)

var defaultTransitionOptions = chasm.TransitionOptions{
	ReusePolicy:    chasm.BusinessIDReusePolicyAllowDuplicate,
	ConflictPolicy: chasm.BusinessIDConflictPolicyFail,
	RequestID:      "",
	Speculative:    false,
}

var ChasmEngineModule = fx.Options(
	fx.Provide(newChasmEngine),
	fx.Provide(func(impl *ChasmEngine) chasm.Engine { return impl }),
	fx.Invoke(func(impl *ChasmEngine, shardController shard.Controller) {
		impl.SetShardController(shardController)
	}),
)

func newChasmEngine(
	entityCache cache.Cache,
	registry *chasm.Registry,
	config *configs.Config,
) *ChasmEngine {
	return &ChasmEngine{
		entityCache: entityCache,
		registry:    registry,
		config:      config,
	}
}

// This is for breaking fx cycle dependency.
// ChasmEngine -> ShardController -> ShardContextFactory -> HistoryEngineFactory -> QueueFactory -> ChasmEngine
func (e *ChasmEngine) SetShardController(
	shardController shard.Controller,
) {
	e.shardController = shardController
}

func (e *ChasmEngine) NewEntity(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	opts ...chasm.TransitionOption,
) (entityKey chasm.EntityKey, newEntityRef []byte, retErr error) {
	options := e.constructTransitionOptions(opts...)

	shardContext, err := e.getShardContext(entityRef)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}

	currentEntityReleaseFn, err := e.lockCurrentEntity(
		ctx,
		shardContext,
		namespace.ID(entityRef.NamespaceID),
		entityRef.BusinessID,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	defer func() {
		currentEntityReleaseFn(retErr)
	}()

	newEntityParams, err := e.createNewEntity(
		ctx,
		shardContext,
		entityRef,
		newFn,
		options,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}

	currentRunInfo, hasCurrentRun, err := e.persistAsBrandNew(
		ctx,
		shardContext,
		newEntityParams,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	if !hasCurrentRun {
		serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
		if err != nil {
			return chasm.EntityKey{}, nil, err
		}
		return newEntityParams.entityRef.EntityKey, serializedRef, nil
	}

	return e.handleEntityConflict(
		ctx,
		shardContext,
		newEntityParams,
		currentRunInfo,
		options,
	)
}

func (e *ChasmEngine) UpdateWithNewEntity(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (newEntityKey chasm.EntityKey, newEntityRef []byte, retError error) {
	return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("UpdateWithNewEntity is not yet supported")
}

func (e *ChasmEngine) UpdateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (updatedRef []byte, retError error) {

	shardContext, executionLease, err := e.getExecutionLease(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer func() {
		executionLease.GetReleaseFn()(retError)
	}()

	mutableState := executionLease.GetMutableState()
	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return nil, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
		)
	}

	mutableContext := chasm.NewMutableContext(ctx, chasmTree)
	component, err := chasmTree.Component(mutableContext, ref)
	if err != nil {
		return nil, err
	}

	if err := updateFn(mutableContext, component); err != nil {
		return nil, err
	}

	// TODO: Support WithSpeculative() TransitionOption.

	if err := executionLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return nil, err
	}

	if err := notifyChasmComponentUpdate(shardContext, ref); err != nil {
		shardContext.GetLogger().Error("failed to send CHASM component update notification", tag.Error(err))
	}

	newSerializedRef, err := mutableContext.Ref(component)
	if err != nil {
		return nil, serviceerror.NewInternalf("componentRef: %+v: %s", ref, err)
	}

	return newSerializedRef, nil
}

func (e *ChasmEngine) ReadComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	readFn func(chasm.Context, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (retError error) {
	_, executionLease, err := e.getExecutionLease(ctx, ref)
	if err != nil {
		return err
	}
	defer func() {
		// Always release the lease with nil error since this is a read only operation
		// So even if it fails, we don't need to clear and reload mutable state.
		executionLease.GetReleaseFn()(nil)
	}()

	chasmTree, ok := executionLease.GetMutableState().ChasmTree().(*chasm.Node)
	if !ok {
		return serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			executionLease.GetMutableState().ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewContext(ctx, chasmTree)
	component, err := chasmTree.Component(chasmContext, ref)
	if err != nil {
		return err
	}

	return readFn(chasmContext, component)
}

func (e *ChasmEngine) PollComponent(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	predicateFn func(chasm.Context, chasm.Component) (any, bool, error),
	operationFn func(chasm.MutableContext, chasm.Component, any) error,
	opts ...chasm.TransitionOption,
) (newEntityRef []byte, err error) {

	// TODO: Implement concurrent poller limits
	// As Yichao mentioned: "I do think we should enforce some limit on the concurrent pollers"
	// Need to track and limit concurrent pollers per execution:
	// - Add a poller registration mechanism
	// - Return an error if max concurrent pollers exceeded
	// - Consider using a semaphore or similar mechanism
	// if err := e.registerPoller(entityRef); err != nil {
	//     return nil, serviceerror.NewResourceExhausted("too many concurrent pollers for execution")
	// }
	// defer e.unregisterPoller(entityRef)

	// if operationFn != nil {
	// 	return nil, fmt.Errorf("PollComponent operationFn not supported (TODO: remove from interface)")
	// }

	newEntityRef, shardContext, executionLease, err := e.getExecutionLeaseAndCheckPredicate(ctx, entityRef, predicateFn)
	if err != nil {
		// TODO: Distinguish between stale reference and stale state errors
		// As per transcript:
		// - Stale reference: Failed precondition, client needs to update its view, not retryable
		// - Stale state: Targeting wrong shard owner, potentially retryable
		//
		// var staleRefErr *consts.ErrStaleReference
		// if errors.As(err, &staleRefErr) {
		//     // This is a failed precondition - return specific error
		//     executionLease.GetReleaseFn()(nil)
		//     return nil, serviceerror.NewFailedPrecondition("stale reference: %v", err)
		// }
		//
		// var staleStateErr *consts.ErrStaleState
		// if errors.As(err, &staleStateErr) {
		//     // Could retry with reload, but for now just fail
		//     executionLease.GetReleaseFn()(nil)
		//     return nil, serviceerror.NewUnavailable("stale state: %v", err)
		// }

		executionLease.GetReleaseFn()(nil)
		return nil, err
	}
	if newEntityRef != nil {
		executionLease.GetReleaseFn()(nil)
		return newEntityRef, nil
	}

	// Wait condition not met, need to long-poll

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		executionLease.GetReleaseFn()(nil)
		return nil, err
	}

	// TODO: make public watch method
	historyEngine, ok := engine.(*historyEngineImpl)
	if !ok {
		executionLease.GetReleaseFn()(nil)
		return nil, serviceerror.NewInternalf("unexpected engine type: %T", engine)
	}

	workflowKey := definition.NewWorkflowKey(
		entityRef.EntityKey.NamespaceID,
		entityRef.EntityKey.BusinessID,
		entityRef.EntityKey.EntityID,
	)

	// See e.g. get_workflow_util.go:131-134
	subscriberID, channel, err := historyEngine.eventNotifier.WatchHistoryEvent(workflowKey)
	if err != nil {
		executionLease.GetReleaseFn()(nil)
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "📡 Subscribed (ID: %s) for %s/%s\n",
		subscriberID[:8], workflowKey.NamespaceID[:8], workflowKey.WorkflowID)
	defer func() {
		_ = historyEngine.eventNotifier.UnwatchHistoryEvent(workflowKey, subscriberID)
	}()

	executionLease.GetReleaseFn()(nil)

	// Set up long-poll timeout
	// See get_workflow_util.go:185-193
	namespaceRegistry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(entityRef.EntityKey.NamespaceID),
	)
	if err != nil {
		return nil, err
	}

	// See get_workflow_util.go:27
	const longPollSoftTimeout = time.Second
	longPollInterval := shardContext.GetConfig().LongPollExpirationInterval(namespaceRegistry.Name().String())
	longPollCtx, cancel := context.WithTimeout(ctx, longPollInterval-longPollSoftTimeout)
	defer cancel()

	// Long-polling loop
	// See get_workflow_util.go:195-252
	for {
		select {
		// TODO: make use of data sent w/ notification
		case notification := <-channel:
			if !isChasmNotification(notification) {
				continue
			}
			fmt.Fprintf(os.Stderr, "⬇️ Received notification (subscriber: %s)\n", subscriberID[:8])

			// TODO: Notification staleness check (see get_workflow_util.go:220-222)
			// The notification might be out of date if sent before our initial check.
			// Need to compare notification's versioned transition with our expected state:
			//
			// if notification.TransitionHistory != nil && len(notification.TransitionHistory) > 0 {
			//     notificationVT := notification.TransitionHistory[len(notification.TransitionHistory)-1]
			//     if notificationVT.GetTransitionCount() < entityRef.entityLastUpdateVT.GetTransitionCount() {
			//         // Notification is stale, ignore it
			//         continue
			//     }
			// }

			// Received a notification. Re-acquire the lock and check the predicate.
			newEntityRef, _, executionLease, err := e.getExecutionLeaseAndCheckPredicate(ctx, entityRef, predicateFn)

			if err != nil {
				// TODO: Handle different error types
				// Common reasons for failing to acquire lock:
				// 1. Shard ownership lost (shard movement)
				// 2. Entity deleted/completed
				// 3. Stale reference
				// 4. Context workflow not found
				//
				// Need to check error type and potentially:
				// - Return ShardOwnershipLost error for retry by frontend
				// - Return NotFound if entity no longer exists
				// - Return FailedPrecondition for stale reference

				executionLease.GetReleaseFn()(nil)
				return nil, err
			}
			if newEntityRef != nil {
				executionLease.GetReleaseFn()(nil)
				return newEntityRef, nil
			}

			// TODO: Additional staleness check after re-acquiring lock
			// Compare the new entityRef's versioned transition with what we started with
			// to ensure we haven't gone backwards (e.g., due to shard movement and reload)
			//
			// deserializedNewRef, _ := chasm.DeserializeComponentRef(newEntityRef)
			// if deserializedNewRef.entityLastUpdateVT.GetTransitionCount() < entityRef.entityLastUpdateVT.GetTransitionCount() {
			//     // We've gone backwards - this shouldn't happen
			//     executionLease.GetReleaseFn()(nil)
			//     return nil, serviceerror.NewInternal("state regression detected")
			// }

			// Condition still not met, release lock and continue polling
			executionLease.GetReleaseFn()(nil)

		case <-longPollCtx.Done():
			// TODO: or return empty response?
			return nil, serviceerror.NewDeadlineExceeded("long-poll timed out")
		}
	}
}

func isChasmNotification(notification *events.Notification) bool {
	// TODO: implement proper way for chasm components to ignore notifications sent by NotifyNewHistorySnapshotEvent
	// HACK:
	if notification.WorkflowState == enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED && notification.WorkflowStatus == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		return true
	}
	return false
}

// getExecutionLeaseAndCheckPredicate is a helper function for PollComponent. It uses
// getExecutionLease to read the component data and acquire the locked lease (with consistency
// assertions) and then evaluates predicateFn. It returns the locked lease together with shard
// context; if the predicateFn is satisfied it also returns a serialized ref to the component data.
func (e *ChasmEngine) getExecutionLeaseAndCheckPredicate(
	ctx context.Context,
	entityRef chasm.ComponentRef,
	predicateFn func(chasm.Context, chasm.Component) (any, bool, error),
) ([]byte, historyi.ShardContext, api.WorkflowLease, error) {

	fmt.Println("🔍 Evaluating predicate")

	// Obtain chasm tree with lock
	// cf. service/history/api/get_workflow_util.go:60-68 (GetMutableStateWithConsistencyCheck)
	// cf. get_workflow_util.go:137-144 (state reloaded after notification)

	// TODO: As discussed in the transcript, we need to use getExecutionLease which
	// already does consistency checks (see chasm_engine.go:550-604).
	// However, we should enhance it to:
	// 1. Check version transition staleness (like GetWorkflowLeaseWithConsistencyCheck)
	// 2. Potentially reload state if stale
	// 3. Distinguish between stale reference and stale state
	//
	// The current getExecutionLease already has some of this logic:
	// - It checks if the ref is stale using chasmTree.IsStale(ref)
	// - It distinguishes between ErrStaleState (reload needed) and stale reference
	// But we may need to enhance it further for long-polling scenarios

	shardContext, executionLease, err := e.getExecutionLease(ctx, entityRef)
	if err != nil {
		return nil, nil, nil, err
	}

	chasmTree, ok := executionLease.GetMutableState().ChasmTree().(*chasm.Node)
	if !ok {
		fmt.Println("  🌈 error: invalid CHASM tree")
		return nil, nil, nil, serviceerror.NewInternalf(
			"invalid CHASM tree, encountered type: %T, expected type: %T",
			executionLease.GetMutableState().ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewContext(ctx, chasmTree)
	component, err := chasmTree.Component(chasmContext, entityRef)
	if err != nil {
		return nil, nil, nil, err
	}

	_, predicateSatisfied, err := predicateFn(chasmContext, component)
	if err != nil {
		return nil, nil, nil, err
	}

	if predicateSatisfied {
		newEntityRef, err := chasmContext.Ref(component)
		if err != nil {
			return nil, nil, nil, serviceerror.NewInternalf("componentRef: %+v: %s", entityRef, err)
		}
		fmt.Fprintf(os.Stderr, "  ✅ Predicate satisfied - returning immediately\n")
		return newEntityRef, shardContext, executionLease, nil
	}

	fmt.Fprintf(os.Stderr, "  🕰️ Predicate not satisfied - entering long-poll\n")
	return nil, shardContext, executionLease, nil
}

func (e *ChasmEngine) constructTransitionOptions(
	opts ...chasm.TransitionOption,
) chasm.TransitionOptions {
	options := defaultTransitionOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.RequestID == "" {
		options.RequestID = primitives.NewUUID().String()
	}
	return options
}

func (e *ChasmEngine) lockCurrentEntity(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	businessID string,
) (historyi.ReleaseWorkflowContextFunc, error) {
	currentEntityReleaseFn, err := e.entityCache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		shardContext,
		namespaceID,
		businessID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}

	return currentEntityReleaseFn, nil
}

func (e *ChasmEngine) createNewEntity(
	ctx context.Context,
	shardContext historyi.ShardContext,
	entityRef chasm.ComponentRef,
	newFn func(chasm.MutableContext) (chasm.Component, error),
	options chasm.TransitionOptions,
) (newEntityParams, error) {
	entityRef.EntityID = primitives.NewUUID().String()

	entityKey := entityRef.EntityKey
	nsRegistry := shardContext.GetNamespaceRegistry()
	nsEntry, err := nsRegistry.GetNamespaceByID(namespace.ID(entityKey.NamespaceID))
	if err != nil {
		return newEntityParams{}, err
	}

	mutableState := workflow.NewMutableState(
		shardContext,
		shardContext.GetEventsCache(),
		shardContext.GetLogger(),
		nsEntry,
		entityKey.BusinessID,
		entityKey.EntityID,
		shardContext.GetTimeSource().Now(),
	)
	mutableState.AttachRequestID(options.RequestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, 0)

	chasmTree, ok := mutableState.ChasmTree().(*chasm.Node)
	if !ok {
		return newEntityParams{}, serviceerror.NewInternalf(
			"CHASM tree implementation not properly wired up, encountered type: %T, expected type: %T",
			mutableState.ChasmTree(),
			&chasm.Node{},
		)
	}

	chasmContext := chasm.NewMutableContext(ctx, chasmTree)
	rootComponent, err := newFn(chasmContext)
	if err != nil {
		return newEntityParams{}, err
	}
	chasmTree.SetRootComponent(rootComponent)

	snapshot, events, err := mutableState.CloseTransactionAsSnapshot(historyi.TransactionPolicyActive)
	if err != nil {
		return newEntityParams{}, err
	}
	if len(events) != 0 {
		return newEntityParams{}, serviceerror.NewInternal(
			fmt.Sprintf("CHASM framework does not support events yet, found events for new run: %v", events),
		)
	}

	return newEntityParams{
		entityRef: entityRef,
		entityContext: workflow.NewContext(
			e.config,
			definition.NewWorkflowKey(
				entityKey.NamespaceID,
				entityKey.BusinessID,
				entityKey.EntityID,
			),
			shardContext.GetLogger(),
			shardContext.GetThrottledLogger(),
			shardContext.GetMetricsHandler(),
		),
		mutableState: mutableState,
		snapshot:     snapshot,
		events:       events,
	}, nil
}

func (e *ChasmEngine) persistAsBrandNew(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
) (currentRunInfo, bool, error) {
	err := newEntityParams.entityContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeBrandNew,
		"", // previousRunID
		0,  // prevlastWriteVersion
		newEntityParams.mutableState,
		newEntityParams.snapshot,
		newEntityParams.events,
	)
	if err == nil {
		// TODO(dan): send notification on creation?
		return currentRunInfo{}, false, nil
	}

	var currentRunConditionFailedError *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &currentRunConditionFailedError) ||
		len(currentRunConditionFailedError.RunID) == 0 {
		return currentRunInfo{}, false, err
	}

	createRequestID := ""
	for requestID, info := range currentRunConditionFailedError.RequestIDs {
		if info.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			createRequestID = requestID
		}
	}
	return currentRunInfo{
		createRequestID:                     createRequestID,
		CurrentWorkflowConditionFailedError: currentRunConditionFailedError,
	}, true, nil
}

func (e *ChasmEngine) handleEntityConflict(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	options chasm.TransitionOptions,
) (chasm.EntityKey, []byte, error) {
	// Check if this a retired request using requestID.
	if _, ok := currentRunInfo.RequestIDs[options.RequestID]; ok {
		newEntityParams.entityRef.EntityID = currentRunInfo.RunID
		serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
		if err != nil {
			return chasm.EntityKey{}, nil, err
		}
		return newEntityParams.entityRef.EntityKey, serializedRef, nil
	}

	// Verify failover version and make sure it won't go backwards even if the case of split brain.
	mutableState := newEntityParams.mutableState
	nsEntry := mutableState.GetNamespaceEntry()
	if mutableState.GetCurrentVersion() < currentRunInfo.LastWriteVersion {
		clusterMetadata := shardContext.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(
			nsEntry.IsGlobalNamespace(),
			currentRunInfo.LastWriteVersion,
		)
		return chasm.EntityKey{}, nil, serviceerror.NewNamespaceNotActive(
			nsEntry.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}

	switch currentRunInfo.State {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return e.handleConflictPolicy(ctx, shardContext, newEntityParams, currentRunInfo, options.ConflictPolicy)
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return e.handleReusePolicy(ctx, shardContext, newEntityParams, currentRunInfo, options.ReusePolicy)
	default:
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unexpected current run state when creating new entity: %v", currentRunInfo.State),
		)
	}
}

func (e *ChasmEngine) handleConflictPolicy(
	_ context.Context,
	_ historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	conflictPolicy chasm.BusinessIDConflictPolicy,
) (chasm.EntityKey, []byte, error) {
	switch conflictPolicy {
	case chasm.BusinessIDConflictPolicyFail:
		return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf(
				"CHASM execution still running. BusinessID: %s, RunID: %s, ID Conflict Policy: %v",
				newEntityParams.entityRef.EntityKey.BusinessID,
				currentRunInfo.RunID,
				conflictPolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	case chasm.BusinessIDConflictPolicyTermiateExisting:
		// TODO: handle BusinessIDConflictPolicyTermiateExisting
		return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("ID Conflict Policy Terminate Existing is not yet supported")
	// case chasm.BusinessIDConflictPolicyUseExisting:
	// 	return chasm.EntityKey{}, nil, serviceerror.NewUnimplemented("ID Conflict Policy Use Existing is not yet supported")
	default:
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID conflict policy for newEntity: %v", conflictPolicy),
		)
	}
}

func (e *ChasmEngine) handleReusePolicy(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newEntityParams newEntityParams,
	currentRunInfo currentRunInfo,
	reusePolicy chasm.BusinessIDReusePolicy,
) (chasm.EntityKey, []byte, error) {
	switch reusePolicy {
	case chasm.BusinessIDReusePolicyAllowDuplicate:
		// No more check needed.
		// Fallthrough to persist the new entity as current run.
	case chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly:
		if _, ok := consts.FailedWorkflowStatuses[currentRunInfo.Status]; !ok {
			return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
				fmt.Sprintf(
					"CHASM execution already completed successfully. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
					newEntityParams.entityRef.EntityKey.BusinessID,
					currentRunInfo.RunID,
					reusePolicy,
				),
				currentRunInfo.createRequestID,
				currentRunInfo.RunID,
			)
		}
		// Fallthrough to persist the new entity as current run.
	case chasm.BusinessIDReusePolicyRejectDuplicate:
		return chasm.EntityKey{}, nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			fmt.Sprintf(
				"CHASM execution already finished. BusinessID: %s, RunID: %s, ID Reuse Policy: %v",
				newEntityParams.entityRef.EntityKey.BusinessID,
				currentRunInfo.RunID,
				reusePolicy,
			),
			currentRunInfo.createRequestID,
			currentRunInfo.RunID,
		)
	default:
		return chasm.EntityKey{}, nil, serviceerror.NewInternal(
			fmt.Sprintf("unknown business ID reuse policy for newEntity: %v", reusePolicy),
		)
	}

	err := newEntityParams.entityContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunInfo.RunID,
		currentRunInfo.LastWriteVersion,
		newEntityParams.mutableState,
		newEntityParams.snapshot,
		newEntityParams.events,
	)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	// TODO(dan): send notification on creation?

	serializedRef, err := newEntityParams.entityRef.Serialize(e.registry)
	if err != nil {
		return chasm.EntityKey{}, nil, err
	}
	return newEntityParams.entityRef.EntityKey, serializedRef, nil
}

func (e *ChasmEngine) getShardContext(
	ref chasm.ComponentRef,
) (historyi.ShardContext, error) {
	shardingKey, err := ref.ShardingKey(e.registry)
	if err != nil {
		return nil, err
	}
	shardID := common.ShardingKeyToShard(
		shardingKey,
		e.config.NumberOfShards,
	)

	return e.shardController.GetShardByID(shardID)
}

func (e *ChasmEngine) getExecutionLease(
	ctx context.Context,
	ref chasm.ComponentRef,
) (historyi.ShardContext, api.WorkflowLease, error) {
	shardContext, err := e.getShardContext(ref)
	if err != nil {
		return nil, nil, err
	}

	// TODO: Check shard ownership before proceeding
	// As discussed, shard ownership can be lost due to shard movement.
	// Need to verify this shard still owns the entity:
	//
	// if !shardContext.GetShardOwnership().IsValid() {
	//     return nil, nil, consts.ErrShardOwnershipLost
	// }

	consistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		e.entityCache,
	)

	lockPriority := locks.PriorityHigh
	callerType := headers.GetCallerInfo(ctx).CallerType
	if callerType == headers.CallerTypeBackgroundHigh || callerType == headers.CallerTypeBackgroundLow || callerType == headers.CallerTypePreemptable {
		lockPriority = locks.PriorityLow
	}

	archetype, err := ref.Archetype(e.registry)
	if err != nil {
		return nil, nil, err
	}

	var staleReferenceErr error
	entityLease, err := consistencyChecker.GetChasmLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState historyi.MutableState) bool {
			// This predicate function performs staleness checks as described by Yichao.
			// It returns false if state needs to be reloaded, true if state is acceptable.

			// First check: Is the component ref stale against current tree?
			err := mutableState.ChasmTree().IsStale(ref)
			if errors.Is(err, consts.ErrStaleState) {
				// State is stale and needs reload (returns false to trigger reload)
				// This happens when versioned transition has moved forward
				return false
			}

			// Reference itself might be stale (version has gone backwards or branch changed).
			// No need to reload mutable state in this case, but request should be failed.
			staleReferenceErr = err

			// TODO: Additional version transition checks as per transcript
			// Check that ref.entityLastUpdateVT is on the current transition history:
			//
			// if ref.entityLastUpdateVT != nil {
			//     transitionHistory := mutableState.GetExecutionInfo().GetTransitionHistory()
			//     if !transitionhistory.ContainsVersionedTransition(transitionHistory, ref.entityLastUpdateVT) {
			//         // The reference points to a non-current branch
			//         staleReferenceErr = serviceerror.NewFailedPrecondition(
			//             "version transition not on current branch")
			//         return true // Don't reload, just fail
			//     }
			// }

			return true
		},
		definition.NewWorkflowKey(
			ref.EntityKey.NamespaceID,
			ref.EntityKey.BusinessID,
			ref.EntityKey.EntityID,
		),
		archetype,
		lockPriority,
	)

	// TODO: After acquiring lease, verify shard ownership again
	// Shard could have moved between initial check and lock acquisition:
	//
	// if entityLease != nil && !shardContext.GetShardOwnership().IsValid() {
	//     entityLease.GetReleaseFn()(nil)
	//     return nil, nil, consts.ErrShardOwnershipLost
	// }

	if err == nil && staleReferenceErr != nil {
		entityLease.GetReleaseFn()(nil)
		err = staleReferenceErr
	}

	return shardContext, entityLease, err
}

// notifyChasmComponentUpdate sends a notification when a CHASM component has been updated.
func notifyChasmComponentUpdate(
	shardContext historyi.ShardContext,
	entityRef chasm.ComponentRef,
) error {
	engine, err := shardContext.GetEngine(context.Background())
	if err != nil {
		return err
	}

	// TODO: Include proper versioned transition information in notification
	// As discussed in the transcript, notifications should include transition history
	// so subscribers can check for staleness without re-acquiring the lock.
	//
	// Currently we're sending minimal info, but should include:
	// 1. The current versioned transition from entityRef.entityLastUpdateVT
	// 2. Potentially the full transition history for staleness checks
	//
	// Example of what should be sent:
	// transitionHistory := []*persistencespb.VersionedTransition{
	//     entityRef.entityLastUpdateVT,
	// }
	//
	// Also consider including namespace failover version and other metadata
	// that would help with staleness detection on the receiver side.

	// For now we do not send full information; the subscriber must read the
	// component data again. This is a temporary hack using special values
	// to distinguish CHASM notifications.
	engine.NotifyNewHistoryEvent(events.NewNotification(
		entityRef.NamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: entityRef.BusinessID,
			RunId:      entityRef.EntityID,
		},
		-1,
		-1,
		-1,
		-1,
		enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED, // Special marker for CHASM
		enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, // Special marker for CHASM
		&historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{},
		},
		nil, // TODO: Should include entityRef.entityLastUpdateVT here
	))
	return nil
}
