package replication

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	streamReceiverSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		highPriorityTaskTracker *MockExecutableTaskTracker
		lowPriorityTaskTracker  *MockExecutableTaskTracker
		stream                  *mockStream
		taskScheduler           *mockScheduler

		streamReceiver         *StreamReceiverImpl
		receiverFlowController *MockReceiverFlowController
	}

	mockStream struct {
		requests []*adminservice.StreamWorkflowReplicationMessagesRequest
		respChan chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]
		closed   bool
	}
	mockScheduler struct {
		tasks []TrackableExecutableTask
	}
)

func TestStreamReceiverSuite(t *testing.T) {
	s := new(streamReceiverSuite)
	suite.Run(t, s)
}

func (s *streamReceiverSuite) SetupSuite() {

}

func (s *streamReceiverSuite) TearDownSuite() {

}

func (s *streamReceiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.highPriorityTaskTracker = NewMockExecutableTaskTracker(s.controller)
	s.lowPriorityTaskTracker = NewMockExecutableTaskTracker(s.controller)
	s.stream = &mockStream{
		requests: nil,
		respChan: make(chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], 100),
	}
	s.taskScheduler = &mockScheduler{
		tasks: nil,
	}

	mockConfig := &configs.Config{
		ReplicationAllowMultiSourceShard: func() bool { return false },
	}
	processToolBox := ProcessToolBox{
		ClusterMetadata:           s.clusterMetadata,
		HighPriorityTaskScheduler: s.taskScheduler,
		LowPriorityTaskScheduler:  s.taskScheduler,
		MetricsHandler:            metrics.NoopMetricsHandler,
		Logger:                    log.NewTestLogger(),
		DLQWriter:                 NoopDLQWriter{},
		Config:                    mockConfig,
	}
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some-cluster-name").AnyTimes()
	s.streamReceiver = NewStreamReceiver(
		processToolBox,
		NewExecutableTaskConverter(processToolBox),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
	)
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(
		map[string]cluster.ClusterInformation{
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(s.streamReceiver.clientShardKey.ClusterID),
			},
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(s.streamReceiver.serverShardKey.ClusterID),
			},
		},
	).AnyTimes()
	s.streamReceiver.highPriorityTaskTracker = s.highPriorityTaskTracker
	s.streamReceiver.lowPriorityTaskTracker = s.lowPriorityTaskTracker
	s.stream.requests = []*adminservice.StreamWorkflowReplicationMessagesRequest{}
	s.receiverFlowController = NewMockReceiverFlowController(s.controller)
	s.streamReceiver.flowController = s.receiverFlowController
}

func (s *streamReceiverSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamReceiverSuite) TestAckMessage_Noop() {
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	s.streamReceiver.ackMessage(s.stream)

	s.Equal(0, len(s.stream.requests))
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeUnset() {
	s.streamReceiver.receiverMode = ReceiverModeUnset // when stream receiver is in unset mode, means no task received yet, so no ACK should be sent
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Equal(0, len(s.stream.requests))
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal([]*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     watermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(watermarkInfo.Timestamp),
			},
		},
	},
	}, s.stream.requests)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack_NoHighPriorityWatermark() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Error(err)
	s.Equal(0, len(s.stream.requests))
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack_HasBothWatermark() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Error(err)
	s.Equal(0, len(s.stream.requests))
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoHighPriorityWatermark() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Equal(0, len(s.stream.requests))
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoLowPriorityWatermark() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Equal(0, len(s.stream.requests))
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	highWatermarkInfo := &WatermarkInfo{
		Watermark: 10,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	lowWatermarkInfo := &WatermarkInfo{
		Watermark: 11,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(highWatermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(lowWatermarkInfo)
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal([]*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     highWatermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
				HighPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     highWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
				},
				LowPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     lowWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(lowWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
				},
			},
		},
	},
	}, s.stream.requests)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp
	close(s.stream.respChan)

	s.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Equal(1, len(s.taskScheduler.tasks))
	s.IsType(&ExecutableUnknownTask{}, s.taskScheduler.tasks[0])
	s.Equal(ReceiverModeSingleStack, s.streamReceiver.receiverMode)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack_ReceivedPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := s.streamReceiver.processMessages(s.stream)
	s.IsType(&StreamError{}, err)
	s.Equal(0, len(s.taskScheduler.tasks))
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack_ReceivedNonPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := s.streamReceiver.processMessages(s.stream)
	s.IsType(&StreamError{}, err)
	s.Equal(0, len(s.taskScheduler.tasks))
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp1 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	streamResp2 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{
						{
							TaskType:       enumsspb.ReplicationTaskType(-1),
							SourceTaskId:   rand.Int63(),
							VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
							Priority:       enumsspb.TASK_PRIORITY_LOW,
						},
					},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp1
	s.stream.respChan <- streamResp2
	close(s.stream.respChan)

	s.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp1.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp1.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)
	s.lowPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Equal(2, len(s.taskScheduler.tasks))
	s.Equal(ReceiverModeTieredStack, s.streamReceiver.receiverMode)
}

func (s *streamReceiverSuite) TestGetTaskScheduler() {
	high := &mockScheduler{}
	low := &mockScheduler{}
	s.streamReceiver.ProcessToolBox.HighPriorityTaskScheduler = high
	s.streamReceiver.ProcessToolBox.LowPriorityTaskScheduler = low

	tests := []struct {
		name         string
		priority     enumsspb.TaskPriority
		task         TrackableExecutableTask
		expected     ctasks.Scheduler[TrackableExecutableTask]
		expectErr    bool
		errorMessage string
	}{
		{
			name:     "Unspecified priority with ExecutableWorkflowStateTask",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableWorkflowStateTask{},
			expected: low,
		},
		{
			name:     "Unspecified priority with other task",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableHistoryTask{},
			expected: high,
		},
		{
			name:     "High priority",
			priority: enumsspb.TASK_PRIORITY_HIGH,
			task:     &ExecutableHistoryTask{},
			expected: high,
		},
		{
			name:     "Low priority",
			priority: enumsspb.TASK_PRIORITY_LOW,
			task:     &ExecutableWorkflowStateTask{},
			expected: low,
		},
		{
			name:         "Invalid priority",
			priority:     enumsspb.TaskPriority(999),
			task:         &ExecutableHistoryTask{},
			expectErr:    true,
			errorMessage: "InvalidArgument",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			scheduler, err := s.streamReceiver.getTaskScheduler(tt.priority, tt.task)
			if tt.expectErr {
				s.Error(err)
			} else {
				s.NoError(err)
				s.True(scheduler == tt.expected, "Expected scheduler to match")
			}
		})
	}
}

func (s *streamReceiverSuite) TestProcessMessage_Err() {
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: nil,
		Err:  serviceerror.NewUnavailable("random recv error"),
	}
	s.stream.respChan <- streamResp
	close(s.stream.respChan)

	err := s.streamReceiver.processMessages(s.stream)
	s.Error(err)
}

func (s *streamReceiverSuite) TestSendEventLoop_Panic_Captured() {
	s.streamReceiver.sendEventLoop() // should not cause panic
}

func (s *streamReceiverSuite) TestRecvEventLoop_Panic_Captured() {
	s.streamReceiver.recvEventLoop() // should not cause panic
}

func (s *streamReceiverSuite) TestLivenessMonitor() {
	livenessMonitor(
		s.streamReceiver.recvSignalChan,
		time.Millisecond,
		s.streamReceiver.shutdownChan,
		s.streamReceiver.Stop,
		s.streamReceiver.logger,
	)
	s.False(s.streamReceiver.IsValid())
}

func (s *mockStream) Send(
	req *adminservice.StreamWorkflowReplicationMessagesRequest,
) error {
	s.requests = append(s.requests, req)
	return nil
}

func (s *mockStream) Recv() (<-chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	return s.respChan, nil
}

func (s *mockStream) Close() {
	s.closed = true
}

func (s *mockStream) IsValid() bool {
	return !s.closed
}

func (s *mockScheduler) Submit(task TrackableExecutableTask) {
	s.tasks = append(s.tasks, task)
}

func (s *mockScheduler) TrySubmit(task TrackableExecutableTask) bool {
	s.tasks = append(s.tasks, task)
	return true
}

func (s *mockScheduler) Start() {}
func (s *mockScheduler) Stop()  {}

// TestMultiSourceShard_FeatureFlagDisabled tests that when the feature flag is disabled,
// behavior remains the same as before (legacy mode)
func (s *streamReceiverSuite) TestMultiSourceShard_FeatureFlagDisabled() {
	// Set the config to return false for multi-source shard (it's already false by default)
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal(1, len(s.stream.requests))
}

// TestMultiSourceShard_GetAggregatedWatermark tests the aggregation logic for watermarks
func (s *streamReceiverSuite) TestMultiSourceShard_GetAggregatedWatermark() {
	// Create mock trackers with different watermarks
	tracker1 := NewMockExecutableTaskTracker(s.controller)
	tracker2 := NewMockExecutableTaskTracker(s.controller)
	tracker3 := NewMockExecutableTaskTracker(s.controller)

	watermark1 := &WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	watermark2 := &WatermarkInfo{Watermark: 50, Timestamp: time.Now()} // This should be the minimum
	watermark3 := &WatermarkInfo{Watermark: 200, Timestamp: time.Now()}

	tracker1.EXPECT().LowWatermark().Return(watermark1)
	tracker2.EXPECT().LowWatermark().Return(watermark2)
	tracker3.EXPECT().LowWatermark().Return(watermark3)

	trackers := map[int32]ExecutableTaskTracker{
		1: tracker1,
		2: tracker2,
		3: tracker3,
	}

	result := s.streamReceiver.getAggregatedWatermark(trackers)
	s.NotNil(result)
	s.Equal(int64(50), result.Watermark) // Should return the minimum watermark
}

// TestMultiSourceShard_GetAggregatedWatermark_WithNilWatermarks tests aggregation when some trackers have nil watermarks
func (s *streamReceiverSuite) TestMultiSourceShard_GetAggregatedWatermark_WithNilWatermarks() {
	tracker1 := NewMockExecutableTaskTracker(s.controller)
	tracker2 := NewMockExecutableTaskTracker(s.controller)
	tracker3 := NewMockExecutableTaskTracker(s.controller)

	watermark1 := &WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	watermark3 := &WatermarkInfo{Watermark: 200, Timestamp: time.Now()}

	tracker1.EXPECT().LowWatermark().Return(watermark1)
	tracker2.EXPECT().LowWatermark().Return(nil) // This tracker has no watermark
	tracker3.EXPECT().LowWatermark().Return(watermark3)

	trackers := map[int32]ExecutableTaskTracker{
		1: tracker1,
		2: tracker2,
		3: tracker3,
	}

	result := s.streamReceiver.getAggregatedWatermark(trackers)
	s.NotNil(result)
	s.Equal(int64(100), result.Watermark) // Should return the minimum non-nil watermark
}

// TestMultiSourceShard_GetAggregatedWatermark_EmptyTrackers tests aggregation with empty tracker map
func (s *streamReceiverSuite) TestMultiSourceShard_GetAggregatedWatermark_EmptyTrackers() {
	trackers := map[int32]ExecutableTaskTracker{}
	result := s.streamReceiver.getAggregatedWatermark(trackers)
	s.Nil(result)
}

// TestMultiSourceShard_GetOrCreateTaskTracker tests task tracker creation per source shard
func (s *streamReceiverSuite) TestMultiSourceShard_GetOrCreateTaskTracker() {
	// Set the config to return true for multi-source shard
	s.streamReceiver.Config.ReplicationAllowMultiSourceShard = func() bool { return true }

	// First call should create a new tracker
	tracker1, err := s.streamReceiver.getOrCreateTaskTrackerForSourceShard(1, enumsspb.TASK_PRIORITY_HIGH)
	s.NoError(err)
	s.NotNil(tracker1)

	// Second call with same source shard should return the same tracker
	tracker2, err := s.streamReceiver.getOrCreateTaskTrackerForSourceShard(1, enumsspb.TASK_PRIORITY_HIGH)
	s.NoError(err)
	s.Equal(tracker1, tracker2)

	// Verify tracker was stored in the map
	s.Len(s.streamReceiver.multiShardHighPriorityTaskTrackers, 1)
	s.Equal(tracker1, s.streamReceiver.multiShardHighPriorityTaskTrackers[1])
}

// TestMultiSourceShard_GetOrCreateTaskTracker_LegacyMode tests fallback to legacy behavior when feature flag is disabled
func (s *streamReceiverSuite) TestMultiSourceShard_GetOrCreateTaskTracker_LegacyMode() {
	// Config is already set to return false for multi-source shard by default
	tracker, err := s.streamReceiver.getOrCreateTaskTrackerForSourceShard(1, enumsspb.TASK_PRIORITY_HIGH)
	s.NoError(err)
	s.Equal(s.streamReceiver.highPriorityTaskTracker, tracker) // Should return the legacy tracker
}

// TestMultiSourceShard_AckMessage_WithSourceShardStates tests that source shard states are populated when feature flag is enabled
func (s *streamReceiverSuite) TestMultiSourceShard_AckMessage_WithSourceShardStates() {
	// Enable multi-source shard feature
	s.streamReceiver.Config.ReplicationAllowMultiSourceShard = func() bool { return true }
	s.streamReceiver.receiverMode = ReceiverModeSingleStack

	// Create mock trackers for different source shards
	tracker1 := NewMockExecutableTaskTracker(s.controller)
	tracker2 := NewMockExecutableTaskTracker(s.controller)

	watermark1 := &WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	watermark2 := &WatermarkInfo{Watermark: 200, Timestamp: time.Now()}

	tracker1.EXPECT().LowWatermark().Return(watermark1).AnyTimes()
	tracker2.EXPECT().LowWatermark().Return(watermark2).AnyTimes()
	tracker1.EXPECT().Size().Return(5).AnyTimes()
	tracker2.EXPECT().Size().Return(3).AnyTimes()

	// Set up multi-shard trackers
	s.streamReceiver.multiShardHighPriorityTaskTrackers[1] = tracker1
	s.streamReceiver.multiShardHighPriorityTaskTrackers[2] = tracker2

	// Mock flow controller
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME).Times(2)

	// Call ackMessage
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)

	// Verify request was sent
	s.Equal(1, len(s.stream.requests))

	// Verify source shard states are populated
	syncState := s.stream.requests[0].GetSyncReplicationState()
	s.NotNil(syncState)
	s.NotNil(syncState.SourceShardStates)

	// Verify we have states for both source shards
	s.Len(syncState.SourceShardStates, 2)

	// Verify individual source shard states
	shardState1 := syncState.SourceShardStates[1]
	s.NotNil(shardState1)
	s.NotNil(shardState1.HighPriorityState)
	s.Equal(int64(100), shardState1.HighPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, shardState1.HighPriorityState.FlowControlCommand)
	s.Nil(shardState1.LowPriorityState) // No low priority tracker was set up

	shardState2 := syncState.SourceShardStates[2]
	s.NotNil(shardState2)
	s.NotNil(shardState2.HighPriorityState)
	s.Equal(int64(200), shardState2.HighPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, shardState2.HighPriorityState.FlowControlCommand)
	s.Nil(shardState2.LowPriorityState) // No low priority tracker was set up
}

// TestMultiSourceShard_AckMessage_LegacyMode_NoSourceShardStates tests that source shard states are not populated when feature flag is disabled
func (s *streamReceiverSuite) TestMultiSourceShard_AckMessage_LegacyMode_NoSourceShardStates() {
	// Feature flag is disabled by default
	s.streamReceiver.receiverMode = ReceiverModeSingleStack

	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)

	// Verify request was sent
	s.Equal(1, len(s.stream.requests))

	// Verify source shard states are NOT populated
	syncState := s.stream.requests[0].GetSyncReplicationState()
	s.NotNil(syncState)
	s.Nil(syncState.SourceShardStates) // Should be nil when feature flag is disabled
}

// TestMultiSourceShard_AckMessage_WithBothPriorityStates tests both high and low priority states
func (s *streamReceiverSuite) TestMultiSourceShard_AckMessage_WithBothPriorityStates() {
	// Enable multi-source shard feature
	s.streamReceiver.Config.ReplicationAllowMultiSourceShard = func() bool { return true }
	s.streamReceiver.receiverMode = ReceiverModeTieredStack // Use tiered stack for both priorities

	// Create mock trackers for different source shards and priorities
	highTracker1 := NewMockExecutableTaskTracker(s.controller)
	highTracker2 := NewMockExecutableTaskTracker(s.controller)
	lowTracker1 := NewMockExecutableTaskTracker(s.controller)
	lowTracker3 := NewMockExecutableTaskTracker(s.controller) // different shard for low priority

	highWatermark1 := &WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	highWatermark2 := &WatermarkInfo{Watermark: 150, Timestamp: time.Now()}
	lowWatermark1 := &WatermarkInfo{Watermark: 80, Timestamp: time.Now()}
	lowWatermark3 := &WatermarkInfo{Watermark: 90, Timestamp: time.Now()}

	highTracker1.EXPECT().LowWatermark().Return(highWatermark1).AnyTimes()
	highTracker2.EXPECT().LowWatermark().Return(highWatermark2).AnyTimes()
	lowTracker1.EXPECT().LowWatermark().Return(lowWatermark1).AnyTimes()
	lowTracker3.EXPECT().LowWatermark().Return(lowWatermark3).AnyTimes()

	// Set up size expectations for aggregated watermark calculation
	highTracker1.EXPECT().Size().Return(2).AnyTimes()
	highTracker2.EXPECT().Size().Return(3).AnyTimes()
	lowTracker1.EXPECT().Size().Return(1).AnyTimes()
	lowTracker3.EXPECT().Size().Return(4).AnyTimes()

	// Set up multi-shard trackers
	s.streamReceiver.multiShardHighPriorityTaskTrackers[1] = highTracker1
	s.streamReceiver.multiShardHighPriorityTaskTrackers[2] = highTracker2
	s.streamReceiver.multiShardLowPriorityTaskTrackers[1] = lowTracker1 // same shard as high priority
	s.streamReceiver.multiShardLowPriorityTaskTrackers[3] = lowTracker3 // different shard

	// Mock flow controller - for both per-source-shard states and original tiered stack logging
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME).AnyTimes()
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE).AnyTimes()

	// The original task trackers might still be used for logging in tiered stack mode
	s.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()

	// Also need LowWatermark expectations for potential aggregated watermark calculations
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(&WatermarkInfo{Watermark: 50, Timestamp: time.Now()}).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(&WatermarkInfo{Watermark: 40, Timestamp: time.Now()}).AnyTimes()

	// Call ackMessage
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)

	// Verify request was sent
	s.Equal(1, len(s.stream.requests))

	// Verify source shard states are populated
	syncState := s.stream.requests[0].GetSyncReplicationState()
	s.NotNil(syncState)
	s.NotNil(syncState.SourceShardStates)

	// Verify we have states for all shards (1, 2, 3)
	s.Len(syncState.SourceShardStates, 3)

	// Verify shard 1 (has both high and low priority)
	shardState1 := syncState.SourceShardStates[1]
	s.NotNil(shardState1)
	s.NotNil(shardState1.HighPriorityState)
	s.Equal(int64(100), shardState1.HighPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, shardState1.HighPriorityState.FlowControlCommand)
	s.NotNil(shardState1.LowPriorityState)
	s.Equal(int64(80), shardState1.LowPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE, shardState1.LowPriorityState.FlowControlCommand)

	// Verify shard 2 (has only high priority)
	shardState2 := syncState.SourceShardStates[2]
	s.NotNil(shardState2)
	s.NotNil(shardState2.HighPriorityState)
	s.Equal(int64(150), shardState2.HighPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME, shardState2.HighPriorityState.FlowControlCommand)
	s.Nil(shardState2.LowPriorityState) // No low priority tracker for shard 2

	// Verify shard 3 (has only low priority)
	shardState3 := syncState.SourceShardStates[3]
	s.NotNil(shardState3)
	s.Nil(shardState3.HighPriorityState) // No high priority tracker for shard 3
	s.NotNil(shardState3.LowPriorityState)
	s.Equal(int64(90), shardState3.LowPriorityState.InclusiveLowWatermark)
	s.Equal(enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE, shardState3.LowPriorityState.FlowControlCommand)
}
