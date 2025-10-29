# Worker Liveness Tracking

## Overview

The Worker Liveness system tracks the health and availability of Temporal workers through periodic heartbeats. It detects inactive workers and reschedules their activities to maintain workflow execution continuity. The system ensures that activities are not lost when workers become unavailable due to network partitions, crashes, or other failures.

## Architecture

### Components

1. **Worker** (`worker.go`) - CHASM entity representing a worker along with persistent state
2. **State Machine** (`statemachine.go`) - Manages worker lifecycle transitions
3. **Task Executors** (`tasks.go`) - Handle lease expiry and cleanup operations
4. **RPC Handler** (`handler.go`) - Processes heartbeat requests from workers

### Key Concepts

- **Worker**: A unique instance of a worker process identified by `WorkerInstanceKey`
- **Lease**: Period for which a worker should be considered alive
- **Heartbeat**: Periodic signal from worker to extend its lease
- **Activity Binding**: Association between workers and their assigned activities

## Worker Lifecycle

### States

```
ACTIVE ──────► INACTIVE ──────► CLEANED_UP
   │              │                 │
   │              │                 │
   └──────────────┘                 │
      (resurrection)                │
                                    │
                              (terminal state)
```

#### ACTIVE
- Worker is sending heartbeats regularly
- Lease is being renewed for all activities that are currently bound to it
- Can receive and execute new activities

#### INACTIVE  
- Worker lease has expired (no heartbeats received)
- Activities are cancelled and rescheduled to other workers
- Worker can still reconnect (resurrection scenario)

#### CLEANED_UP
- Terminal state after cleanup grace period
- All the underlying state is cleaned up
- Heartbeats in this state return errors

### State Transitions

#### ACTIVE → INACTIVE (Lease Expiry)
**Trigger**: `LeaseExpiryTask` fires when lease deadline is reached
**Actions**:
- Mark worker as INACTIVE
- Schedule `WorkerCleanupTask` with configurable delay
- Create and schedule `ActivityRescheduleTask` for each bound activity
- Fan-out tasks to appropriate History service shards

#### INACTIVE → ACTIVE (Worker Resurrection)
**Trigger**: Heartbeat received from previously inactive worker
**Actions**:
- Update lease with new deadline
- Mark worker as ACTIVE
- Previous activities remain cancelled

#### INACTIVE → CLEANED_UP (Cleanup)
**Trigger**: `WorkerCleanupTask` executes after grace period
**Actions**:
- Mark worker as CLEANED_UP (terminal state)
- Clean up worker state and metadata
- Release any remaining resources

## Heartbeat Flow

### End-to-End Process

```
Worker Process ──► Frontened Service ──► History Service ──► CHASM Worker Component
      │                    │                   │                      │
      │                    │                   │                      │
   [heartbeat]         [forward]           [update]              [state machine]
      │                    │                   │                      │
      │                    │                   │                      │
      └────────────────────┴───────────────────┴──────────────────────┘
                                    [lease extended]
```

### 1. Worker Process
- Sends periodic heartbeats with `WorkerHeartbeat` containing:
  - `WorkerInstanceKey`: Unique identifier for worker process
  - `lease_duration`: Requested lease duration (e.g., 30 seconds)
  - worker metadata: Capabilities, build ID, etc.
  - `bound_activities`: List of activities newly bound to this worker
  - `unbound_activities`: List of activities no longer bound to this worker

### 2. Frontend Service
- Receives heartbeat from worker
- Forwards to History service via `RecordWorkerHeartbeat` RPC
- Handles routing and load balancing

### 3. History Service
- Processes heartbeat through CHASM handler
- Calculates lease deadline: `current_time + lease_duration`
- Updates or creates Worker component

### 4. CHASM Worker Component
- Applies appropriate state transition based on current state
- Updates activity bindings based on `bound_activities` and `unbound_activities`
- Schedules `LeaseExpiryTask` for the new deadline
- Persists updated state with current activity bindings

## Activity Association & Rescheduling

### Activity Binding
- Activities are bound/unbound through heartbeat requests
- Worker state maintains current set of bound activities
- Activity lifecycle is tied to worker state
- Binding updates are processed incrementally:
  - `bound_activities`: Added to worker's activity list
  - `unbound_activities`: Removed from worker's activity list

### Rescheduling Logic
When worker transitions to INACTIVE:
1. Retrieve all activities currently bound to the worker from state
2. Create `ActivityRescheduleTask` for each bound activity
3. Schedule tasks with fan-out to appropriate History service shards:
   - Each activity may be on a different shard based on workflow execution
   - Tasks are routed using activity's workflow execution key
4. Each `ActivityRescheduleTask` execution:
   - Sends cancellation signal to the running activity
   - Marks activity as available for rescheduling
   - Removes worker binding from activity
   - Notifies Matching service for reassignment

### Resurrection Handling
When worker resurrects (INACTIVE → ACTIVE):
- Previous activities remain cancelled/rescheduled
- Worker starts with clean slate
- New activities can be assigned immediately

### Activity States During Worker Transitions
- **Worker ACTIVE**: Activities execute normally, binding updates processed
- **Worker INACTIVE**: All bound activities are rescheduled via fan-out tasks
- **Worker CLEANED_UP**: No activities remain bound, state is cleaned up

### Fan-Out Rescheduling Architecture
The rescheduling process uses a fan-out pattern to handle activities distributed across multiple History service shards:

```
Worker INACTIVE ──► LeaseExpiryTask ──► ActivityRescheduleTask (per activity)
                                              │
                                              ├──► History Shard A (Activity 1, 3)
                                              ├──► History Shard B (Activity 2)
                                              └──► History Shard C (Activity 4, 5)
```

**Benefits**:
- Parallel processing of activity cancellations
- Proper routing to correct History service shards
- Fault isolation (failure of one activity doesn't block others)
- Scalable with number of bound activities

## Configuration

### Dynamic Configuration
- `matching.InactiveWorkerCleanupDelay`: Time to wait before cleaning up inactive workers (default: 60 minutes)

### Lease Duration
- Configurable per heartbeat request
- Default: 30 seconds (defined in client)
- Server validates and may impose bounds for security

## Error Handling

### Network Partitions
- Worker appears inactive due to connectivity loss
- Activities are rescheduled to maintain progress
- Worker can reconnect and resume with new activities

### Clock Skew
- Server calculates lease deadline to avoid client clock issues
- Consistent timing based on server's perspective

### Duplicate Workers
- Same `WorkerInstanceKey` across restarts is handled gracefully
- Previous session is effectively replaced by new session

## Monitoring & Observability

### Metrics
- Worker heartbeat frequency and latency
- Lease expiry events and timing
- Activity rescheduling counts and success rates
- Worker resurrection events and patterns
- Worker state distribution (ACTIVE/INACTIVE/CLEANED_UP)

### Logging
- Worker state transitions with `worker-id` tag
- Lease expiry and cleanup events
- Error conditions and edge cases

## Testing Strategy

### Unit Tests
- State machine transitions (`statemachine_test.go`)
- Component lifecycle (`worker_test.go`)
- Task executor behavior (`tasks_test.go`)
- RPC handler logic (`handler_test.go`)

### Integration Tests
- End-to-end heartbeat flow
- Activity rescheduling scenarios
- Network partition simulation
- Worker resurrection cases
- Load testing with multiple workers

### Chaos Testing
- Random worker failures
- Network partition scenarios
- Clock skew simulation
- Resource exhaustion tests

## Security Considerations

### Authentication
- Worker identity validation and authorization
- Prevent unauthorized heartbeats
- Secure worker registration process
- Namespace-based access control

### Resource Protection
- Rate limiting on heartbeat endpoints
- Bounds on lease duration requests
- Protection against resource exhaustion
- Monitoring for suspicious patterns

## Performance Characteristics

### Scalability
- CHASM provides horizontal scaling for worker tracking
- Efficient state storage and retrieval
- Minimal overhead per heartbeat

### Latency
- Fast heartbeat processing (< 10ms target)
- Asynchronous task scheduling
- Non-blocking state transitions

### Storage
- Compact protobuf state representation
- Efficient cleanup of terminated workers
- Configurable retention policies
