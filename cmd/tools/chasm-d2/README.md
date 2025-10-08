# chasm-d2

A tool to generate D2 state machine diagrams from chasm StateMachine implementations.

## Example usage

```bash
go run -C ./cmd/tools/chasm-d2 . \
  --package go.temporal.io/server/chasm/lib/activity \
  --output /tmp/activity-state-machines
```
