package queue

import "time"

const TaskLifecycle = "lifecycle:run"

// UniqueTTL is slightly shorter than the schedule interval so the next slot can enqueue.
func UniqueTTL(interval time.Duration) time.Duration {
	d := interval - time.Second
	if d < time.Second {
		return time.Second
	}
	return d
}
