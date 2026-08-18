package queue

import (
	"testing"
	"time"
)

func TestTaskLifecycle(t *testing.T) {
	if TaskLifecycle != "lifecycle:run" {
		t.Fatalf("task name %q", TaskLifecycle)
	}
}

func TestUniqueTTL(t *testing.T) {
	if got := UniqueTTL(time.Hour); got != time.Hour-time.Second {
		t.Fatalf("hour ttl %s", got)
	}
	if got := UniqueTTL(time.Second); got != time.Second {
		t.Fatalf("floor ttl %s", got)
	}
}
