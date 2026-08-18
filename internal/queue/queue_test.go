package queue

import "testing"

func TestTaskLifecycle(t *testing.T) {
	if TaskLifecycle != "lifecycle:run" {
		t.Fatalf("task name %q", TaskLifecycle)
	}
	if TaskLifecycleRule != "lifecycle:rule" {
		t.Fatalf("rule task %q", TaskLifecycleRule)
	}
}
