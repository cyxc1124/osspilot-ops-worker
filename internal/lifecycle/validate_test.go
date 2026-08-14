package lifecycle

import "testing"

func TestRequireAction(t *testing.T) {
	if err := requireAction(nil, nil, nil, nil); err == nil {
		t.Fatal("expected error")
	}
	n := 7
	if err := requireAction(&n, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := validDays(&n); err != nil {
		t.Fatal(err)
	}
	bad := 0
	if err := validDays(&bad); err == nil {
		t.Fatal("expected error")
	}
}
