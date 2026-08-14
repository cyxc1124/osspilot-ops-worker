package lifecycle

import "testing"

func TestJoinPrefix(t *testing.T) {
	if got := joinPrefix(".trash/", "docs/"); got != ".trash/docs/" {
		t.Fatalf("got %q", got)
	}
	if got := joinPrefix(".versions/", ""); got != ".versions/" {
		t.Fatalf("got %q", got)
	}
}

func TestLiveOnly(t *testing.T) {
	if !liveOnly("a.txt") || liveOnly(".trash/a") || liveOnly(".versions/a") {
		t.Fatal("liveOnly filter")
	}
}
