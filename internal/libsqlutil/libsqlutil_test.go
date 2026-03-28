package libsqlutil

import "testing"

func TestLocalDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "absolute path", in: "/tmp/chromakopia.db", want: "file:/tmp/chromakopia.db"},
		{name: "relative path", in: "./chromakopia.db", want: "file:./chromakopia.db"},
		{name: "memory", in: ":memory:", want: ":memory:"},
		{name: "existing file uri", in: "file:/tmp/chromakopia.db", want: "file:/tmp/chromakopia.db"},
	}

	for _, tt := range tests {
		if got := LocalDSN(tt.in); got != tt.want {
			t.Fatalf("%s: LocalDSN(%q) = %q, want %q", tt.name, tt.in, got, tt.want)
		}
	}
}
