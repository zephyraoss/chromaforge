package build

import "testing"

func TestBuildSubFingerprintInsert(t *testing.T) {
	query, args := buildSubFingerprintInsert(42, []SubFP{
		{Hash: 11, Position: 0},
		{Hash: 22, Position: 8},
	})

	wantQuery := "INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES (?,?,?),(?,?,?)"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 6 {
		t.Fatalf("len(args) = %d, want 6", len(args))
	}
}
