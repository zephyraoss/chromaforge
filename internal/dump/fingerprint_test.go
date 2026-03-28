package dump

import "testing"

func TestNormalizeFingerprint(t *testing.T) {
	got, err := NormalizeFingerprint([]int64{-1, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != ^uint32(0) || got[2] != 1 {
		t.Fatalf("unexpected normalized fingerprint: %#v", got)
	}
}

func TestExtractSubFingerprints(t *testing.T) {
	fp := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := ExtractSubFingerprints(fp)
	if len(got) != 2 {
		t.Fatalf("got %d sub fingerprints, want 2", len(got))
	}
	if got[0].Hash != 1 || got[0].Position != 0 || got[1].Hash != 9 || got[1].Position != 8 {
		t.Fatalf("unexpected sub fingerprints: %#v", got)
	}
}
