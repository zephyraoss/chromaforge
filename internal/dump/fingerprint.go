package dump

import (
	"fmt"
	"math"
)

type SubFingerprint struct {
	Hash     uint32
	Position int
}

func NormalizeFingerprint(raw []int64) ([]uint32, error) {
	out := make([]uint32, 0, len(raw))
	for _, v := range raw {
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("fingerprint value %d outside int32 range", v)
		}
		out = append(out, uint32(int32(v)))
	}
	return out, nil
}

func ExtractSubFingerprints(fp []uint32) []SubFingerprint {
	subs := make([]SubFingerprint, 0, (len(fp)+7)/8)
	for i := 0; i < len(fp); i += 8 {
		subs = append(subs, SubFingerprint{Hash: fp[i], Position: i})
	}
	return subs
}
