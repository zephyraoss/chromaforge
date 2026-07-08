package build

import (
	"github.com/google/uuid"
	chroma "github.com/zephyraoss/libchroma"
)

type syncState struct {
	fpTrack   map[uint32]uint32
	trackFPs  map[uint32][]uint32
	trackMBID map[uint32]uuid.UUID
}

func newSyncState() *syncState {
	return &syncState{
		fpTrack:   map[uint32]uint32{},
		trackFPs:  map[uint32][]uint32{},
		trackMBID: map[uint32]uuid.UUID{},
	}
}

func (s *syncState) scanDataset(mm *chroma.MetadataMap) error {
	return mm.IterateMappings(func(rec *chroma.MappingRecord) error {
		s.fpTrack[rec.FingerprintID] = rec.TrackID
		if rec.TrackID == 0 {
			return nil
		}
		s.trackFPs[rec.TrackID] = append(s.trackFPs[rec.TrackID], rec.FingerprintID)
		if rec.MBID != uuid.Nil {
			if _, ok := s.trackMBID[rec.TrackID]; !ok {
				s.trackMBID[rec.TrackID] = rec.MBID
			}
		}
		return nil
	})
}

func (s *syncState) applyJournal(j *syncJournal) {
	for _, tm := range j.trackMBIDs {
		if _, ok := s.trackMBID[tm.trackID]; !ok {
			s.trackMBID[tm.trackID] = tm.mbid
		}
	}
	for _, fp := range j.newFPs {
		s.fpTrack[fp.id] = fp.trackID
		s.trackFPs[fp.trackID] = append(s.trackFPs[fp.trackID], fp.id)
	}
}
