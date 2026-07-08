package build

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/google/uuid"
)

const (
	syncJournalMagic   = "CKSYNCJ2"
	syncJournalMagicV1 = "CKSYNCJ1"
)

const syncJournalNoCKD = int64(-1)

const (
	syncJournalKindNewFP      byte = 1
	syncJournalKindTrackMBID  byte = 2
	syncJournalKindDayDone    byte = 3
	syncJournalKindOrphanLink byte = 4
	syncJournalKindMerge      byte = 5
	syncJournalKindBackfill   byte = 6
	syncJournalKindApplied    byte = 7
)

const (
	syncJournalGenerationSize = 36
	syncJournalHeaderSizeV1   = 8 + 3*8
	syncJournalHeaderSize     = syncJournalHeaderSizeV1 + syncJournalGenerationSize
)

type syncNewFP struct {
	id         uint32
	durationMs uint32
	trackID    uint32
	mbid       uuid.UUID
	rawCount   uint16
	blob       []byte
}

type syncTrackMBID struct {
	trackID uint32
	mbid    uuid.UUID
}

type syncJournal struct {
	path string
	f    *os.File
	w    *bufio.Writer

	baseCKD int64
	baseCKI int64
	baseCKM int64

	generation string

	newFPs      []*syncNewFP
	newFPByID   map[uint32]*syncNewFP
	trackMBIDs  []syncTrackMBID
	orphanLinks map[uint32]uint32
	backfills   map[uint32]uint32
	days        map[string]struct{}
	lastDay     string

	dirty bool
}

func syncJournalPath(datasetPrefix string) string {
	return datasetPrefix + ".sync-journal"
}

func newSyncJournalState(path string) *syncJournal {
	return &syncJournal{
		path:        path,
		newFPByID:   map[uint32]*syncNewFP{},
		orphanLinks: map[uint32]uint32{},
		backfills:   map[uint32]uint32{},
		days:        map[string]struct{}{},
	}
}

func (j *syncJournal) hasRecords() bool {
	return len(j.newFPs) > 0 || len(j.trackMBIDs) > 0 || len(j.backfills) > 0 || len(j.orphanLinks) > 0
}

func createSyncJournal(datasetPrefix string, baseCKD, baseCKI, baseCKM int64, generation string) (*syncJournal, error) {
	if len(generation) > syncJournalGenerationSize {
		return nil, fmt.Errorf("sync journal: generation %q longer than %d bytes", generation, syncJournalGenerationSize)
	}
	path := syncJournalPath(datasetPrefix)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	j := newSyncJournalState(path)
	j.f = f
	j.w = bufio.NewWriterSize(f, 1<<20)
	j.baseCKD, j.baseCKI, j.baseCKM = baseCKD, baseCKI, baseCKM
	j.generation = generation

	var hdr [syncJournalHeaderSize]byte
	copy(hdr[0:8], syncJournalMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(baseCKD))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(baseCKI))
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(baseCKM))
	copy(hdr[32:32+syncJournalGenerationSize], generation)
	if _, err := j.w.Write(hdr[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := j.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return j, nil
}

func loadSyncJournal(datasetPrefix string) (*syncJournal, bool, error) {
	path := syncJournalPath(datasetPrefix)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	j := newSyncJournalState(path)
	r := bufio.NewReaderSize(f, 1<<20)

	var hdr [syncJournalHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:syncJournalHeaderSizeV1]); err != nil {
		_ = f.Close()
		return nil, false, fmt.Errorf("sync journal header: %w", err)
	}
	headerSize := int64(syncJournalHeaderSize)
	switch string(hdr[0:8]) {
	case syncJournalMagic:
		if _, err := io.ReadFull(r, hdr[syncJournalHeaderSizeV1:]); err != nil {
			_ = f.Close()
			return nil, false, fmt.Errorf("sync journal header: %w", err)
		}
		gen := hdr[32 : 32+syncJournalGenerationSize]
		j.generation = string(bytes.TrimRight(gen, "\x00"))
	case syncJournalMagicV1:
		headerSize = syncJournalHeaderSizeV1
	default:
		_ = f.Close()
		return nil, false, fmt.Errorf("sync journal magic mismatch in %s", path)
	}
	j.baseCKD = int64(binary.LittleEndian.Uint64(hdr[8:16]))
	j.baseCKI = int64(binary.LittleEndian.Uint64(hdr[16:24]))
	j.baseCKM = int64(binary.LittleEndian.Uint64(hdr[24:32]))

	offset := headerSize
	committedOffset := offset

	var pending []func()
	commit := func() {
		for _, apply := range pending {
			apply()
		}
		pending = nil
		committedOffset = offset
	}

	truncated := false
scan:
	for {
		kind, err := r.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = f.Close()
			return nil, false, err
		}
		offset++

		readFull := func(buf []byte) bool {
			n, err := io.ReadFull(r, buf)
			offset += int64(n)
			return err == nil
		}

		switch kind {
		case syncJournalKindNewFP:
			var fixed [34]byte
			if !readFull(fixed[:]) {
				truncated = true
				break scan
			}
			fp := &syncNewFP{
				id:         binary.LittleEndian.Uint32(fixed[0:4]),
				durationMs: binary.LittleEndian.Uint32(fixed[4:8]),
				trackID:    binary.LittleEndian.Uint32(fixed[8:12]),
				rawCount:   binary.LittleEndian.Uint16(fixed[28:30]),
			}
			copy(fp.mbid[:], fixed[12:28])
			blobLen := binary.LittleEndian.Uint32(fixed[30:34])
			fp.blob = make([]byte, blobLen)
			if !readFull(fp.blob) {
				truncated = true
				break scan
			}
			pending = append(pending, func() {
				j.newFPs = append(j.newFPs, fp)
				j.newFPByID[fp.id] = fp
			})
		case syncJournalKindTrackMBID:
			var buf [20]byte
			if !readFull(buf[:]) {
				truncated = true
				break scan
			}
			rec := syncTrackMBID{trackID: binary.LittleEndian.Uint32(buf[0:4])}
			copy(rec.mbid[:], buf[4:20])
			pending = append(pending, func() {
				j.trackMBIDs = append(j.trackMBIDs, rec)
			})
		case syncJournalKindDayDone:
			var buf [10]byte
			if !readFull(buf[:]) {
				truncated = true
				break scan
			}
			day := string(buf[:])
			pending = append(pending, func() {
				j.days[day] = struct{}{}
				if day > j.lastDay {
					j.lastDay = day
				}
				j.dirty = true
			})
			commit()
		case syncJournalKindOrphanLink:
			var buf [8]byte
			if !readFull(buf[:]) {
				truncated = true
				break scan
			}
			fp := binary.LittleEndian.Uint32(buf[0:4])
			track := binary.LittleEndian.Uint32(buf[4:8])
			pending = append(pending, func() {
				j.orphanLinks[fp] = track
			})
		case syncJournalKindMerge:
			var buf [8]byte
			if !readFull(buf[:]) {
				truncated = true
				break scan
			}
			fp := binary.LittleEndian.Uint32(buf[0:4])
			dur := binary.LittleEndian.Uint32(buf[4:8])
			pending = append(pending, func() {
				if rec, ok := j.newFPByID[fp]; ok && rec.durationMs == 0 {
					rec.durationMs = dur
				}
			})
		case syncJournalKindBackfill:
			var buf [8]byte
			if !readFull(buf[:]) {
				truncated = true
				break scan
			}
			fp := binary.LittleEndian.Uint32(buf[0:4])
			dur := binary.LittleEndian.Uint32(buf[4:8])
			pending = append(pending, func() {
				j.backfills[fp] = dur
			})
		case syncJournalKindApplied:
			if len(pending) > 0 {
				_ = f.Close()
				return nil, false, fmt.Errorf("sync journal: applied marker inside a day group in %s", path)
			}
			j.dirty = false
			committedOffset = offset
		default:
			_ = f.Close()
			return nil, false, fmt.Errorf("sync journal: unknown record kind %d in %s", kind, path)
		}
	}

	if truncated || len(pending) > 0 {
		log.Printf("sync journal: discarding interrupted day tail (%d bytes)", offset-committedOffset)
	}
	if err := f.Truncate(committedOffset); err != nil {
		_ = f.Close()
		return nil, false, err
	}
	if _, err := f.Seek(committedOffset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, false, err
	}

	j.f = f
	j.w = bufio.NewWriterSize(f, 1<<20)
	return j, true, nil
}

func (j *syncJournal) WriteNewFP(fp *syncNewFP) error {
	var fixed [35]byte
	fixed[0] = syncJournalKindNewFP
	binary.LittleEndian.PutUint32(fixed[1:5], fp.id)
	binary.LittleEndian.PutUint32(fixed[5:9], fp.durationMs)
	binary.LittleEndian.PutUint32(fixed[9:13], fp.trackID)
	copy(fixed[13:29], fp.mbid[:])
	binary.LittleEndian.PutUint16(fixed[29:31], fp.rawCount)
	binary.LittleEndian.PutUint32(fixed[31:35], uint32(len(fp.blob)))
	if _, err := j.w.Write(fixed[:]); err != nil {
		return err
	}
	if _, err := j.w.Write(fp.blob); err != nil {
		return err
	}
	j.newFPs = append(j.newFPs, fp)
	j.newFPByID[fp.id] = fp
	return nil
}

func (j *syncJournal) WriteTrackMBID(trackID uint32, mbid uuid.UUID) error {
	var buf [21]byte
	buf[0] = syncJournalKindTrackMBID
	binary.LittleEndian.PutUint32(buf[1:5], trackID)
	copy(buf[5:21], mbid[:])
	if _, err := j.w.Write(buf[:]); err != nil {
		return err
	}
	j.trackMBIDs = append(j.trackMBIDs, syncTrackMBID{trackID: trackID, mbid: mbid})
	return nil
}

func (j *syncJournal) WriteOrphanLink(fp, trackID uint32) error {
	var buf [9]byte
	buf[0] = syncJournalKindOrphanLink
	binary.LittleEndian.PutUint32(buf[1:5], fp)
	binary.LittleEndian.PutUint32(buf[5:9], trackID)
	if _, err := j.w.Write(buf[:]); err != nil {
		return err
	}
	j.orphanLinks[fp] = trackID
	return nil
}

func (j *syncJournal) WriteMerge(fp, durationMs uint32) error {
	var buf [9]byte
	buf[0] = syncJournalKindMerge
	binary.LittleEndian.PutUint32(buf[1:5], fp)
	binary.LittleEndian.PutUint32(buf[5:9], durationMs)
	if _, err := j.w.Write(buf[:]); err != nil {
		return err
	}
	if rec, ok := j.newFPByID[fp]; ok && rec.durationMs == 0 {
		rec.durationMs = durationMs
	}
	return nil
}

func (j *syncJournal) WriteBackfill(fp, durationMs uint32) error {
	var buf [9]byte
	buf[0] = syncJournalKindBackfill
	binary.LittleEndian.PutUint32(buf[1:5], fp)
	binary.LittleEndian.PutUint32(buf[5:9], durationMs)
	if _, err := j.w.Write(buf[:]); err != nil {
		return err
	}
	j.backfills[fp] = durationMs
	return nil
}

func (j *syncJournal) WriteDayDone(day string) error {
	if len(day) != 10 {
		return fmt.Errorf("sync journal: bad day %q", day)
	}
	buf := make([]byte, 11)
	buf[0] = syncJournalKindDayDone
	copy(buf[1:], day)
	if _, err := j.w.Write(buf); err != nil {
		return err
	}
	if err := j.Sync(); err != nil {
		return err
	}
	j.days[day] = struct{}{}
	if day > j.lastDay {
		j.lastDay = day
	}
	j.dirty = true
	return nil
}

func (j *syncJournal) WriteApplied() error {
	if err := j.w.WriteByte(syncJournalKindApplied); err != nil {
		return err
	}
	if err := j.Sync(); err != nil {
		return err
	}
	j.dirty = false
	return nil
}

func (j *syncJournal) Sync() error {
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

func (j *syncJournal) Close() error {
	if j.f == nil {
		return nil
	}
	if err := j.w.Flush(); err != nil {
		_ = j.f.Close()
		j.f = nil
		return err
	}
	err := j.f.Close()
	j.f = nil
	return err
}
