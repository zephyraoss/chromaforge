package build

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const ckafSpoolMagic = "CKAFSPL1"

const (
	ckafSpoolKindFingerprint byte = 1
	ckafSpoolKindMerge       byte = 2
)

type ckafSpoolRecord struct {
	Kind       byte
	ID         uint32
	DurationMs uint32
	RawCount   uint16
	Blob       []byte
}

type ckafSpoolWriter struct {
	f      *os.File
	w      *bufio.Writer
	offset int64
	closed bool
}

func createCKAFSpool(path string) (*ckafSpoolWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.WriteString(ckafSpoolMagic); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &ckafSpoolWriter{f: f, w: w, offset: int64(len(ckafSpoolMagic))}, nil
}

func openCKAFSpoolForAppend(path string, offset int64) (*ckafSpoolWriter, error) {
	if offset < int64(len(ckafSpoolMagic)) {
		return nil, fmt.Errorf("ckaf spool resume offset %d is smaller than the header", offset)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	var magic [len(ckafSpoolMagic)]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("ckaf spool header: %w", err)
	}
	if string(magic[:]) != ckafSpoolMagic {
		_ = f.Close()
		return nil, fmt.Errorf("ckaf spool magic mismatch in %s", path)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() < offset {
		_ = f.Close()
		return nil, fmt.Errorf("ckaf spool is %d bytes, smaller than resume offset %d", info.Size(), offset)
	}
	if err := f.Truncate(offset); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &ckafSpoolWriter{f: f, w: bufio.NewWriterSize(f, 1<<20), offset: offset}, nil
}

func (s *ckafSpoolWriter) WriteFingerprint(id, durationMs uint32, rawCount uint16, blob []byte) error {
	var hdr [15]byte
	hdr[0] = ckafSpoolKindFingerprint
	binary.LittleEndian.PutUint32(hdr[1:5], id)
	binary.LittleEndian.PutUint32(hdr[5:9], durationMs)
	binary.LittleEndian.PutUint16(hdr[9:11], rawCount)
	binary.LittleEndian.PutUint32(hdr[11:15], uint32(len(blob)))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(blob); err != nil {
		return err
	}
	s.offset += int64(len(hdr)) + int64(len(blob))
	return nil
}

func (s *ckafSpoolWriter) WriteMerge(id, durationMs uint32) error {
	var hdr [9]byte
	hdr[0] = ckafSpoolKindMerge
	binary.LittleEndian.PutUint32(hdr[1:5], id)
	binary.LittleEndian.PutUint32(hdr[5:9], durationMs)
	if _, err := s.w.Write(hdr[:]); err != nil {
		return err
	}
	s.offset += int64(len(hdr))
	return nil
}

func (s *ckafSpoolWriter) Offset() int64 {
	return s.offset
}

func (s *ckafSpoolWriter) Sync() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *ckafSpoolWriter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.w.Flush(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}

func scanCKAFSpool(path string, withBlob bool, fn func(ckafSpoolRecord) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var magic [len(ckafSpoolMagic)]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("ckaf spool header: %w", err)
	}
	if string(magic[:]) != ckafSpoolMagic {
		return fmt.Errorf("ckaf spool magic mismatch in %s", path)
	}

	for {
		kind, err := r.ReadByte()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch kind {
		case ckafSpoolKindFingerprint:
			var hdr [14]byte
			if _, err := io.ReadFull(r, hdr[:]); err != nil {
				return fmt.Errorf("ckaf spool fingerprint record: %w", err)
			}
			rec := ckafSpoolRecord{
				Kind:       kind,
				ID:         binary.LittleEndian.Uint32(hdr[0:4]),
				DurationMs: binary.LittleEndian.Uint32(hdr[4:8]),
				RawCount:   binary.LittleEndian.Uint16(hdr[8:10]),
			}
			blobLen := int(binary.LittleEndian.Uint32(hdr[10:14]))
			if withBlob {
				rec.Blob = make([]byte, blobLen)
				if _, err := io.ReadFull(r, rec.Blob); err != nil {
					return fmt.Errorf("ckaf spool fingerprint payload: %w", err)
				}
			} else if _, err := r.Discard(blobLen); err != nil {
				return fmt.Errorf("ckaf spool fingerprint payload: %w", err)
			}
			if err := fn(rec); err != nil {
				return err
			}
		case ckafSpoolKindMerge:
			var hdr [8]byte
			if _, err := io.ReadFull(r, hdr[:]); err != nil {
				return fmt.Errorf("ckaf spool merge record: %w", err)
			}
			if err := fn(ckafSpoolRecord{
				Kind:       kind,
				ID:         binary.LittleEndian.Uint32(hdr[0:4]),
				DurationMs: binary.LittleEndian.Uint32(hdr[4:8]),
			}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("ckaf spool: unknown record kind %d", kind)
		}
	}
}
