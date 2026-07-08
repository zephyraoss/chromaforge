package build

import (
	"encoding/json"
	"os"
	"time"
)

type ckafProgress struct {
	LastCompletedDay string    `json:"last_completed_day"`
	SpoolBytes       int64     `json:"spool_bytes"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func ckafProgressPath(outputPrefix string) string {
	return outputPrefix + ".ckaf-progress.json"
}

func ckafSpoolPath(outputPrefix string) string {
	return outputPrefix + ".ckaf-spool"
}

func loadCKAFProgress(outputPrefix string) (ckafProgress, bool, error) {
	data, err := os.ReadFile(ckafProgressPath(outputPrefix))
	if err != nil {
		if os.IsNotExist(err) {
			return ckafProgress{}, false, nil
		}
		return ckafProgress{}, false, err
	}

	var progress ckafProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return ckafProgress{}, false, err
	}
	return progress, true, nil
}

func saveCKAFProgress(outputPrefix string, day time.Time, spoolBytes int64) error {
	progress := ckafProgress{
		LastCompletedDay: day.Format("2006-01-02"),
		SpoolBytes:       spoolBytes,
		UpdatedAt:        time.Now().UTC(),
	}

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ckafProgressPath(outputPrefix), data, 0o644)
}

func clearCKAFProgress(outputPrefix string) error {
	err := os.Remove(ckafProgressPath(outputPrefix))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
