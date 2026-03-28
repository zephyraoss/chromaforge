package build

import (
	"encoding/json"
	"os"
	"time"
)

type buildProgress struct {
	LastCompletedDay string    `json:"last_completed_day"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func progressPath(dbPath string) string {
	return dbPath + ".progress.json"
}

func loadBuildProgress(dbPath string) (buildProgress, bool, error) {
	path := progressPath(dbPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return buildProgress{}, false, nil
		}
		return buildProgress{}, false, err
	}

	var progress buildProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return buildProgress{}, false, err
	}
	return progress, true, nil
}

func saveBuildProgress(dbPath string, day time.Time) error {
	progress := buildProgress{
		LastCompletedDay: day.Format("2006-01-02"),
		UpdatedAt:        time.Now().UTC(),
	}

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(progressPath(dbPath), data, 0o644)
}

func clearBuildProgress(dbPath string) error {
	err := os.Remove(progressPath(dbPath))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
