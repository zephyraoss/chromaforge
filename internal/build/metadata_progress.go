package build

import (
	"encoding/json"
	"os"
	"time"
)

func metadataBackfillProgressPath(dbPath string) string {
	return dbPath + ".metadata.progress.json"
}

func loadMetadataBackfillProgress(dbPath string) (buildProgress, bool, error) {
	return loadBuildProgressFile(metadataBackfillProgressPath(dbPath))
}

func saveMetadataBackfillProgress(dbPath string, day time.Time) error {
	return saveBuildProgressFile(metadataBackfillProgressPath(dbPath), day)
}

func clearMetadataBackfillProgress(dbPath string) error {
	return clearBuildProgressFile(metadataBackfillProgressPath(dbPath))
}

func loadBuildProgressFile(path string) (buildProgress, bool, error) {
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

func saveBuildProgressFile(path string, day time.Time) error {
	progress := buildProgress{
		LastCompletedDay: day.Format("2006-01-02"),
		UpdatedAt:        time.Now().UTC(),
	}

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func clearBuildProgressFile(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
