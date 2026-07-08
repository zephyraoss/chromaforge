package build

import (
	"encoding/json"
	"os"
	"time"
)

type syncProgress struct {
	LastSyncedDay string    `json:"last_synced_day"`
	Generation    string    `json:"generation,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func syncProgressPath(datasetPrefix string) string {
	return datasetPrefix + ".sync-progress.json"
}

func loadSyncProgress(datasetPrefix string) (syncProgress, bool, error) {
	data, err := os.ReadFile(syncProgressPath(datasetPrefix))
	if err != nil {
		if os.IsNotExist(err) {
			return syncProgress{}, false, nil
		}
		return syncProgress{}, false, err
	}

	var progress syncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return syncProgress{}, false, err
	}
	return progress, true, nil
}

func saveSyncProgress(datasetPrefix, lastSyncedDay, generation string) error {
	progress := syncProgress{
		LastSyncedDay: lastSyncedDay,
		Generation:    generation,
		UpdatedAt:     time.Now().UTC(),
	}

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(syncProgressPath(datasetPrefix), data, 0o644)
}
