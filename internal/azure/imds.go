package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type VMMetadata struct {
	SubscriptionID    string
	ResourceGroupName string
	Name              string
}

func FetchVMMetadata(ctx context.Context, client *http.Client) (VMMetadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://169.254.169.254/metadata/instance?api-version=2021-02-01", nil)
	if err != nil {
		return VMMetadata{}, err
	}
	req.Header.Set("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return VMMetadata{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return VMMetadata{}, fmt.Errorf("imds returned %s", resp.Status)
	}

	var payload struct {
		Compute struct {
			SubscriptionID    string `json:"subscriptionId"`
			ResourceGroupName string `json:"resourceGroupName"`
			Name              string `json:"name"`
		} `json:"compute"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return VMMetadata{}, err
	}

	return VMMetadata{
		SubscriptionID:    payload.Compute.SubscriptionID,
		ResourceGroupName: payload.Compute.ResourceGroupName,
		Name:              payload.Compute.Name,
	}, nil
}
