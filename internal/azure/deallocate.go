package azure

import (
	"context"
	"log"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
)

func SelfDeallocate(ctx context.Context) error {
	meta, err := FetchVMMetadata(ctx, http.DefaultClient)
	if err != nil {
		return err
	}

	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		return err
	}

	client, err := armcompute.NewVirtualMachinesClient(meta.SubscriptionID, cred, nil)
	if err != nil {
		return err
	}

	if _, err := client.BeginDeallocate(ctx, meta.ResourceGroupName, meta.Name, nil); err != nil {
		return err
	}

	log.Printf("self-deallocation triggered for %s/%s", meta.ResourceGroupName, meta.Name)
	return nil
}
