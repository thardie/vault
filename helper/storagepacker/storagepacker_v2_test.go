package storagepacker

import (
	"strconv"
	"testing"

	"github.com/golang/protobuf/ptypes"
	uuid "github.com/hashicorp/go-uuid"
	"github.com/hashicorp/vault/helper/identity"
	"github.com/hashicorp/vault/logical"
	log "github.com/mgutz/logxi/v1"
)

func TestStoragePackerV2_Sharding(t *testing.T) {
	sp, err := NewStoragePackerV2(&ConfigV2{
		View:             &logical.InmemStorage{},
		Logger:           log.New("storagepackertest"),
		BucketCount:      8,
		BucketShardCount: 2,
		BucketMaxSize:    256,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < 20000; i++ {
		id := strconv.Itoa(i)
		message := &identity.Entity{
			ID: id,
			Metadata: map[string]string{
				"samplekey": "samplevalue",
			},
		}

		marshaledMessage, err := ptypes.MarshalAny(message)
		if err != nil {
			t.Fatal(err)
		}

		item := &Item{
			ID:      id,
			Message: marshaledMessage,
		}
		if err != nil {
			t.Fatal(err)
		}

		bucketKey, err := sp.PutItem(item)
		if err != nil {
			t.Fatal(err)
		}

		itemFetched, err := sp.GetItem(id)
		if err != nil {
			t.Fatal(err)
		}
		if itemFetched == nil {
			t.Fatalf("failed to read the inserted item; iteration: %d, bucketKey: %q, itemID: %q\n", i, bucketKey, id)
		}

		var fetchedMessage identity.Entity
		err = ptypes.UnmarshalAny(itemFetched.Message, &fetchedMessage)
		if err != nil {
			t.Fatal(err)
		}

		if fetchedMessage.ID != id {
			t.Fatalf("failed to fetch item ID: %q\n", id)
		}
	}
}

func TestStoragePackerV2_Basic(t *testing.T) {
	sp, err := NewStoragePackerV2(&Config{
		View:   &logical.InmemStorage{},
		Logger: log.New("storagepackertest"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write 100 items; check if it can be read back out
	for i := 1; i < 100; i++ {
		messageID := strconv.Itoa(i)

		message := &identity.Entity{
			ID: messageID,
		}

		marshaledMessage, err := ptypes.MarshalAny(message)
		if err != nil {
			t.Fatal(err)
		}

		itemID, err := uuid.GenerateUUID()
		if err != nil {
			t.Fatal(err)
		}

		item := &Item{
			ID:      itemID,
			Message: marshaledMessage,
		}
		if err != nil {
			t.Fatal(err)
		}

		bucketKey, err := sp.PutItem(item)
		if err != nil {
			t.Fatal(err)
		}

		itemFetched, err := sp.GetItem(itemID)
		if err != nil {
			t.Fatal(err)
		}
		if itemFetched == nil {
			t.Fatalf("failed to read the inserted item; iteration: %d, bucketKey: %q, itemID: %q\n", i, bucketKey, itemID)
		}

		var fetchedMessage identity.Entity
		err = ptypes.UnmarshalAny(itemFetched.Message, &fetchedMessage)
		if err != nil {
			t.Fatal(err)
		}

		if fetchedMessage.ID != messageID {
			t.Fatalf("failed to fetch item ID: %q\n", itemID)
		}
	}
}
