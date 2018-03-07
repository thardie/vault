package storagepacker

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/vault/helper/identity"
	"github.com/hashicorp/vault/logical"
	log "github.com/mgutz/logxi/v1"
)

func TestStoragePackerV2_PutGet(t *testing.T) {
	sp, err := NewStoragePackerV2(&Config{
		View:   &logical.InmemStorage{},
		Logger: log.New("storagepackertest"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write 100 items; check if it can be read back out
	for i := 1; i <= 100; i++ {
		id := strconv.Itoa(i)

		message := &identity.Entity{
			ID: id,
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

func Benchmark_StoragePackerV2_Sharding(b *testing.B) {
	sp, err := NewStoragePackerV2(&Config{
		View:             &logical.InmemStorage{},
		Logger:           log.New("storagepackertest"),
		BucketCount:      8,
		BucketShardCount: 2,
		BucketMaxSize:    256,
	})
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		id := strconv.Itoa(i)
		message := &identity.Entity{
			ID: id,
			Metadata: map[string]string{
				"samplekey": "samplevalue",
			},
		}

		marshaledMessage, err := ptypes.MarshalAny(message)
		if err != nil {
			b.Fatal(err)
		}

		item := &Item{
			ID:      id,
			Message: marshaledMessage,
		}
		if err != nil {
			b.Fatal(err)
		}

		bucketKey, err := sp.PutItem(item)
		if err != nil {
			b.Fatal(err)
		}

		itemFetched, err := sp.GetItem(id)
		if err != nil {
			b.Fatal(err)
		}
		if itemFetched == nil {
			b.Fatalf("failed to read the inserted item; iteration: %d, bucketKey: %q, itemID: %q\n", i, bucketKey, id)
		}

		var fetchedMessage identity.Entity
		err = ptypes.UnmarshalAny(itemFetched.Message, &fetchedMessage)
		if err != nil {
			b.Fatal(err)
		}

		if fetchedMessage.ID != id {
			b.Fatalf("failed to fetch item ID: %q\n", id)
		}
	}
}

func TestStoragePackerV2_Delete(t *testing.T) {
	sp, err := NewStoragePackerV2(&Config{
		View:             &logical.InmemStorage{},
		Logger:           log.New("storagepackertest"),
		BucketCount:      8,
		BucketShardCount: 4,
		BucketMaxSize:    512,
	})
	if err != nil {
		t.Fatal(err)
	}

	count := 1000000

	for i := 1; i <= count; i++ {
		if i%500 == 0 {
			fmt.Printf("put item iteration: %d\n", i)
		}
		id := strconv.Itoa(i)
		message := &identity.Entity{
			ID: id,
			Metadata: map[string]string{
				"samplekey1": "samplevalue2",
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

		_, err = sp.PutItem(item)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= count; i++ {
		if i%500 == 0 {
			fmt.Printf("get item iteration: %d\n", i)
		}
		id := strconv.Itoa(i)
		itemFetched, err := sp.GetItem(id)
		if err != nil {
			t.Fatal(err)
		}
		if itemFetched == nil {
			t.Fatalf("failed to read the inserted item; itemID: %q\n", id)
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

	for i := 1; i <= count; i++ {
		if i%500 == 0 {
			fmt.Printf("delete item iteration: %d\n", i)
		}
		id := strconv.Itoa(i)
		err = sp.DeleteItem(id)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= count; i++ {
		if i%500 == 0 {
			fmt.Printf("verify deletion; get item iteration: %d\n", i)
		}
		id := strconv.Itoa(i)
		itemFetched, err := sp.GetItem(id)
		if err != nil {
			t.Fatal(err)
		}
		if itemFetched != nil {
			t.Fatalf("item should not exist; id: %q", id)
		}
	}
}
