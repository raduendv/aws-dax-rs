package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	dax "github.com/raduendv/aws-dax-ffi-go/aws_dax_ffi"
)

func liveOperations(client *dax.DaxClient) (err error) {
	table := os.Getenv("DAX_TABLE_NAME")
	if table == "" {
		table = "DaxParityTable"
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return err
	}
	key := map[string]dax.DaxAttributeValue{
		"PK": dax.DaxAttributeValueString{String: fmt.Sprintf("PACKAGE#GO#%x", id)},
		"SK": dax.DaxAttributeValueString{String: fmt.Sprintf("ITEM#%x", id)},
	}
	item := map[string]dax.DaxAttributeValue{
		"PK": key["PK"], "SK": key["SK"], "value": dax.DaxAttributeValueString{String: "created"},
	}
	inserted := false
	// Register cleanup before PutItem: a failed response can still mean a write.
	defer func() {
		allOld := dax.DaxReturnValueAllOld
		output, cleanupErr := client.DeleteItem(dax.DaxDeleteItemRequest{
			TableName: table, Key: key, ReturnValues: &allOld,
			ExpressionAttributeNames: map[string]string{}, ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
		})
		if cleanupErr == nil && inserted && (output.Attributes == nil || !reflect.DeepEqual(*output.Attributes, item)) {
			cleanupErr = errors.New("DeleteItem ALL_OLD did not match the inserted item")
		}
		err = errors.Join(err, cleanupErr)
	}()
	_, err = client.PutItem(dax.DaxPutItemRequest{
		TableName: table, Item: item,
		ExpressionAttributeNames: map[string]string{}, ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
	})
	if err != nil {
		return err
	}
	inserted = true
	for attempt := 0; attempt < 20; attempt++ {
		output, getErr := client.GetItem(dax.DaxGetItemRequest{
			TableName: table, Key: key, ExpressionAttributeNames: map[string]string{},
		})
		if getErr != nil {
			return getErr
		}
		if output.Item != nil {
			if !reflect.DeepEqual(*output.Item, item) {
				return errors.New("GetItem did not match the inserted item")
			}
			return nil
		}
		if attempt < 19 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return errors.New("GetItem did not observe the inserted item after 20 attempts")
}

func run() (err error) {
	if dax.FfiContractVersion() == "" {
		return errors.New("empty FFI contract version")
	}
	config := dax.DaxClientConfig{
		Endpoints: []string{"127.0.0.1:8111"}, RequestTimeoutMillis: 60_000,
		WriteRetries: 2, ReadRetries: 2, ThrottleBaseDelayMillis: 70,
		ThrottleMaxBackoffMillis: 20_000, MaxPendingConnectionsPerHost: 10,
		ClusterUpdateIntervalMillis: 4_000, ClusterUpdateThresholdMillis: 125,
		IdleConnectionReapDelayMillis: 30_000, ClientHealthCheckIntervalMillis: 5_000,
		IpDiscovery: dax.DaxIpDiscoveryDefault, LogLevel: dax.DaxLogLevelOff,
	}
	if region := os.Getenv("AWS_REGION"); region != "" {
		config.Region = &region
	}
	if os.Getenv("DAX_FFI_LIVE") == "1" || os.Getenv("DAX_FFI_CREATE_CLIENT") == "1" {
		endpoint := os.Getenv("DAX_ENDPOINT")
		if endpoint == "" {
			return errors.New("DAX_ENDPOINT is required")
		}
		config.Endpoints = []string{endpoint}
		client, createErr := dax.CreateDaxClient(config)
		if createErr != nil {
			return createErr
		}
		defer client.Destroy()
		defer func() { err = errors.Join(err, client.Shutdown()) }()
		if os.Getenv("DAX_FFI_LIVE") == "1" {
			return liveOperations(client)
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
	if os.Getenv("DAX_FFI_LIVE") == "1" {
		fmt.Println("Go package live operations passed")
	} else {
		fmt.Println("go package native load ok")
	}
}
