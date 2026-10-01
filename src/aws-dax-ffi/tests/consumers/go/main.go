package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	dax "live/aws_dax_ffi"
)

func stringValue(value string) dax.DaxAttributeValue {
	return dax.DaxAttributeValueString{String: value}
}

func faultBarrier(client *dax.DaxClient) error {
	ready, release := os.Getenv("DAX_FAULT_READY_FILE"), os.Getenv("DAX_FAULT_RELEASE_FILE")
	if ready == "" || release == "" {
		return nil
	}
	endpoints, err := client.DiscoverEndpoints()
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return errors.New("fault prewarm discovered no endpoints")
	}
	if _, err = client.RefreshEndpoints(); err != nil {
		return err
	}
	routes, err := client.RouteSnapshot()
	if err != nil {
		return err
	}
	if len(routes) == 0 {
		return errors.New("fault prewarm has no routes")
	}
	file, err := os.OpenFile(ready, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		info, err := os.Stat(release)
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("fault release path is not a regular file")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds")
}

func main() {
	endpoint := os.Getenv("DAX_ENDPOINT")
	if endpoint == "" {
		panic("DAX_ENDPOINT is required")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "eu-west-1"
	}
	table := os.Getenv("DAX_TABLE_NAME")
	if table == "" {
		table = "DaxParityTable"
	}
	tableCleanupEnabled := !strings.EqualFold(os.Getenv("DAX_FFI_TABLE_CLEANUP"), "false")
	config := dax.DaxClientConfig{
		Endpoints:                       []string{endpoint},
		Region:                          &region,
		RequestTimeoutMillis:            60_000,
		WriteRetries:                    2,
		ReadRetries:                     2,
		ThrottleBaseDelayMillis:         70,
		ThrottleMaxBackoffMillis:        20_000,
		MaxPendingConnectionsPerHost:    10,
		ClusterUpdateIntervalMillis:     4_000,
		ClusterUpdateThresholdMillis:    125,
		IdleConnectionReapDelayMillis:   30_000,
		ClientHealthCheckIntervalMillis: 5_000,
		IpDiscovery:                     dax.DaxIpDiscoveryDefault,
		LogLevel:                        dax.DaxLogLevelOff,
	}
	client, err := dax.CreateDaxClient(config)
	must(err)
	defer client.Destroy()
	if err = faultBarrier(client); err != nil {
		must(errors.Join(err, client.Shutdown()))
	}

	var runID [16]byte
	_, err = rand.Read(runID[:])
	must(err)
	partition := fmt.Sprintf("FFI#GO#%x", runID)
	key := map[string]dax.DaxAttributeValue{
		"PK": stringValue(partition),
		"SK": stringValue("ITEM#1"),
	}
	postShutdownRequest := dax.DaxQueryRequest{
		TableName: table, KeyConditionExpression: "PK = :pk",
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":pk": stringValue(partition)},
	}
	postShutdownPaginator, err := client.QueryPaginator(postShutdownRequest)
	must(err)
	defer func() {
		must(client.Shutdown())
		if !client.IsClosed() {
			panic("client was not closed after shutdown")
		}
		if _, shutdownErr := postShutdownPaginator.NextPage(); shutdownErr == nil {
			panic("post-shutdown paginator call unexpectedly succeeded")
		}
		if _, shutdownErr := client.GetItem(dax.DaxGetItemRequest{
			TableName: table, Key: key, ExpressionAttributeNames: map[string]string{},
		}); shutdownErr == nil {
			panic("post-shutdown operation unexpectedly succeeded")
		}
		postShutdownPaginator.Destroy()
	}()
	inserted := false
	if tableCleanupEnabled {
		defer func() {
			allOld := dax.DaxReturnValueAllOld
			output, deleteErr := client.DeleteItem(dax.DaxDeleteItemRequest{
				TableName: table, Key: key, ReturnValues: &allOld,
				ExpressionAttributeNames:  map[string]string{},
				ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
			})
			must(deleteErr)
			if inserted && output.Attributes == nil {
				panic("DeleteItem did not return ALL_OLD")
			}
		}()
	}
	batchKeys := []map[string]dax.DaxAttributeValue{
		{"PK": stringValue(partition), "SK": stringValue("BATCH#1")},
		{"PK": stringValue(partition), "SK": stringValue("BATCH#2")},
	}
	paginatorKeys := []map[string]dax.DaxAttributeValue{
		{"PK": stringValue(partition), "SK": stringValue("PAGE#1")},
		{"PK": stringValue(partition), "SK": stringValue("PAGE#2")},
		{"PK": stringValue(partition), "SK": stringValue("PAGE#3")},
	}
	transactionKeys := []map[string]dax.DaxAttributeValue{
		{"PK": stringValue(partition), "SK": stringValue("TRANSACTION#1")},
		{"PK": stringValue(partition), "SK": stringValue("TRANSACTION#2")},
	}
	if tableCleanupEnabled {
		for _, cleanupKey := range append(append(batchKeys, paginatorKeys...), transactionKeys...) {
			defer func() {
				_, deleteErr := client.DeleteItem(dax.DaxDeleteItemRequest{
					TableName: table, Key: cleanupKey,
					ExpressionAttributeNames:  map[string]string{},
					ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
				})
				must(deleteErr)
			}()
		}
	}

	_, err = client.PutItem(dax.DaxPutItemRequest{
		TableName: table,
		Item: map[string]dax.DaxAttributeValue{
			"PK": key["PK"], "SK": key["SK"], "value": stringValue("created"),
		},
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
	})
	must(err)
	inserted = true

	observed := false
	for range 20 {
		output, getErr := client.GetItem(dax.DaxGetItemRequest{
			TableName: table, Key: key, ExpressionAttributeNames: map[string]string{},
		})
		must(getErr)
		if output.Item != nil {
			observed = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !observed {
		panic("GetItem did not observe the inserted item")
	}

	updateExpression := "SET #value = :value"
	allOld := dax.DaxReturnValueAllOld
	updated, err := client.UpdateItem(dax.DaxUpdateItemRequest{
		TableName: table, Key: key, UpdateExpression: updateExpression,
		ExpressionAttributeNames:  map[string]string{"#value": "value"},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":value": stringValue("updated")},
		ReturnValues:              &allOld,
	})
	must(err)
	if updated.Attributes == nil {
		panic("UpdateItem did not return ALL_OLD")
	}

	query, err := client.Query(dax.DaxQueryRequest{
		TableName: table, KeyConditionExpression: "PK = :pk",
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":pk": stringValue(partition)},
	})
	must(err)
	if len(query.Items) == 0 {
		panic("Query returned no items")
	}

	scan, err := client.Scan(dax.DaxScanRequest{
		TableName: table, FilterExpression: stringPointer("PK = :pk"),
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":pk": stringValue(partition)},
	})
	must(err)
	if len(scan.Items) == 0 {
		panic("Scan returned no items")
	}

	total := dax.DaxReturnConsumedCapacityTotal
	allBatchKeys := append(append([]map[string]dax.DaxAttributeValue{}, batchKeys...), paginatorKeys...)
	batchPuts := make([]dax.DaxBatchWriteAction, 0, len(allBatchKeys))
	batchDeletes := make([]dax.DaxBatchWriteAction, 0, len(allBatchKeys))
	for _, batchKey := range allBatchKeys {
		batchPuts = append(batchPuts, dax.DaxBatchWriteActionPut{Item: itemWithValue(batchKey, "batch")})
		batchDeletes = append(batchDeletes, dax.DaxBatchWriteActionDelete{Key: batchKey})
	}
	batchWrite, err := client.BatchWriteItem(dax.DaxBatchWriteItemRequest{
		RequestItems:           map[string][]dax.DaxBatchWriteAction{table: batchPuts},
		ReturnConsumedCapacity: &total,
	})
	must(err)
	if len(batchWrite.UnprocessedItems) != 0 {
		panic("BatchWriteItem put returned unprocessed items")
	}
	batchGet, err := client.BatchGetItem(dax.DaxBatchGetItemRequest{
		RequestItems: map[string]dax.DaxKeysAndAttributes{
			table: {Keys: batchKeys, ExpressionAttributeNames: map[string]string{}},
		},
		ReturnConsumedCapacity: &total,
	})
	must(err)
	if len(batchGet.Responses[table]) != 2 || len(batchGet.UnprocessedKeys) != 0 {
		panic("BatchGetItem did not return both items without unprocessed keys")
	}
	for _, batchKey := range batchKeys {
		found := false
		for _, item := range batchGet.Responses[table] {
			found = found || matchesItem(item, batchKey, "batch")
		}
		if !found {
			panic("BatchGetItem returned an unexpected item")
		}
	}
	must(verifyDynamoDBItems(context.Background(), region, table, batchKeys, "batch"))
	for _, stopOnDuplicateToken := range []bool{false, true} {
		paginator, paginatorErr := client.BatchGetItemPaginator(dax.DaxBatchGetItemRequest{
			RequestItems: map[string]dax.DaxKeysAndAttributes{
				table: {Keys: batchKeys, ExpressionAttributeNames: map[string]string{}},
			},
		}, stopOnDuplicateToken)
		must(paginatorErr)
		page, pageErr := paginator.NextPage()
		must(pageErr)
		if page == nil || len(page.Responses[table]) != len(batchKeys) {
			panic("BatchGet paginator did not return its terminal page")
		}
		if paginator.HasMorePages() {
			panic("BatchGet paginator retained a continuation without unprocessed keys")
		}
		page, pageErr = paginator.NextPage()
		must(pageErr)
		if page != nil {
			panic("BatchGet paginator returned a page after its terminal token")
		}
		paginator.Destroy()
	}
	queryPaginator, err := client.QueryPaginator(dax.DaxQueryRequest{
		TableName: table, KeyConditionExpression: "PK = :pk",
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":pk": stringValue(partition)},
		Limit:                     int32Pointer(2),
	})
	must(err)
	queryPages, queryItems := 0, 0
	for queryPaginator.HasMorePages() {
		page, pageErr := queryPaginator.NextPage()
		must(pageErr)
		if page == nil {
			break
		}
		queryPages++
		queryItems += len(page.Items)
	}
	queryPaginator.Destroy()
	if queryPages < 2 || queryItems < len(allBatchKeys)+1 {
		panic("Query paginator did not traverse at least two pages of inserted items")
	}
	scanPaginator, err := client.ScanPaginator(dax.DaxScanRequest{
		TableName: table, FilterExpression: stringPointer("PK = :pk"),
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]dax.DaxAttributeValue{":pk": stringValue(partition)},
		Limit:                     int32Pointer(2),
	})
	must(err)
	scanPages, scanItems := 0, 0
	for scanPaginator.HasMorePages() && scanPages < 100 {
		page, pageErr := scanPaginator.NextPage()
		must(pageErr)
		if page == nil {
			break
		}
		scanPages++
		scanItems += len(page.Items)
		if scanItems >= len(allBatchKeys)+1 && scanPages >= 2 {
			break
		}
	}
	scanPaginator.Destroy()
	if scanPages < 2 || scanItems < len(allBatchKeys)+1 {
		panic("Scan paginator did not traverse at least two pages of inserted items")
	}
	discovered, err := client.DiscoverEndpoints()
	must(err)
	if len(discovered) == 0 {
		panic("endpoint discovery returned no endpoints")
	}
	active, err := client.ActiveEndpoints()
	must(err)
	if active == nil || len(*active) == 0 {
		panic("active endpoints were not committed by discovery")
	}
	_, err = client.RefreshEndpoints()
	must(err)
	routes, err := client.RouteSnapshot()
	must(err)
	if len(routes) == 0 {
		panic("route snapshot was empty after endpoint refresh")
	}
	if tableCleanupEnabled {
		batchDelete, err := client.BatchWriteItem(dax.DaxBatchWriteItemRequest{
			RequestItems: map[string][]dax.DaxBatchWriteAction{table: batchDeletes},
		})
		must(err)
		if len(batchDelete.UnprocessedItems) != 0 {
			panic("BatchWriteItem delete returned unprocessed items")
		}
	}

	transactionPuts := make([]dax.DaxTransactWriteAction, 0, 2)
	transactionGets := make([]dax.DaxTransactGetAction, 0, 2)
	transactionDeletes := make([]dax.DaxTransactWriteAction, 0, 2)
	for _, transactionKey := range transactionKeys {
		transactionPuts = append(transactionPuts, dax.DaxTransactWriteActionPut{
			TableName: table, Item: itemWithValue(transactionKey, "transaction"),
			ExpressionAttributeNames: map[string]string{}, ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
		})
		transactionGets = append(transactionGets, dax.DaxTransactGetAction{
			TableName: table, Key: transactionKey, ExpressionAttributeNames: map[string]string{},
		})
		transactionDeletes = append(transactionDeletes, dax.DaxTransactWriteActionDelete{
			TableName: table, Key: transactionKey,
			ExpressionAttributeNames: map[string]string{}, ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
		})
	}
	_, err = client.TransactWriteItems(dax.DaxTransactWriteItemsRequest{
		TransactItems: transactionPuts, ReturnConsumedCapacity: &total,
	})
	must(err)
	transactionGet, err := client.TransactGetItems(dax.DaxTransactGetItemsRequest{
		TransactItems: transactionGets, ReturnConsumedCapacity: &total,
	})
	must(err)
	if len(transactionGet.Responses) != 2 {
		panic("TransactGetItems did not return both items")
	}
	for i, response := range transactionGet.Responses {
		if response.Item == nil || !matchesItem(*response.Item, transactionKeys[i], "transaction") {
			panic("TransactGetItems returned an unexpected item")
		}
	}

	failureAllOld := dax.DaxReturnValuesOnConditionCheckFailureAllOld
	_, err = client.TransactWriteItems(dax.DaxTransactWriteItemsRequest{
		TransactItems: []dax.DaxTransactWriteAction{dax.DaxTransactWriteActionConditionCheck{
			TableName: table, Key: transactionKeys[0], ConditionExpression: "attribute_not_exists(PK)",
			ExpressionAttributeNames: map[string]string{}, ExpressionAttributeValues: map[string]dax.DaxAttributeValue{},
			ReturnValuesOnConditionCheckFailure: &failureAllOld,
		}},
	})
	if err == nil {
		panic("failing transaction unexpectedly succeeded")
	}
	var failure *dax.DaxFfiErrorFailure
	if !errors.As(err, &failure) {
		panic(err)
	}
	if failure.Details.Category != dax.DaxErrorCategoryService || len(failure.Details.CancellationReasons) == 0 {
		panic("failing transaction did not return structured service cancellation reasons")
	}
	if tableCleanupEnabled {
		_, err = client.TransactWriteItems(dax.DaxTransactWriteItemsRequest{TransactItems: transactionDeletes})
		must(err)
	}

	if !tableCleanupEnabled {
		fmt.Printf("Go FFI retained test items PK=%s\n", partition)
	}
	fmt.Println("Go FFI direct, batch, transaction, paginator, route, and lifecycle operations passed")
}

func itemWithValue(key map[string]dax.DaxAttributeValue, value string) map[string]dax.DaxAttributeValue {
	return map[string]dax.DaxAttributeValue{"PK": key["PK"], "SK": key["SK"], "value": stringValue(value)}
}

func verifyDynamoDBItems(ctx context.Context, region, table string, keys []map[string]dax.DaxAttributeValue, value string) error {
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}
	client := dynamodb.NewFromConfig(awsConfig)
	for _, key := range keys {
		partition, ok := key["PK"].(dax.DaxAttributeValueString)
		if !ok {
			return errors.New("DynamoDB verification expected a string PK")
		}
		sort, ok := key["SK"].(dax.DaxAttributeValueString)
		if !ok {
			return errors.New("DynamoDB verification expected a string SK")
		}
		output, getErr := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      &table,
			ConsistentRead: aws.Bool(true),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: partition.String},
				"SK": &types.AttributeValueMemberS{Value: sort.String},
			},
		})
		if getErr != nil {
			return getErr
		}
		if !matchesDynamoDBItem(output.Item, partition.String, sort.String, value) {
			return fmt.Errorf("DynamoDB did not return expected item PK=%q SK=%q", partition.String, sort.String)
		}
	}
	return nil
}

func matchesDynamoDBItem(item map[string]types.AttributeValue, partition, sort, value string) bool {
	for name, expected := range map[string]string{"PK": partition, "SK": sort, "value": value} {
		actual, ok := item[name].(*types.AttributeValueMemberS)
		if !ok || actual.Value != expected {
			return false
		}
	}
	return true
}

func matchesItem(item, key map[string]dax.DaxAttributeValue, value string) bool {
	for name, expected := range itemWithValue(key, value) {
		actual, ok := item[name].(dax.DaxAttributeValueString)
		if !ok || actual != expected {
			return false
		}
	}
	return true
}

func stringPointer(value string) *string {
	return &value
}

func int32Pointer(value int32) *int32 {
	return &value
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
