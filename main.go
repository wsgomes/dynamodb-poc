package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Item struct {
	UserID   string `json:"UserID"`   // Partition key
	FirstDay string `json:"FirstDay"` // Sort key (start date)
	LastDay  int64  `json:"LastDay"`  // Also used as TTL
	Data     string `json:"Data"`
}

func main() {
	dynamoDBEndpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if dynamoDBEndpoint == "" {
		log.Fatal("DYNAMODB_ENDPOINT is not set")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"), // Dummy region
		Endpoint:    aws.String(dynamoDBEndpoint),
		Credentials: credentials.NewStaticCredentials("fakeMyKeyId", "fakeSecretAccessKey", ""), // Fake credentials
	})
	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	svc := dynamodb.New(sess)

	tableName := "TestTable"
	_, err = svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		fmt.Printf("Failed to delete table: %v\n", err)
	} else {
		fmt.Println("Table deleted successfully!")
	}

	time.Sleep(5 * time.Second)

	createTableInput := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("UserID"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("FirstDay"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("UserID"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("FirstDay"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	_, err = svc.CreateTable(createTableInput)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Table created successfully!")

	time.Sleep(5 * time.Second)

	/**
	/ PutItem flow
	**/

	items := []Item{
		{UserID: "123", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "123", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "123", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "123", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "123", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
		{UserID: "123", FirstDay: "20240829#bills#groupid1", LastDay: GetLastDayUnix("20240905"), Data: "XYZ"},
		{UserID: "124", FirstDay: "20240826#bills#groupid2", LastDay: GetLastDayUnix("20240828"), Data: "XYZ"},
		{UserID: "124", FirstDay: "20240827#bills#groupid2", LastDay: GetLastDayUnix("20240829"), Data: "XYZ"},
	}

	var inputConsumedCapacity float64
	for _, item := range items {
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			log.Fatalf("Failed to marshal item: %v", err)
		}

		putItemInput := &dynamodb.PutItemInput{
			TableName:              aws.String(tableName),
			Item:                   av,
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		}

		putItemOutput, err := svc.PutItem(putItemInput)
		if err != nil {
			log.Fatalf("Failed to put item: %v", err)
		}
		inputConsumedCapacity += *putItemOutput.ConsumedCapacity.CapacityUnits
	}
	fmt.Printf("Items inserted successfully! Consumed capacity by %d items: %.2f\n", len(items), inputConsumedCapacity)

	/**
	/ BatchWriteItem flow
	**/

	moreItems := []Item{
		{UserID: "125", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "125", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "125", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "125", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "125", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
		{UserID: "126", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "126", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "126", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "126", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "126", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
		{UserID: "127", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "127", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "127", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "127", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "127", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
		{UserID: "128", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "128", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "128", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "128", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "128", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
		{UserID: "129", FirstDay: "20240820#bills#groupid1", LastDay: GetLastDayUnix("20240825"), Data: "XYZ"},
		{UserID: "129", FirstDay: "20240822#bills#groupid1", LastDay: GetLastDayUnix("20240827"), Data: "XYZ"},
		{UserID: "129", FirstDay: "20240825#bills#groupid1", LastDay: GetLastDayUnix("20240830"), Data: "XYZ"},
		{UserID: "129", FirstDay: "20240827#bills#groupid1", LastDay: GetLastDayUnix("20240901"), Data: "XYZ"},
		{UserID: "129", FirstDay: "20240828#bills#groupid1", LastDay: GetLastDayUnix("20240904"), Data: "XYZ"},
	}

	var writeRequests []*dynamodb.WriteRequest
	for _, item := range moreItems {
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			log.Fatalf("Failed to marshal item: %v", err)
		}

		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: av,
			},
		})
	}

	const batchSize = 25
	var totalConsumedCapacity float64

	for i := 0; i < len(writeRequests); i += batchSize {
		fmt.Println(i)
		end := i + batchSize
		if end > len(writeRequests) {
			end = len(writeRequests)
		}

		batch := writeRequests[i:end]
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				"TestTable": batch,
			},
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		}

		for {
			output, err := svc.BatchWriteItem(input)
			if err != nil {
				log.Fatalf("Failed to write batch: %v", err)
			}

			for _, consumed := range output.ConsumedCapacity {
				totalConsumedCapacity += *consumed.CapacityUnits
			}

			if len(output.UnprocessedItems) == 0 {
				break
			}
			log.Println("Retrying unprocessed items...")
			input.RequestItems = output.UnprocessedItems
		}
	}

	fmt.Printf("Batch write completed successfully. Consumed capacity: %.2f\n", totalConsumedCapacity)

	location, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		log.Fatalf("Failed to load location: %v", err)
	}
	queryDate := time.Date(2024, time.Month(8), 27, 0, 0, 0, 0, location) // Today

	/**
	/ Get flow
	**/

	keyCond := expression.Key("UserID").Equal(expression.Value("123")).And(
		expression.Key("FirstDay").LessThan(expression.Value(queryDate.Add(24 * time.Hour).Format("20060102"))),
	)

	filterExpr := expression.Name("LastDay").GreaterThan(expression.Value(queryDate.Unix()))

	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithFilter(filterExpr).Build()
	if err != nil {
		log.Fatalf("Failed to build expression: %v", err)
	}

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	result, err := svc.Query(queryInput)
	if err != nil {
		log.Fatalf("Failed to query items: %v", err)
	}

	var retrievedItems []Item
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &retrievedItems)
	if err != nil {
		log.Fatalf("Failed to unmarshal query result items: %v", err)
	}

	fmt.Printf("Consumed capacity to query: %.2f\n", *result.ConsumedCapacity.CapacityUnits)

	fmt.Println("Query results:")
	for _, item := range retrievedItems {
		fmt.Printf("UserID: %s, FirstDay: %s, LastDay: %s, Data: %s\n", item.UserID, item.FirstDay, time.Unix(item.LastDay, 0).In(location), item.Data)
	}

	/**
	/ Delete flow
	**/

	userID := "124"
	firstDay := "20240827#bills#groupid2"

	deleteInput := &dynamodb.DeleteItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"UserID": {
				S: aws.String(userID),
			},
			"FirstDay": {
				S: aws.String(firstDay),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	deleteOutput, err := svc.DeleteItem(deleteInput)
	if err != nil {
		log.Fatalf("Failed to delete item: %v", err)
	}

	fmt.Printf("Item successfully deleted. Consumed capacity: %.2f\n", *deleteOutput.ConsumedCapacity.CapacityUnits)

	/**
	/ Delete BatchWriteItem flow
	**/

	var deleteRequests []*dynamodb.WriteRequest
	for _, key := range moreItems {
		deleteRequests = append(deleteRequests, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"UserID": {
						S: aws.String(key.UserID),
					},
					"FirstDay": {
						S: aws.String(key.FirstDay),
					},
				},
			},
		})
	}

	totalConsumedCapacity = 0

	for i := 0; i < len(deleteRequests); i += batchSize {
		fmt.Println(i)
		end := i + batchSize
		if end > len(deleteRequests) {
			end = len(deleteRequests)
		}

		batch := deleteRequests[i:end]
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				"TestTable": batch,
			},
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		}

		for {
			output, err := svc.BatchWriteItem(input)
			if err != nil {
				log.Fatalf("Failed to delete batch: %v", err)
			}

			for _, consumed := range output.ConsumedCapacity {
				totalConsumedCapacity += *consumed.CapacityUnits
			}

			if len(output.UnprocessedItems) == 0 {
				break
			}
			log.Println("Retrying unprocessed items...")
			input.RequestItems = output.UnprocessedItems
		}
	}

	fmt.Printf("Batch delete completed successfully. Consumed capacity: %.2f\n", totalConsumedCapacity)
}

func GetLastDayUnix(dateStr string) int64 {
	const layout = "20060102"

	parsedTime, err := time.Parse(layout, dateStr)
	if err != nil {
		log.Fatalf("Failed to parse dateStr: %v", err)
	}

	location, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		log.Fatalf("Failed to load location: %v", err)
	}

	timeInLocation := time.Date(
		parsedTime.Year(),
		parsedTime.Month(),
		parsedTime.Day(),
		0, 0, 0, 0,
		location,
	)

	return timeInLocation.Add(24 * time.Hour).Unix()
}
