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
	LastDay  string `json:"LastDay"`  // Additional attribute (end date)
	Data     string `json:"Data"`     // Additional data
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

	items := []Item{
		{UserID: "123", FirstDay: "20240820#bills#groupid1", LastDay: "20240825", Data: "XYZ"},
		{UserID: "123", FirstDay: "20240822#bills#groupid1", LastDay: "20240827", Data: "XYZ"},
		{UserID: "123", FirstDay: "20240825#bills#groupid1", LastDay: "20240830", Data: "XYZ"},
		{UserID: "123", FirstDay: "20240827#bills#groupid1", LastDay: "20240901", Data: "XYZ"},
		{UserID: "123", FirstDay: "20240828#bills#groupid1", LastDay: "20240904", Data: "XYZ"},
		{UserID: "123", FirstDay: "20240829#bills#groupid1", LastDay: "20240905", Data: "XYZ"},
		{UserID: "124", FirstDay: "20240826#bills#groupid2", LastDay: "20240828", Data: "XYZ"},
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

	today := "20240827"
	todayLimit := "20240828" // Everything LessThan todayLimit

	keyCond := expression.Key("UserID").Equal(expression.Value("123")).And(
		expression.Key("FirstDay").LessThan(expression.Value(todayLimit)),
	)

	filterExpr := expression.Name("LastDay").GreaterThanEqual(expression.Value(today))

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
		fmt.Printf("UserID: %s, FirstDay: %s, LastDay: %s, Data: %s\n", item.UserID, item.FirstDay, item.LastDay, item.Data)
	}
}
