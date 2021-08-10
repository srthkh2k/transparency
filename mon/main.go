package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

type foodTable struct {
	OrderID    string `json:"OrderID"`
	ObjectURL  string `json:"ObjectURL"`
	ObjectSize int64  `json:"ObjectSize"`
}

func main() {
	lambda.Start(handler)
}
func handler(ctx context.Context, s3Event events.S3Event) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String("ap-south-1")})
	if err != nil {
		exitErrorf("Unable to Create AWS session")
	}
	// Creating a S3 and DDB service client
	svc := s3.New(sess)
	dbSvc := dynamodb.New(sess)
	table_name := "FoodPrepVideos"
	bucket_name := s3Event.Records[0].S3.Bucket.Name
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket_name)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket_name, err)
	}
	for _, item := range resp.Contents {
		orderID := *item.Key
		foodVideo := foodTable{
			OrderID:    orderID[:len(orderID)-4],
			ObjectURL:  "https://food-preparation-videos.s3.ap-south-1.amazonaws.com/" + orderID,
			ObjectSize: *item.Size,
		}
		av, err := dynamodbattribute.MarshalMap(foodVideo)
		if err != nil {
			exitErrorf("Got error marshalling new movie item: %s", err)
		}
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(table_name),
		}
		_, err = dbSvc.PutItem(input)
		if err != nil {
			exitErrorf("Got error calling PutItem: %s", err)
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
