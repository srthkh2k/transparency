package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"io/ioutil"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3 := record.S3
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
		fileContent := getDataFromS3File(s3.Bucket.Name, s3.Object.Key)
		dataExtracted := extractData(fileContent)
		insertIntoDynamoDB(dataExtracted, s3.Object.Key)
		fmt.Printf("Finished")
	}
}

func getDataFromS3File(bucket string, s3File string) string {
	//the only writable directory in the lambda is /tmp
	file, err := os.Create("/tmp/" + s3File)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", s3File, err)
	}

	defer file.Close()

	// replace with your bucket region
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("ap-south-1")},
	)

	downloader := s3manager.NewDownloader(sess)

	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3File),
		})
	if err != nil {
		exitErrorf("Unable to download s3File %q, %v", s3File, err)
	}

	dat, err := ioutil.ReadFile(file.Name())

	if err != nil {
		exitErrorf("Cannot read the file", err)
	}

	return string(dat)

}

func extractData(data string) []string {
	lines := strings.Split(data, "\n")

	var names []string

	for _, currentLine := range lines {
		name := strings.Split(currentLine, ",")[1]
		names = append(names, name)
	}

	return names
}

func insertIntoDynamoDB(dataToInsert []string, fileName string) {
	type MyDataFromS3 struct {
		FileKey string
		Name    string
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	for currentIndex, currentItem := range dataToInsert {
		data := MyDataFromS3{
			Name:    currentItem,
			FileKey: fileName + "-" + strconv.Itoa(currentIndex),
		}

		av, err := dynamodbattribute.MarshalMap(data)
		if err != nil {
			exitErrorf("Got error marshalling new movie item:", av, err)
		}

		tableName := "MyDataFromS3"
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}

		_, err = svc.PutItem(input)
		if err != nil {
			exitErrorf("Got error calling PutItem:", err)
		}
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
