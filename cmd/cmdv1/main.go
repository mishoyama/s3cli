package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	endpoint     string
	region       string
	ak           string
	sk           string
	bucket       string
	key          string
	sql          string
	compressType string
	debug        bool
)

func init() {
	flag.StringVar(&endpoint, "e", "http://192.168.55.2:9000", "The `endpoint` of the S3 service")
	flag.StringVar(&region, "r", "cn", "region")
	flag.StringVar(&ak, "ak", "object_user1", "access key")
	flag.StringVar(&sk, "sk", "ChangeMeChangeMeChangeMeChangeMeChangeMe", "secret key")
	flag.StringVar(&bucket, "b", "bucket", "The `name` of the S3 bucket")
	flag.StringVar(&key, "k", "TotalPopulation.csv", "The `name` of the Object key")
	flag.StringVar(&sql, "sql", "select * from s3object s where s.Location like '%United States%'", "The SELECT sql expression")
	flag.StringVar(&compressType, "t", "NONE", "Object compress type, Valid values: NONE, GZIP, BZIP2")
	flag.BoolVar(&debug, "debug", false, "show debug log")
}

func main() {
	flag.Parse()
	if len(bucket) == 0 || len(sql) == 0 {
		flag.PrintDefaults()
		log.Fatalf("invalid parameters")
	}
	cfg := aws.Config{
		Endpoint:         &endpoint,
		Region:           &region,
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(ak, sk, ""),
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		panic(err)
	}
	s3Client := s3.New(sess)

	resp, err := s3Client.SelectObjectContent(&s3.SelectObjectContentInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		Expression:     aws.String(sql),
		ExpressionType: aws.String("SQL"),
		InputSerialization: &s3.InputSerialization{
			CSV: &s3.CSVInput{
				FileHeaderInfo: aws.String("Use"),
			},
			CompressionType: aws.String(compressType),
		},
		OutputSerialization: &s3.OutputSerialization{
			CSV: &s3.CSVOutput{},
		},
	})
	if err != nil {
		log.Fatal("download ", err)
		return
	}
	defer resp.EventStream.Close()

	for e := range resp.EventStream.Events() {
		switch v := e.(type) {
		case *s3.RecordsEvent:
			fmt.Printf("%s", v.Payload)
		}
	}

}
