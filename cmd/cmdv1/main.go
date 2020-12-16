package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

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
	presign      bool
)

func init() {
	flag.StringVar(&endpoint, "e", "http://192.168.55.2:9000", "The `endpoint` of the S3 service")
	flag.StringVar(&region, "r", "cn", "region")
	flag.StringVar(&ak, "ak", "object_user1", "access key")
	flag.StringVar(&sk, "sk", "ChangeMeChangeMeChangeMeChangeMeChangeMe", "secret key")
	flag.StringVar(&bucket, "b", "", "The `name` of the S3 bucket")
	flag.StringVar(&key, "k", "", "The `name` of the Object key")
	flag.StringVar(&sql, "sql", "", "The SELECT sql expression(select * from s3object where PopDensity=12.669)")
	flag.StringVar(&compressType, "t", "NONE", "Object compress type, Valid values: NONE, GZIP, BZIP2")
	flag.BoolVar(&debug, "debug", false, "show debug log")
	flag.BoolVar(&presign, "presign", false, "presign getObject request")
}

func catObject(c *s3.S3, bucket, key string, presign bool) error {
	req, out := c.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if presign {
		purl, err := req.Presign(7 * 24 * time.Hour)
		if err != nil {
			return err
		}
		fmt.Println(purl)
		return nil
	}
	if err := req.Send(); err != nil {
		return err
	}
	_, err := io.Copy(os.Stdout, out.Body)
	return err
}

func selectObjectContent(c *s3.S3, bucket, key, sql string) error {
	resp, err := c.SelectObjectContent(&s3.SelectObjectContentInput{
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
		return err
	}
	defer resp.EventStream.Close()

	for e := range resp.EventStream.Events() {
		switch v := e.(type) {
		case *s3.RecordsEvent:
			fmt.Printf("%s", v.Payload)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if len(bucket) == 0 || len(key) == 0 {
		flag.PrintDefaults()
		fmt.Println("invalid parameters")
		return
	}
	var logLevel aws.LogLevelType
	if debug {
		logLevel = aws.LogDebug
	}
	cfg := aws.Config{
		Endpoint:         &endpoint,
		Region:           &region,
		LogLevel:         &logLevel,
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(ak, sk, ""),
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		panic(err)
	}
	s3Client := s3.New(sess)

	if sql == "" {
		err := catObject(s3Client, bucket, key, presign)
		if err != nil {
			fmt.Println("get object error, ", err)
			return
		}
	} else {
		err := selectObjectContent(s3Client, bucket, key, sql)
		if err != nil {
			fmt.Println("select object content error, ", err)
			return
		}
	}
}
